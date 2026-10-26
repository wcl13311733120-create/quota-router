#!/usr/bin/env python3
"""
QuotaRouter: Multi-Provider Quota-Aware Resilient API Gateway (MVP)
-------------------------------------------------------------------
Features:
- OpenAI API compatibility (/v1/chat/completions, /v1/models)
- Real-time sliding-window RPM & TPM tracking
- Automatic failover & circuit breaker upon 429 / rate limits / 5xx
- Multiple routing strategies: priority, quota_aware, low_latency
- Built-in Mock Simulation mode for testing failover scenarios
- Interactive Web Dashboard (/ for GUI, /api/status for JSON metrics)
"""

import os
import sys
import time
import json
import socket
import logging
import threading
from typing import List, Dict, Any, Optional
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
)
logger = logging.getLogger("QuotaRouter")


class ProviderStatus:
    ACTIVE = "active"
    COOLDOWN = "cooldown"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXCEEDED = "quota_exceeded"
    DISABLED = "disabled"


class Provider:
    def __init__(self, data: Dict[str, Any]):
        self.id = data["id"]
        self.name = data.get("name", self.id)
        self.base_url = data.get("base_url", "https://api.openai.com/v1").rstrip("/")
        self.api_key = data.get("api_key", "")
        self.models = data.get("models", [])
        self.priority = data.get("priority", 10)
        self.max_rpm = data.get("max_rpm", 60)
        self.max_tpm = data.get("max_tpm", 50000)
        self.cooldown_seconds = data.get("cooldown_seconds", 30)
        self.is_active = data.get("is_active", True)

        # Runtime dynamic metrics
        self.status = ProviderStatus.ACTIVE if self.is_active else ProviderStatus.DISABLED
        self.status_reason = "Initialized"
        self.cooldown_until = 0.0

        # Request sliding window tracking (timestamp lists)
        self.request_timestamps: List[float] = []
        self.token_records: List[tuple] = []  # (timestamp, token_count)

        # Lifetime metrics
        self.total_requests = 0
        self.success_requests = 0
        self.failed_requests = 0
        self.rate_limited_count = 0
        self.avg_latency_ms = 0.0
        self._latencies: List[float] = []
        self.last_used_at = 0.0
        self.lock = threading.RLock()

    def clean_sliding_windows(self, now: float):
        cutoff = now - 60.0
        self.request_timestamps = [t for t in self.request_timestamps if t > cutoff]
        self.token_records = [rec for rec in self.token_records if rec[0] > cutoff]

    def current_rpm(self, now: float) -> int:
        self.clean_sliding_windows(now)
        return len(self.request_timestamps)

    def current_tpm(self, now: float) -> int:
        self.clean_sliding_windows(now)
        return sum(rec[1] for rec in self.token_records)

    def is_available(self, now: float, model: Optional[str] = None) -> bool:
        with self.lock:
            if not self.is_active:
                return False
            if model and self.models and model not in self.models:
                return False

            # Check cooldown expiration
            if self.status in (ProviderStatus.COOLDOWN, ProviderStatus.RATE_LIMITED):
                if now >= self.cooldown_until:
                    self.status = ProviderStatus.ACTIVE
                    self.status_reason = "Cooldown recovered"
                else:
                    return False
            elif self.status == ProviderStatus.QUOTA_EXCEEDED:
                return False

            # Check RPM / TPM soft limits
            if self.current_rpm(now) >= self.max_rpm:
                return False
            if self.current_tpm(now) >= self.max_tpm:
                return False

            return True

    def calculate_score(self, now: float) -> float:
        """
        Quota-aware scoring algorithm:
        Score = Remaining RPM Ratio (40%) + Remaining TPM Ratio (40%) + Priority Score (20%)
        Higher score means better candidate.
        """
        with self.lock:
            rpm_ratio = max(0.0, 1.0 - (self.current_rpm(now) / max(1, self.max_rpm)))
            tpm_ratio = max(0.0, 1.0 - (self.current_tpm(now) / max(1, self.max_tpm)))
            priority_score = 1.0 / max(1, self.priority)
            return (rpm_ratio * 0.4) + (tpm_ratio * 0.4) + (priority_score * 0.2)

    def mark_success(self, tokens: int, latency_ms: float):
        with self.lock:
            now = time.time()
            self.total_requests += 1
            self.success_requests += 1
            self.last_used_at = now
            self.request_timestamps.append(now)
            self.token_records.append((now, tokens))
            self._latencies.append(latency_ms)
            if len(self._latencies) > 50:
                self._latencies.pop(0)
            self.avg_latency_ms = round(sum(self._latencies) / len(self._latencies), 1)

    def mark_failure(self, status_code: int, reason: str):
        with self.lock:
            now = time.time()
            self.total_requests += 1
            self.failed_requests += 1
            self.last_used_at = now
            if status_code == 429:
                self.rate_limited_count += 1
                self.status = ProviderStatus.RATE_LIMITED
                self.cooldown_until = now + self.cooldown_seconds
                self.status_reason = f"HTTP 429 Rate Limited. Cooldown for {self.cooldown_seconds}s ({reason})"
            elif status_code in (401, 403) and "quota" in reason.lower():
                self.status = ProviderStatus.QUOTA_EXCEEDED
                self.status_reason = f"Quota Exceeded: {reason}"
            else:
                self.status = ProviderStatus.COOLDOWN
                self.cooldown_until = now + min(self.cooldown_seconds, 15)
                self.status_reason = f"HTTP {status_code} Failure: {reason}"

    def reset_status(self):
        with self.lock:
            self.status = ProviderStatus.ACTIVE if self.is_active else ProviderStatus.DISABLED
            self.status_reason = "Manually reset"
            self.cooldown_until = 0.0

    def to_dict(self) -> Dict[str, Any]:
        now = time.time()
        with self.lock:
            self.clean_sliding_windows(now)
            remaining_cooldown = max(0.0, round(self.cooldown_until - now, 1))
            return {
                "id": self.id,
                "name": self.name,
                "base_url": self.base_url,
                "models": self.models,
                "priority": self.priority,
                "max_rpm": self.max_rpm,
                "max_tpm": self.max_tpm,
                "current_rpm": len(self.request_timestamps),
                "current_tpm": sum(rec[1] for rec in self.token_records),
                "status": self.status,
                "status_reason": self.status_reason,
                "remaining_cooldown_seconds": remaining_cooldown,
                "total_requests": self.total_requests,
                "success_requests": self.success_requests,
                "failed_requests": self.failed_requests,
                "rate_limited_count": self.rate_limited_count,
                "avg_latency_ms": self.avg_latency_ms,
                "score": round(self.calculate_score(now), 3),
            }


class QuotaRouter:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.providers: List[Provider] = []
        self.strategy = "quota_aware"
        self.host = "0.0.0.0"
        self.port = 8090
        self.load_config()
        self.audit_log: List[Dict[str, Any]] = []
        self.log_lock = threading.Lock()

    def load_config(self):
        if not os.path.exists(self.config_path):
            logger.warning(f"Config {self.config_path} not found. Using defaults.")
            return

        with open(self.config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        server_cfg = cfg.get("server", {})
        self.host = server_cfg.get("host", "0.0.0.0")
        self.port = server_cfg.get("port", 8090)
        self.strategy = server_cfg.get("strategy", "quota_aware")

        self.providers = [Provider(p) for p in cfg.get("providers", [])]
        logger.info(f"Loaded {len(self.providers)} providers from {self.config_path}")

    def log_event(self, event: Dict[str, Any]):
        with self.log_lock:
            event["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            self.audit_log.insert(0, event)
            if len(self.audit_log) > 200:
                self.audit_log.pop()

    def select_candidate_order(self, model: Optional[str] = None) -> List[Provider]:
        now = time.time()
        # Filter available providers
        available = [p for p in self.providers if p.is_available(now, model)]

        if self.strategy == "priority":
            available.sort(key=lambda p: (p.priority, -p.calculate_score(now)))
        elif self.strategy == "low_latency":
            available.sort(key=lambda p: (p.avg_latency_ms if p.avg_latency_ms > 0 else 9999, p.priority))
        else:  # quota_aware default
            available.sort(key=lambda p: p.calculate_score(now), reverse=True)

        return available

    def execute_chat_completion(self, request_payload: Dict[str, Any], raw_headers: Dict[str, str]) -> tuple:
        requested_model = request_payload.get("model", "")
        candidates = self.select_candidate_order(requested_model)

        if not candidates:
            # Check if any provider exists regardless of cooldown to give helpful info
            msg = "All upstream providers exhausted or in rate-limit cooldown."
            self.log_event({
                "type": "ROUTER_EXHAUSTED",
                "model": requested_model,
                "details": "No available providers found."
            })
            return 503, {
                "error": {
                    "message": msg,
                    "type": "quota_router_exhaustion_error",
                    "code": 503
                }
            }

        tried_records = []
        for provider in candidates:
            start_time = time.time()
            logger.info(f"Routing request (model={requested_model}) to provider: {provider.id} [{provider.name}]")

            # Check if this is a simulation / mock target (e.g. localhost mock, mock-api or test keys)
            is_mock = (
                "mock" in provider.base_url
                or "8091" in provider.base_url
                or provider.base_url.startswith("mock://")
                or "mock" in provider.api_key
                or "mock" in provider.id
                or provider.api_key == "free-token"
            )

            if is_mock:
                status_code, resp_json = self._simulate_provider_call(provider, request_payload)
            else:
                status_code, resp_json = self._forward_upstream(provider, request_payload, raw_headers)

            latency_ms = round((time.time() - start_time) * 1000, 1)

            if status_code == 200:
                # Estimate token consumption
                usage = resp_json.get("usage", {})
                tokens = usage.get("total_tokens", 100)
                provider.mark_success(tokens, latency_ms)

                self.log_event({
                    "type": "SUCCESS",
                    "provider": provider.id,
                    "model": requested_model,
                    "tokens": tokens,
                    "latency_ms": latency_ms,
                    "fallback_attempts": len(tried_records)
                })

                # Attach routing metadata for transparency
                resp_json["_quota_router"] = {
                    "routed_provider": provider.id,
                    "provider_name": provider.name,
                    "fallback_count": len(tried_records),
                    "latency_ms": latency_ms
                }
                return 200, resp_json

            # Upstream failure (429, 503, connection error, etc.)
            error_reason = str(resp_json.get("error", {}).get("message", "Upstream error"))
            provider.mark_failure(status_code, error_reason)
            tried_records.append({"provider": provider.id, "status": status_code, "reason": error_reason})

            self.log_event({
                "type": "FAILOVER_TRIGGERED",
                "failed_provider": provider.id,
                "status_code": status_code,
                "reason": error_reason,
                "next_action": "Attempting next candidate"
            })
            logger.warning(f"Provider {provider.id} failed with {status_code}: {error_reason}. Initiating failover...")

        # If all candidates failed in loop
        return 502, {
            "error": {
                "message": "All attempted providers failed in current failover chain.",
                "attempts": tried_records,
                "code": 502
            }
        }

    def _simulate_provider_call(self, provider: Provider, payload: Dict[str, Any]) -> tuple:
        """Internal mock simulation for self-contained testing & verification."""
        time.sleep(0.05)  # simulate small network delay
        # If simulated rate limit flag is set on provider
        if getattr(provider, "_simulate_rate_limit", False):
            return 429, {
                "error": {
                    "message": f"Rate limit exceeded (simulated 429) on {provider.id}",
                    "type": "insufficient_quota"
                }
            }

        return 200, {
            "id": f"chatcmpl-mock-{int(time.time()*1000)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": payload.get("model", "mock-model"),
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": f"[Simulated Response via {provider.name}] Hello! Your request was routed seamlessly."
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 20,
                "completion_tokens": 15,
                "total_tokens": 35
            }
        }

    def _forward_upstream(self, provider: Provider, payload: Dict[str, Any], raw_headers: Dict[str, str]) -> tuple:
        url = f"{provider.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {provider.api_key}" if provider.api_key else raw_headers.get("Authorization", "")
        }
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            try:
                body = resp.json()
            except Exception:
                body = {"error": {"message": resp.text}}
            return resp.status_code, body
        except requests.exceptions.Timeout:
            return 504, {"error": {"message": f"Timeout connecting to {provider.id}"}}
        except Exception as e:
            return 502, {"error": {"message": f"Connection error: {str(e)}"}}


class QuotaRouterHandler(BaseHTTPRequestHandler):
    router: QuotaRouter = None
    web_dir: str = ""

    def address_string(self):
        # Avoid reverse DNS lookup delay
        return self.client_address[0]

    def _send_json(self, status: int, data: Any):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/status":
            providers_info = [p.to_dict() for p in self.router.providers]
            total_reqs = sum(p.total_requests for p in self.router.providers)
            total_success = sum(p.success_requests for p in self.router.providers)
            total_429 = sum(p.rate_limited_count for p in self.router.providers)

            data = {
                "server": {
                    "strategy": self.router.strategy,
                    "host": self.router.host,
                    "port": self.router.port,
                    "total_providers": len(self.router.providers),
                    "active_providers": sum(1 for p in self.router.providers if p.status == ProviderStatus.ACTIVE)
                },
                "summary": {
                    "total_requests": total_reqs,
                    "total_success": total_success,
                    "total_rate_limited": total_429,
                    "success_rate": f"{round((total_success/total_reqs)*100, 1)}%" if total_reqs > 0 else "100.0%"
                },
                "providers": providers_info,
                "audit_logs": self.router.audit_log[:30]
            }
            self._send_json(200, data)
            return

        if path == "/v1/models":
            # Return list of combined models
            all_models = set()
            for p in self.router.providers:
                for m in p.models:
                    all_models.add(m)
            data = {
                "object": "list",
                "data": [{"id": m, "object": "model", "owned_by": "quota-router"} for m in sorted(all_models)]
            }
            self._send_json(200, data)
            return

        if path in ("/", "/index.html"):
            index_path = os.path.join(self.web_dir, "index.html")
            if os.path.exists(index_path):
                with open(index_path, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(content)
                self.wfile.flush()
                return

        self._send_json(404, {"error": "Not Found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_len = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_len).decode("utf-8") if content_len > 0 else "{}"
        try:
            body = json.loads(raw_body)
        except Exception:
            self._send_json(400, {"error": "Invalid JSON body"})
            return

        if path == "/v1/chat/completions":
            status, resp = self.router.execute_chat_completion(body, dict(self.headers))
            self._send_json(status, resp)
            return

        if path == "/api/providers/simulate_error":
            provider_id = body.get("provider_id")
            code = body.get("code", 429)
            target = next((p for p in self.router.providers if p.id == provider_id), None)
            if target:
                target._simulate_rate_limit = (code == 429)
                target.mark_failure(code, f"Manually simulated {code} error")
                self.router.log_event({
                    "type": "SIMULATED_ALERT",
                    "provider": target.id,
                    "code": code,
                    "details": f"Triggered manual simulation error {code}"
                })
                self._send_json(200, {"message": f"Simulated {code} applied to {provider_id}", "provider": target.to_dict()})
            else:
                self._send_json(404, {"error": f"Provider {provider_id} not found"})
            return

        if path == "/api/providers/reset":
            for p in self.router.providers:
                setattr(p, "_simulate_rate_limit", False)
                p.reset_status()
            self.router.log_event({"type": "RESET_ALL", "details": "All providers reset to active"})
            self._send_json(200, {"message": "All providers reset to active"})
            return

        if path == "/api/strategy":
            new_strat = body.get("strategy")
            if new_strat in ("quota_aware", "priority", "low_latency"):
                self.router.strategy = new_strat
                self._send_json(200, {"strategy": self.router.strategy})
            else:
                self._send_json(400, {"error": "Invalid strategy"})
            return

        self._send_json(404, {"error": "Not Found"})


def run_server(config_path: str = "config.json", port: Optional[int] = None):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.isabs(config_path):
        cfg_file = config_path
    elif os.path.exists(config_path):
        cfg_file = os.path.abspath(config_path)
    else:
        cfg_file = os.path.join(base_dir, config_path)
    
    router = QuotaRouter(cfg_file)
    if port:
        router.port = port

    QuotaRouterHandler.router = router
    QuotaRouterHandler.web_dir = os.path.join(base_dir, "web")

    server_address = (router.host, router.port)
    httpd = ThreadingHTTPServer(server_address, QuotaRouterHandler)
    logger.info(f"🚀 QuotaRouter started at http://{router.host}:{router.port}")
    logger.info(f"OpenAI Endpoint: http://{router.host}:{router.port}/v1/chat/completions")
    logger.info(f"Dashboard Web UI: http://{router.host}:{router.port}/")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down QuotaRouter server...")
        httpd.server_close()


if __name__ == "__main__":
    cfg = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    p = int(sys.argv[2]) if len(sys.argv) > 2 else None
    run_server(cfg, p)
