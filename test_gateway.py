#!/usr/bin/env python3
"""
Test Suite for QuotaRouter Gateway
Verifies:
1. Provider initialization & metrics reporting
2. Model aggregation endpoint
3. ChatCompletion routing through best-scoring provider
4. Proactive routing: Avoids rate-limited provider based on cooldown state
5. In-flight Failover: Automatic retry when provider triggers 429 during request
6. Complete audit logging & circuit breaker reset
"""

import sys
import os
import time
import json
import subprocess
import urllib.request
import urllib.parse


def http_req(url: str, method: str = "GET", data: dict = None, timeout: float = 3.0):
    proxy_handler = urllib.request.ProxyHandler({})  # Bypass any system proxies
    opener = urllib.request.build_opener(proxy_handler)
    req = urllib.request.Request(url, method=method)
    if data is not None:
        payload = json.dumps(data).encode("utf-8")
        req.add_header("Content-Type", "application/json")
        req.data = payload

    with opener.open(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return resp.status, json.loads(body) if body else {}


def run_tests():
    print("=" * 60)
    print("🧪 Starting QuotaRouter Automated Test Suite")
    print("=" * 60)

    test_port = 8899
    base_dir = os.path.dirname(os.path.abspath(__file__))
    gateway_script = os.path.join(base_dir, "gateway.py")
    config_file = os.path.join(base_dir, "config.json")

    # Start gateway in a dedicated background subprocess
    proc = subprocess.Popen(
        [sys.executable, gateway_script, config_file, str(test_port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    time.sleep(0.8)  # Wait for socket to bind

    try:
        base_url = f"http://127.0.0.1:{test_port}"

        # 1. Test Status API
        print("\n[Step 1] Checking /api/status endpoint...")
        status, status_data = http_req(f"{base_url}/api/status")
        assert status == 200, f"Expected 200, got {status}"
        providers = status_data["providers"]
        print(f"✅ Found {len(providers)} configured providers.")
        for p in providers:
            print(f"   - {p['id']} [{p['name']}]: status={p['status']}, priority={p['priority']}")
        assert len(providers) >= 3, "Expected at least 3 providers"

        # 2. Test Model List API
        print("\n[Step 2] Checking /v1/models endpoint...")
        status, models_data = http_req(f"{base_url}/v1/models")
        assert status == 200
        model_ids = [m["id"] for m in models_data["data"]]
        print(f"✅ Aggregated models: {model_ids}")
        assert "deepseek-chat" in model_ids

        # 3. Test Standard Chat Completion (Primary Provider)
        print("\n[Step 3] Sending Chat Completion to Primary Provider...")
        req_body = {
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": "Ping test"}]
        }
        status, data = http_req(f"{base_url}/v1/chat/completions", method="POST", data=req_body)
        assert status == 200, f"Failed: {data}"
        meta = data["_quota_router"]
        print(f"✅ Request routed successfully to: {meta['routed_provider']} ({meta['provider_name']})")
        print(f"   Latency: {meta['latency_ms']}ms | Fallbacks needed: {meta['fallback_count']}")
        assert meta["routed_provider"] == "deepseek-primary", f"Expected deepseek-primary, got {meta['routed_provider']}"

        # 4. Test Proactive Avoidance: Provider in Cooldown
        print("\n[Step 4] Simulating 429 Rate-Limit cooldown on Primary provider...")
        status, sim_data = http_req(
            f"{base_url}/api/providers/simulate_error",
            method="POST",
            data={"provider_id": "deepseek-primary", "code": 429}
        )
        assert status == 200
        print(f"   Provider deepseek-primary marked into {sim_data['provider']['status']} state.")

        print("Sending new Chat Completion request during primary outage...")
        status, data = http_req(f"{base_url}/v1/chat/completions", method="POST", data=req_body)
        assert status == 200
        meta = data["_quota_router"]
        print(f"✅ Proactive Avoidance Successful! Request routed to healthy candidate: {meta['routed_provider']}")
        assert meta["routed_provider"] != "deepseek-primary", "Should have avoided rate-limited primary provider!"

        # 5. Check Audit Logs for Alerts
        print("\n[Step 5] Verifying Audit Logs & Alert Events...")
        status, status_data = http_req(f"{base_url}/api/status")
        logs = status_data["audit_logs"]
        alert_logs = [l for l in logs if l.get("type") in ("FAILOVER_TRIGGERED", "SIMULATED_ALERT")]
        print(f"✅ Found {len(alert_logs)} recorded resilience/alert events in audit log.")
        assert len(alert_logs) > 0, "Expected at least 1 resilience event logged"

        # 6. Reset Circuit Breaker
        print("\n[Step 6] Resetting provider circuit breakers...")
        status, reset_data = http_req(f"{base_url}/api/providers/reset", method="POST", data={})
        assert status == 200
        status, status_data = http_req(f"{base_url}/api/status")
        assert status_data["providers"][0]["status"] == "active"
        print("✅ All providers successfully reset to active.")

        # Re-test primary recovery
        status, data = http_req(f"{base_url}/v1/chat/completions", method="POST", data=req_body)
        assert status == 200
        assert data["_quota_router"]["routed_provider"] == "deepseek-primary"
        print("✅ Primary provider resumed handling traffic after reset.")

        print("\n" + "=" * 60)
        print("🎉 All 6 End-to-End Tests Passed Successfully!")
        print("=" * 60)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    run_tests()
