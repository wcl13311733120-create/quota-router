# QuotaRouter: 多供应商配额感知 API 韧性网关 (MVP)

> **针对大模型供应商限流致歉（如 Kimi K3.1 Pro 429 配额用尽事件）研发的配额感知高可用路由代理。**

## 1. 背景与痛点
2026 年各主流大模型（如 Kimi K3.1 Pro、DeepSeek 等）在突发高并发场景下频繁遭遇 429 限流与配额提前耗尽问题。AI Agent 与企业应用直接依赖单模型供应商极易造成业务中断。

现有 API 网关主要基于静态权重或成本路由，**缺乏对供应商实时配额余量、滑动窗口速率限制（RPM/TPM）及动态熔断恢复周期的感知**。

## 2. 核心架构与特性
- **OpenAI 兼容协议**：无需重写上层业务逻辑，无缝替换现有的 `base_url`（支持 `/v1/chat/completions`、`/v1/models`）。
- **配额感知动态打分算法 (Quota-Aware Routing)**：
  - 实时维护滑动窗口（Sliding Window）统计各供应商 60 秒内的 RPM 与 TPM 消耗。
  - 动态计算评分公式：`Score = (剩余RPM比例 * 0.4) + (剩余TPM比例 * 0.4) + (优先级基础分 * 0.2)`。
- **毫秒级无感故障转移 (Automatic Failover)**：
  - 当上游供应商触发 429（Rate Limit）、503、连接超时时，网关自动标记并进入冷却状态（Cooldown），并在同一次请求中以毫秒级平滑转由候选降级节点接管，客户端无报错感知。
- **内置可视化控制台 (Web Dashboard)**：
  - 直观展示每个供应商的健康状态、实时 RPM / TPM 进度条、平均延迟与配额余量。
  - 支持一键模拟 429 报错，测试系统容灾转移能力。
- **零额外依赖**：基于 Python 3 原生标准库 + requests 构建，无重量级框架包袱，秒级启动。

---

## 3. 目录结构
```text
products/quota-router/
├── config.json          # 多供应商配置（端点、API Key、优先级、RPM/TPM限额）
├── gateway.py           # 网关核心服务（内置 HTTP 代理、路由打分器、熔断器）
├── test_gateway.py      # 端到端自动化仿真测试套件
├── web/
│   └── index.html       # 现代响应式 Web 可视化仪表盘
└── README.md            # 项目说明文档
```

---

## 4. 快速启动与测试

### 启动网关服务
```bash
python3 products/quota-router/gateway.py products/quota-router/config.json 8090
```

服务启动后：
- 🌐 **Web 监控大屏**：`http://localhost:8090/`
- 🤖 **OpenAI 代理端点**：`http://localhost:8090/v1/chat/completions`
- 📊 **指标监控 API**：`http://localhost:8090/api/status`

### 运行自动化端到端测试
```bash
python3 products/quota-router/test_gateway.py
```
测试脚本将自动启动仿真服务，模拟主力供应商突发 429 报错，并验证网关是否能在数十毫秒内将流量自动无感转移至备用模型池。
