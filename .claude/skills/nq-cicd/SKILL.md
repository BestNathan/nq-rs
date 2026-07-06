---
name: nq-cicd
description: Use when deploying nq-rs services, updating image tags in Kubernetes, managing ArgoCD applications, building Docker images, or understanding the CI/CD pipeline for this project. Covers GitHub Actions → GHCR → ArgoCD → Kubernetes flow.
---

# nq-rs CI/CD 流水线

## 概述

**CI**: GitHub Actions 自动构建 Docker 镜像并推送到 GHCR（GitHub Container Registry）。
**CD**: ArgoCD 以 GitOps 方式监控 `deploy/` 目录，自动同步到 Kubernetes 集群。

```
git push → GitHub Actions → ghcr.io/bestnathan/nq-rs/<app>:sha-<hash>
                              ↓
                    ArgoCD 检测 deploy/ 变更
                              ↓
                    Kubernetes 滚动更新 Pod
```

## 涉及的应用

| 应用 | 目录 | 镜像 |
|------|------|------|
| `deribit-option-monitor` | `apps/deribit-option-monitor/` | `ghcr.io/bestnathan/nq-rs/deribit-option-monitor` |
| `deribit-subscription` | `apps/deribit-subscription/` | `ghcr.io/bestnathan/nq-rs/deribit-subscription` |

## CI: GitHub Actions

### 工作流文件 (`.github/workflows/docker-build.yml`, name: `cicd`)

### 触发条件

- **Push** 到 `main` → 构建并推送镜像 + 自动更新 deploy 标签 + 清理旧镜像
- **手动触发** (`workflow_dispatch`)

### Jobs

| Job | 说明 |
|-----|------|
| `build` | 为每个 app 构建 Docker 镜像，推送 `sha-<hash>` + `latest` 到 GHCR |
| `update-deploy-tags` | 自动将 `deploy/<app>/deployment.yaml` 中的镜像标签替换为最新 `sha-<hash>`，commit + push（`[skip ci]` 避免循环触发） |
| `cleanup-old-images` | 对每个 app 调用 GitHub API，保留最新 5 个非-`latest` 版本，删除更旧的 |

### 构建流程

1. **Checkout** 代码
2. **Docker Buildx** 设置多架构构建
3. **登录 GHCR**（使用 `secrets.GITHUB_TOKEN`）
4. **生成镜像标签**：
   - `sha-<short-hash>` — 每次构建
   - `latest` — 仅 main 分支
5. **构建并推送**：多阶段 Docker 构建
   - `clux/muslrust:stable` → 静态链接 musl
   - `cargo-chef` → 依赖缓存加速
   - `alpine:3.20.1` → 运行时最小镜像

### 镜像标签策略

```
main push → sha-abc1234, latest
           → 自动更新 deploy/*/deployment.yaml 中的镜像标签
           → 清理旧镜像，保留 latest + 最近 5 个 sha 版本
```

## CD: ArgoCD (GitOps)

### 架构：App-of-Apps

```
root-application (nq-rs)
├── nq-shared           ← 共享资源
│   ├── namespace.yaml          # nq 命名空间
│   └── image-pull-secret.yaml  # GHCR 拉取凭证 (SealedSecret)
├── deribit-option-monitor
│   ├── application.yaml        # ArgoCD Application
│   ├── deployment.yaml         # Kubernetes Deployment ← 镜像标签在这里
│   ├── app-secrets.yaml        # 应用密钥 (SealedSecret)
│   └── kustomization.yaml
└── deribit-subscription
    ├── application.yaml
    ├── deployment.yaml
    ├── app-secrets.yaml
    └── kustomization.yaml
```

### ArgoCD 同步策略

所有 Application 均配置：
- **automated.prune: true** — 自动删除 Git 中移除的资源
- **automated.selfHeal: true** — 自动修复手动变更（集群状态回退到 Git 定义）
- **syncOptions.CreateNamespace: true** — 自动创建命名空间

### ⛔ 铁律：禁止手动修改集群资源

**绝对不能使用以下命令直接操作集群：**

- ❌ `kubectl apply` — 会被 selfHeal 回退
- ❌ `kubectl set image` — 会被 selfHeal 回退
- ❌ `kubectl edit deployment` — 会被 selfHeal 回退
- ❌ `kubectl patch` — 会被 selfHeal 回退
- ❌ `kubectl rollout restart` — 无意义，会被回退

**任何手动变更都会在 3 分钟内被 ArgoCD 的 selfHeal 自动还原。** 部署的唯一正确方式是：修改 Git → 推送 → ArgoCD 同步。

查看只读信息（`get`、`logs`、`describe`）不受限制。

### 关键文件

| 文件 | 作用 |
|------|------|
| `deploy/root-application.yaml` | App-of-Apps 入口，指向 `deploy/` 目录 |
| `deploy/<app>/application.yaml` | 定义 ArgoCD 如何部署该应用 |
| `deploy/<app>/deployment.yaml` | Kubernetes Deployment（镜像、环境变量、资源限制） |
| `deploy/<app>/app-secrets.yaml` | SealedSecret 加密的应用密钥 |
| `deploy/shared/image-pull-secret.yaml` | GHCR 私有镜像拉取凭证 |

## 标准部署流程

### 1. 代码修改 & 提交

```bash
git add -A
git commit -m "fix: description"
git push origin <branch>
```

### 2. 创建 PR 并合并到 main

合并到 `main` 后，CI 自动触发：
1. 构建镜像，推送 `sha-<hash>` + `latest`
2. 自动更新 `deploy/<app>/deployment.yaml` 中的镜像标签
3. 清理旧镜像（保留最新 5 个）

### 3. ArgoCD 自动同步

`deploy/` 变更推送后，ArgoCD 默认每 **3 分钟** 轮询一次 Git 仓库，自动同步。

#### 手动触发同步（即时生效）

```bash
kubectl patch application -n argocd deribit-option-monitor \
  --type merge \
  -p '{"operation": {"sync": {"revision": "main"}}}'
```

### 4. 验证部署

```bash
# 查看 ArgoCD Application 同步状态
kubectl get application -n argocd deribit-option-monitor

# 查看 Pod 状态和镜像
kubectl get pods -n nq -l app=option-monitor -o wide
kubectl get pod -n nq <pod-name> -o jsonpath='{.spec.containers[0].image}'

# 查看 Pod 日志
kubectl logs -n nq <pod-name> --tail=50 -f
```

### 6. 紧急手动构建（跳过 CI）

如果 CI 不可用，可直接本地构建 Docker 镜像：

```bash
make deribit-option-monitor
# 或
make deribit-subscription
```

注意：这只构建本地镜像。推到集群仍需走 ArgoCD 流程。

## 密钥管理

### 查看密钥

应用密钥使用 **SealedSecret** 加密存储在 Git 中，只能通过集群内的 SealedSecret Controller 解密。

### 更新密钥

```bash
# 1. 创建原始 Secret YAML
cat > secret.yaml <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: option-monitor-secrets
  namespace: nq
type: Opaque
data:
  DERIBIT_CLIENT_ID: $(echo -n "your-client-id" | base64)
  DERIBIT_CLIENT_SECRET: $(echo -n "your-secret" | base64)
EOF

# 2. 使用 kubeseal 加密
cat secret.yaml | kubeseal --format=yaml \
  > deploy/deribit-option-monitor/app-secrets.yaml

# 3. 提交
git add deploy/deribit-option-monitor/app-secrets.yaml
git commit -m "chore: update secrets for option-monitor"
git push origin main

# 4. 清理
rm secret.yaml
```

### 前置条件

- 本地安装 `kubeseal` CLI
- 集群已安装 SealedSecret Controller
- 有权限访问目标 Kubernetes 集群

## 回滚

### 方法 1：Git 回滚（推荐）

```bash
# 回滚部署标签
git revert <commit-hash>
git push origin main
# ArgoCD 自动同步到回滚后状态
```

### 方法 2：直接修改标签

```bash
# 编辑 deployment.yaml 改为上一个正常标签
# 提交并推送
```

### 方法 3：ArgoCD 回滚

```bash
argocd app rollback deribit-option-monitor <revision-id>
```

## 添加新应用

1. 创建应用目录：`deploy/<app-name>/`
2. 添加以下文件：
   - `application.yaml` — ArgoCD Application（参考已有应用）
   - `deployment.yaml` — Deployment 配置（镜像、环境变量、资源限制）
   - `app-secrets.yaml` — SealedSecret（如需要）
   - `kustomization.yaml` — 列出所有资源文件
3. 在 `.github/workflows/docker-build.yml` 的 `matrix.app`、`update-deploy-tags` 的 `APPS`、`cleanup-old-images` 的 `matrix.app` 中添加新应用名
4. 提交推送，ArgoCD 自动发现

## 部署文件关键配置说明

### Deployment 环境变量来源

| 来源方式 | 用途 | 示例 |
|----------|------|------|
| 硬编码 | 非敏感配置 | `RUST_LOG`, `EMQX_HOST`, `ALL_PROXY` |
| `secretKeyRef` | 敏感凭证 | `DERIBIT_API_CLIENT_ID`, `DERIBIT_API_CLIENT_SECRET` |

### 资源配置

```yaml
resources:
  requests:           # 调度保证
    memory: "128Mi"
    cpu: "100m"
  limits:             # 硬限制
    memory: "1Gi"     # option-monitor
    memory: "256Mi"   # subscription
    cpu: "500m"
```

### 镜像拉取策略

- `imagePullPolicy: Always` — 每次重启拉取最新镜像
- `imagePullSecrets: ghcr-bestnathan` — 私有 GHCR 仓库凭证（SealedSecret）

## 常用命令速查

```bash
# CI 状态
gh run list --workflow=cicd
gh run watch <run-id>                      # 实时查看构建日志

# ArgoCD — 触发同步
argocd app sync deribit-option-monitor     # 手动触发同步 (argocd CLI)
kubectl patch application -n argocd deribit-option-monitor \
  --type merge -p '{"operation": {"sync": {"revision": "main"}}}'  # 触发同步 (kubectl)

# ArgoCD — 查看状态
argocd app list                            # 所有应用状态
kubectl get application -n argocd          # 所有 Application 状态 (kubectl)
argocd app history deribit-option-monitor  # 部署历史
argocd app rollback deribit-option-monitor <id>  # 回滚

# Kubernetes — 只读操作
kubectl get pods -n nq                     # Pod 列表
kubectl describe pod -n nq <pod>          # Pod 详情
kubectl logs -n nq <pod> --tail=100 -f    # 实时日志
kubectl get events -n nq --sort-by='.lastTimestamp'  # 最近事件

# 本地 Docker 构建（紧急）
make deribit-option-monitor
make deribit-subscription
```
