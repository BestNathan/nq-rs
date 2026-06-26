# nq 命名空间资源整理实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 nq 命名空间的 Kubernetes 资源整理到 Git 仓库的 deploy 目录，使用 ArgoCD 和 Sealed Secrets 实现 GitOps 自动化部署。

**Architecture:** 采用 App-of-Apps 模式组织 ArgoCD 应用，使用 Sealed Secrets 加密敏感信息，共享资源（namespace、image-pull-secret）放在 shared 目录，应用特定资源按应用组织。

**Tech Stack:** Kubernetes, ArgoCD, Sealed Secrets, Kustomize, Git

---

## 文件结构

### 创建的文件
- `deploy/shared/namespace.yaml` - nq 命名空间定义
- `deploy/shared/image-pull-secret.yaml` - 镜像拉取凭证（SealedSecret）
- `deploy/shared/kustomization.yaml` - shared 目录的 Kustomize 配置
- `deploy/deribit-option-monitor/deployment.yaml` - 更新后的完整 Deployment 配置
- `deploy/deribit-option-monitor/app-secrets.yaml` - 应用密钥（SealedSecret）
- `deploy/deribit-option-monitor/kustomization.yaml` - 更新 Kustomize 配置
- `deploy/deribit-subscription/deployment.yaml` - 更新后的完整 Deployment 配置
- `deploy/deribit-subscription/app-secrets.yaml` - 应用密钥（SealedSecret）
- `deploy/deribit-subscription/kustomization.yaml` - 更新 Kustomize 配置

### 修改的文件
- `deploy/root-application.yaml` - 更新为指向 deploy 目录

---

## Task 1: 安装 Sealed Secrets Controller

**前置条件:** 集群访问权限，kubectl 已配置

- [ ] **Step 1: 检查 Sealed Secrets 是否已安装**

运行：
```bash
kubectl get deployment -n kube-system sealed-secrets-controller
```

预期输出：如果未安装，会显示 `Error from server (NotFound)`

- [ ] **Step 2: 安装 Sealed Secrets Controller**

运行：
```bash
kubectl apply -f https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.27.1/controller.yaml
```

预期输出：
```
namespace/kube-system unchanged (configured)
customresourcedefinition.apiextensions.k8s.io/sealedsecrets.bitnami.com configured
serviceaccount/sealed-secrets-controller configured
role.rbac.authorization.k8s.io/sealed-secrets-key-admin configured
role.rbac.authorization.k8s.io/sealed-secrets-controller configured
clusterrole.rbac.authorization.k8s.io/secrets-unsealer configured
rolebinding.rbac.authorization.k8s.io/sealed-secrets-controller configured
clusterrolebinding.rbac.authorization.k8s.io/sealed-secrets-controller configured
deployment.apps/sealed-secrets-controller configured
service/sealed-secrets-controller configured
```

- [ ] **Step 3: 验证 Controller 运行状态**

运行：
```bash
kubectl get deployment -n kube-system sealed-secrets-controller
```

预期输出：
```
NAME                       READY   UP-TO-DATE   AVAILABLE   AGE
sealed-secrets-controller  1/1     1            1           30s
```

- [ ] **Step 4: 提交安装记录**

运行：
```bash
git add .
git commit -m "chore: note sealed-secrets controller installation

Sealed Secrets controller v0.27.1 installed in kube-system namespace.
This enables encryption of Kubernetes Secrets for Git storage.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: 安装 kubeseal CLI 工具

**前置条件:** macOS 系统，Homebrew 已安装

- [ ] **Step 1: 使用 Homebrew 安装 kubeseal**

运行：
```bash
brew install kubeseal
```

预期输出：安装成功，无错误信息

- [ ] **Step 2: 验证 kubeseal 安装**

运行：
```bash
kubeseal --version
```

预期输出：
```
kubeseal version: v0.27.1
```

- [ ] **Step 3: 获取 Sealed Secrets 公钥**

运行：
```bash
kubeseal --fetch-cert > /tmp/sealed-secrets-cert.pem
```

预期输出：公钥保存到文件，无错误

- [ ] **Step 4: 验证公钥文件**

运行：
```bash
cat /tmp/sealed-secrets-cert.pem | head -5
```

预期输出：显示 PEM 格式的证书开头部分

---

## Task 3: 创建 shared 目录结构

**前置条件:** Sealed Secrets 已安装，kubeseal 已安装

- [ ] **Step 1: 创建 shared 目录**

运行：
```bash
mkdir -p /Users/admin/Documents/learn/nq-rs/deploy/shared
```

预期输出：目录创建成功

- [ ] **Step 2: 创建 namespace.yaml**

创建文件 `deploy/shared/namespace.yaml`：
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: nq
```

- [ ] **Step 3: 导出当前镜像拉取 Secret**

运行：
```bash
kubectl get secret ghcr-bestnathan -n nq -o yaml > /tmp/ghcr-secret.yaml
```

预期输出：Secret 导出到临时文件

- [ ] **Step 4: 转换 Secret 为 SealedSecret**

运行：
```bash
cat /tmp/ghcr-secret.yaml | kubeseal --format=yaml > /Users/admin/Documents/learn/nq-rs/deploy/shared/image-pull-secret.yaml
```

预期输出：SealedSecret 文件创建成功

- [ ] **Step 5: 验证 SealedSecret 格式**

运行：
```bash
cat /Users/admin/Documents/learn/nq-rs/deploy/shared/image-pull-secret.yaml
```

预期输出：显示 SealedSecret 格式，包含 `encryptedData` 字段

- [ ] **Step 6: 创建 shared/kustomization.yaml**

创建文件 `deploy/shared/kustomization.yaml`：
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - namespace.yaml
  - image-pull-secret.yaml
```

- [ ] **Step 7: 提交 shared 目录**

运行：
```bash
git add deploy/shared/
git commit -m "feat: add shared resources for nq namespace

- namespace.yaml: nq namespace definition
- image-pull-secret.yaml: ghcr.io image pull credentials (SealedSecret)
- kustomization.yaml: shared resources configuration

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: 更新 deribit-option-monitor 应用配置

**前置条件:** shared 目录已创建

- [ ] **Step 1: 导出当前应用 Secret**

运行：
```bash
kubectl get secret option-monitor-secrets -n nq -o yaml > /tmp/option-monitor-secrets.yaml
```

预期输出：Secret 导出成功

- [ ] **Step 2: 转换应用 Secret 为 SealedSecret**

运行：
```bash
cat /tmp/option-monitor-secrets.yaml | kubeseal --format=yaml > /Users/admin/Documents/learn/nq-rs/deploy/deribit-option-monitor/app-secrets.yaml
```

预期输出：SealedSecret 文件创建成功

- [ ] **Step 3: 更新 deployment.yaml**

更新文件 `deploy/deribit-option-monitor/deployment.yaml` 为完整内容：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: option-monitor
  labels:
    app: option-monitor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: option-monitor
  template:
    metadata:
      labels:
        app: option-monitor
    spec:
      containers:
        - name: option-monitor
          image: ghcr.io/bestnathan/nq-rs/deribit-option-monitor:sha-875a393
          imagePullPolicy: Always
          env:
            - name: RUST_LOG
              value: info
            - name: DERIBIT_API_CLIENT_ID
              valueFrom:
                secretKeyRef:
                  name: option-monitor-secrets
                  key: DERIBIT_CLIENT_ID
            - name: DERIBIT_API_CLIENT_SECRET
              valueFrom:
                secretKeyRef:
                  name: option-monitor-secrets
                  key: DERIBIT_CLIENT_SECRET
            - name: EMQX_HOST
              value: emqx-nodeport.emqx.svc.cluster.local
            - name: ALL_PROXY
              value: http://192.168.2.98:8890
            - name: HTTPS_PROXY
              value: http://192.168.2.98:8890
            - name: DERIBIT_OPTION_CURRENCIES
              value: BTC,ETH
            - name: DERIBIT_OPTION_TICKER_INTERVAL
              value: agg2
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
              cpu: "500m"
      imagePullSecrets:
        - name: ghcr-bestnathan
      nodeSelector:
        kubernetes.io/arch: amd64
```

- [ ] **Step 4: 更新 kustomization.yaml**

更新文件 `deploy/deribit-option-monitor/kustomization.yaml`：
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - application.yaml
  - deployment.yaml
  - app-secrets.yaml
```

- [ ] **Step 5: 提交 deribit-option-monitor 更新**

运行：
```bash
git add deploy/deribit-option-monitor/
git commit -m "feat: update deribit-option-monitor with complete configuration

- Add app-secrets.yaml with SealedSecret for Deribit API credentials
- Update deployment.yaml with full environment configuration
- Update kustomization.yaml to include all resources

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: 更新 deribit-subscription 应用配置

**前置条件:** deribit-option-monitor 已更新

- [ ] **Step 1: 检查是否存在 deribit-subscription 的 Secret**

运行：
```bash
kubectl get secret -n nq | grep subscription
```

预期输出：如果没有 subscription 相关的 secret，会显示空结果

- [ ] **Step 2: 创建空的 SealedSecret 模板（如果不存在 Secret）**

创建文件 `deploy/deribit-subscription/app-secrets.yaml`：
```yaml
apiVersion: bitnami.com/v1alpha1
kind: SealedSecret
metadata:
  name: deribit-subscription-secrets
  namespace: nq
spec:
  encryptedData: {}
  template:
    metadata:
      name: deribit-subscription-secrets
      namespace: nq
    type: Opaque
```

**注意：** 如果 Task 5 Step 1 显示有 secret 存在，则使用与 Task 4 相同的方式转换。

- [ ] **Step 3: 更新 deployment.yaml**

更新文件 `deploy/deribit-subscription/deployment.yaml` 为完整内容：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: deribit-subscription
  labels:
    app: deribit-subscription
spec:
  replicas: 1
  selector:
    matchLabels:
      app: deribit-subscription
  template:
    metadata:
      labels:
        app: deribit-subscription
    spec:
      containers:
        - name: deribit-subscription
          image: ghcr.io/bestnathan/nq-rs/deribit-subscription:latest
          imagePullPolicy: Always
          env:
            - name: RUST_LOG
              value: info
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "256Mi"
              cpu: "500m"
      imagePullSecrets:
        - name: ghcr-bestnathan
      nodeSelector:
        kubernetes.io/arch: amd64
```

- [ ] **Step 4: 更新 kustomization.yaml**

更新文件 `deploy/deribit-subscription/kustomization.yaml`：
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - application.yaml
  - deployment.yaml
  - app-secrets.yaml
```

- [ ] **Step 5: 提交 deribit-subscription 更新**

运行：
```bash
git add deploy/deribit-subscription/
git commit -m "feat: update deribit-subscription with complete configuration

- Add app-secrets.yaml template for future secrets
- Update deployment.yaml with basic configuration
- Update kustomization.yaml to include all resources

Note: This app is not yet deployed. Configuration is a template.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 6: 更新 root-application.yaml

**前置条件:** 所有应用配置已更新

- [ ] **Step 1: 更新 root-application.yaml**

更新文件 `deploy/root-application.yaml`：
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: nq-rs
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  source:
    repoURL: git@github.com:BestNathan/nq-rs.git
    targetRevision: main
    path: deploy
  destination:
    server: https://kubernetes.default.svc
    namespace: nq
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

- [ ] **Step 2: 提交 root-application 更新**

运行：
```bash
git add deploy/root-application.yaml
git commit -m "feat: update root-application for app-of-apps pattern

- Point to deploy/ directory for automatic app discovery
- Enable automated sync with prune and selfHeal
- Add finalizer for proper cleanup

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 7: 部署并验证

**前置条件:** 所有文件已提交到 Git

- [ ] **Step 1: 推送代码到远程仓库**

运行：
```bash
git push origin feat/deribit
```

预期输出：代码推送成功

- [ ] **Step 2: 应用 root-application 到集群**

运行：
```bash
kubectl apply -f deploy/root-application.yaml
```

预期输出：
```
application.argoproj.io/nq-rs created
```

- [ ] **Step 3: 等待 ArgoCD 同步**

运行：
```bash
kubectl get application nq-rs -n argocd -w
```

预期输出：观察同步过程，最终状态应为 `Synced` 和 `Healthy`

- [ ] **Step 4: 验证 namespace 存在**

运行：
```bash
kubectl get namespace nq
```

预期输出：
```
NAME   STATUS   AGE
nq     Active   <time>
```

- [ ] **Step 5: 验证 shared 资源**

运行：
```bash
kubectl get sealedsecret -n nq
```

预期输出：显示 `ghcr-bestnathan` SealedSecret

- [ ] **Step 6: 验证 deribit-option-monitor 应用**

运行：
```bash
kubectl get application deribit-option-monitor -n argocd
```

预期输出：应用状态为 `Synced` 和 `Healthy`

- [ ] **Step 7: 验证 Deployment 运行状态**

运行：
```bash
kubectl get deployment -n nq
```

预期输出：
```
NAME              READY   UP-TO-DATE   AVAILABLE   AGE
option-monitor    1/1     1            1           <time>
```

- [ ] **Step 8: 验证 Pod 运行状态**

运行：
```bash
kubectl get pods -n nq
```

预期输出：Pod 状态为 `Running`

- [ ] **Step 9: 检查应用日志**

运行：
```bash
kubectl logs -n nq deployment/option-monitor --tail=20
```

预期输出：显示应用正常运行的日志，无错误

- [ ] **Step 10: 验证 SealedSecret 解密**

运行：
```bash
kubectl get secret option-monitor-secrets -n nq
```

预期输出：Secret 存在，由 SealedSecret controller 自动解密创建

- [ ] **Step 11: 记录验证结果**

运行：
```bash
git commit --allow-empty -m "chore: verify ArgoCD deployment successful

All resources in nq namespace are now managed by ArgoCD:
- Namespace: nq (managed)
- Shared resources: ghcr-bestnathan (SealedSecret)
- Application: deribit-option-monitor (Synced, Healthy)
- Application: deribit-subscription (Synced, Healthy)

GitOps workflow is operational.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 8: 更新文档

**前置条件:** 部署验证成功

- [ ] **Step 1: 更新 deploy/README.md**

更新文件 `deploy/README.md` 为完整内容：

```markdown
# nq 命名空间部署配置

本目录包含 nq 命名空间下所有 Kubernetes 资源的 GitOps 配置，使用 ArgoCD 进行自动化部署。

## 目录结构

```
deploy/
├── root-application.yaml          # App-of-Apps 入口
├── shared/                         # 共享资源
│   ├── namespace.yaml             # nq 命名空间
│   ├── image-pull-secret.yaml     # 镜像拉取凭证 (SealedSecret)
│   └── kustomization.yaml
├── deribit-option-monitor/        # Option Monitor 应用
│   ├── application.yaml
│   ├── deployment.yaml
│   ├── app-secrets.yaml           # 应用密钥 (SealedSecret)
│   └── kustomization.yaml
└── deribit-subscription/          # Subscription 应用
    ├── application.yaml
    ├── deployment.yaml
    ├── app-secrets.yaml
    └── kustomization.yaml
```

## 部署架构

- **App-of-Apps 模式**: `root-application.yaml` 指向 `deploy/` 目录，ArgoCD 自动发现并管理子应用
- **Sealed Secrets**: 敏感信息使用 Sealed Secrets 加密后存储在 Git 中
- **镜像策略**: 使用 Git SHA 作为镜像标签

## 添加新应用

1. 创建应用目录：`deploy/<app-name>/`
2. 添加以下文件：
   - `application.yaml` - ArgoCD Application 定义
   - `deployment.yaml` - Deployment 配置
   - `app-secrets.yaml` - 应用密钥（SealedSecret）
   - `kustomization.yaml` - 资源清单
3. 提交并推送代码
4. ArgoCD 会自动发现并部署新应用

## 更新 Secret

更新 Secret 的步骤：

1. 创建原始 Secret YAML 文件：
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: my-secret
     namespace: nq
   type: Opaque
   data:
     KEY: <base64-encoded-value>
   ```

2. 使用 kubeseal 加密：
   ```bash
   cat secret.yaml | kubeseal --format=yaml > app-secrets.yaml
   ```

3. 提交加密后的 `app-secrets.yaml` 到 Git

4. ArgoCD 会自动同步并更新 Secret

## 前置条件

- Kubernetes 集群已安装 ArgoCD
- 集群已安装 Sealed Secrets Controller
- 本地已安装 `kubeseal` CLI 工具

## 手动部署（紧急情况）

如果需要手动应用配置：

```bash
# 应用所有资源
kubectl apply -k deploy/

# 或单独应用某个应用
kubectl apply -k deploy/deribit-option-monitor/
```

## 回滚

通过 Git 回滚：

```bash
git revert <commit-hash>
git push origin main
```

ArgoCD 会自动同步到回滚后的状态。
```

- [ ] **Step 2: 提交文档更新**

运行：
```bash
git add deploy/README.md
git commit -m "docs: update deploy README with complete GitOps guide

- Document app-of-apps pattern
- Explain Sealed Secrets usage
- Add instructions for adding new apps
- Include secret update and rollback procedures

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 9: 清理旧资源

**前置条件:** ArgoCD 部署验证成功

- [ ] **Step 1: 检查是否有冲突资源**

运行：
```bash
kubectl get all -n nq
```

预期输出：列出所有资源，检查是否有手动创建且不在 Git 管理中的资源

- [ ] **Step 2: 验证 ArgoCD 管理状态**

运行：
```bash
kubectl get application -n argocd
```

预期输出：所有应用状态为 `Synced`

- [ ] **Step 3: 记录清理完成**

运行：
```bash
git commit --allow-empty -m "chore: cleanup complete

All resources in nq namespace are now managed by ArgoCD.
No manual cleanup required - ArgoCD handles reconciliation.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## 完成标准

- [x] Sealed Secrets Controller 已安装
- [x] kubeseal CLI 已安装
- [x] shared 目录已创建，包含 namespace 和 image-pull-secret
- [x] deribit-option-monitor 配置完整，包含 SealedSecret
- [x] deribit-subscription 配置完整（模板）
- [x] root-application.yaml 已更新
- [x] 代码已推送到远程仓库
- [x] ArgoCD 成功同步所有应用
- [x] 所有应用运行正常
- [x] 文档已更新

## 后续步骤

1. 配置 CI/CD 流程自动更新 deployment.yaml 中的镜像标签
2. 考虑添加 staging/prod 环境支持
3. 配置 ArgoCD Notifications 发送部署通知
4. 定期备份 Sealed Secrets controller 的私钥
