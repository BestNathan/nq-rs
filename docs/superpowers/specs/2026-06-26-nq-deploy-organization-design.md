# nq 命名空间资源整理与 ArgoCD 部署设计

**日期**: 2026-06-26  
**状态**: 设计中

## 概述

将当前手动管理的 `nq` 命名空间下的所有 Kubernetes 资源整理到 Git 仓库的 `deploy/` 目录中，使用 ArgoCD 实现 GitOps 自动化部署。

## 当前状态

### nq 命名空间资源
- **Deployment**: `option-monitor`
  - 镜像: `ghcr.io/bestnathan/nq-rs/deribit-option-monitor:sha-875a393`
  - 环境变量: DERIBIT_API_CLIENT_ID, DERIBIT_API_CLIENT_SECRET, EMQX_HOST, ALL_PROXY, HTTPS_PROXY 等
  - 资源限制: CPU 100m-500m, Memory 128Mi-512Mi
  - 镜像拉取 Secret: `ghcr-bestnathan`
  - 应用 Secret: `option-monitor-secrets`

- **Secrets**:
  - `ghcr-bestnathan`: 镜像仓库认证 (kubernetes.io/dockerconfigjson)
  - `option-monitor-secrets`: Deribit API 凭证 (Opaque)

- **Namespace**: `nq`

### 已有目录结构
```
deploy/
├── root-application.yaml
├── deribit-option-monitor/
│   ├── application.yaml
│   ├── deployment.yaml
│   └── kustomization.yaml
└── deribit-subscription/
    ├── application.yaml
    ├── deployment.yaml
    └── kustomization.yaml
```

## 设计方案

### 目录结构

```
deploy/
├── root-application.yaml          # App-of-Apps 入口，指向 deploy/ 目录
├── shared/                         # 共享资源
│   ├── namespace.yaml             # nq 命名空间定义
│   ├── image-pull-secret.yaml     # ghcr-bestnathan (Sealed Secret)
│   └── kustomization.yaml
├── deribit-option-monitor/        # 应用目录
│   ├── application.yaml           # ArgoCD Application
│   ├── deployment.yaml            # Deployment 清单
│   ├── app-secrets.yaml           # option-monitor-secrets (Sealed Secret)
│   └── kustomization.yaml
└── deribit-subscription/          # 应用目录
    ├── application.yaml
    ├── deployment.yaml
    ├── app-secrets.yaml
    └── kustomization.yaml
```

### 核心组件

#### 1. App-of-Apps 模式
- `root-application.yaml` 指向 `deploy/` 目录
- ArgoCD 自动扫描并创建子应用的 Application 资源
- 每个应用的 `application.yaml` 指向自己的目录

#### 2. Sealed Secrets
- 使用 [Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets) 加密敏感信息
- 加密后的 Secret 可以安全存储在 Git 仓库中
- ArgoCD 部署时自动解密
- 需要预先在集群中安装 Sealed Secrets controller

#### 3. 镜像策略
- 使用 Git SHA 作为镜像标签（如 `sha-875a393`）
- 与 CI/CD 流程集成，每次提交自动构建并推送镜像
- ArgoCD 可配置 Image Updater 自动更新镜像标签

#### 4. 单环境部署
- 当前只有 dev 环境
- 保持简单，避免过度工程化
- 后续需要时可扩展为多环境（staging/prod）

### 文件详细说明

#### root-application.yaml
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: nq-rs
  namespace: argocd
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

#### shared/namespace.yaml
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: nq
```

#### shared/image-pull-secret.yaml
使用 Sealed Secrets 加密的镜像拉取凭证：
```yaml
apiVersion: bitnami.com/v1alpha1
kind: SealedSecret
metadata:
  name: ghcr-bestnathan
  namespace: nq
spec:
  encryptedData:
    .dockerconfigjson: <encrypted-value>
  template:
    metadata:
      name: ghcr-bestnathan
      namespace: nq
    type: kubernetes.io/dockerconfigjson
```

#### deribit-option-monitor/application.yaml
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: deribit-option-monitor
  namespace: argocd
spec:
  project: default
  source:
    repoURL: git@github.com:BestNathan/nq-rs.git
    targetRevision: main
    path: deploy/deribit-option-monitor
  destination:
    server: https://kubernetes.default.svc
    namespace: nq
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

#### deribit-option-monitor/deployment.yaml
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

#### deribit-option-monitor/app-secrets.yaml
```yaml
apiVersion: bitnami.com/v1alpha1
kind: SealedSecret
metadata:
  name: option-monitor-secrets
  namespace: nq
spec:
  encryptedData:
    DERIBIT_CLIENT_ID: <encrypted-value>
    DERIBIT_CLIENT_SECRET: <encrypted-value>
  template:
    metadata:
      name: option-monitor-secrets
      namespace: nq
    type: Opaque
```

## 迁移步骤

### 阶段 1: 准备工作
1. 在 Kubernetes 集群中安装 Sealed Secrets controller
2. 下载 `kubeseal` 命令行工具
3. 获取 Sealed Secrets 公钥

### 阶段 2: 转换现有资源
1. 导出当前 Secret 资源
2. 使用 `kubeseal` 将 Secret 转换为 SealedSecret
3. 创建 `shared/` 目录结构
4. 创建 namespace.yaml 和 image-pull-secret.yaml

### 阶段 3: 更新应用配置
1. 为每个应用创建完整的 deployment.yaml
2. 创建应用特定的 app-secrets.yaml (SealedSecret)
3. 更新每个应用的 kustomization.yaml
4. 确保 application.yaml 配置正确

### 阶段 4: 部署与验证
1. 提交所有文件到 Git 仓库
2. 在 ArgoCD 中创建 root-application
3. 观察 ArgoCD 同步状态
4. 验证所有资源正确部署
5. 验证应用正常运行

### 阶段 5: 清理
1. 删除手动创建的旧资源（如果存在冲突）
2. 确认 ArgoCD 完全接管管理
3. 更新文档

## 前置条件

1. **ArgoCD 已安装** - 集群中已部署 ArgoCD
2. **Sealed Secrets Controller** - 需要在集群中安装
3. **kubeseal CLI** - 本地需要安装用于加密 Secret
4. **镜像仓库访问** - 确保 ghcr.io 凭证有效
5. **外部依赖** - EMQX 服务在 `emqx` 命名空间运行

## 成功标准

- [ ] 所有 nq 命名空间的资源都在 Git 中管理
- [ ] ArgoCD 能够自动同步并管理所有资源
- [ ] Secrets 使用 Sealed Secrets 加密存储
- [ ] 应用正常运行，功能不受影响
- [ ] 修改 Git 中的配置后，ArgoCD 自动同步到集群

## 风险与注意事项

1. **Secret 轮换** - 更新 Secret 后需要重新加密并提交
2. **镜像标签** - 需要 CI/CD 流程自动更新 deployment.yaml 中的镜像标签
3. **Sealed Secrets 密钥** - Sealed Secrets controller 的私钥需要安全备份
4. **回滚策略** - 通过 Git 回滚实现，需要测试回滚流程

## 未来扩展

- 添加 staging/prod 环境支持（使用 Kustomize overlays）
- 配置 ArgoCD Image Updater 自动更新镜像
- 添加 Health Checks 和 Notifications
- 集成 External Secrets Operator（如果需要更复杂的密钥管理）
