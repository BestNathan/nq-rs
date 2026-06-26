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
