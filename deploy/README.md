# Deploy

ArgoCD 部署配置目录，采用 App-of-Apps 模式。

## 目录结构

```
deploy/
├── root-application.yaml          # 根 Application（App-of-Apps 入口）
├── deribit-option-monitor/        # 应用部署目录
│   ├── application.yaml           # ArgoCD Application
│   ├── deployment.yaml
│   └── kustomization.yaml
└── deribit-subscription/
    ├── application.yaml
    ├── deployment.yaml
    └── kustomization.yaml
```

## App-of-Apps 模式

- `root-application.yaml` 指向 `deploy/` 目录
- ArgoCD 会自动扫描 `deploy/` 下所有子目录的 `application.yaml`
- 每个应用的 `application.yaml` 指向自己的目录，管理该应用的所有 K8s 资源

## 添加新应用

1. 创建应用目录：`deploy/<app-name>/`
2. 添加 `application.yaml`（参考现有应用的配置）
3. 添加应用的 K8s 资源文件（deployment.yaml 等）
4. 添加 `kustomization.yaml`
5. 提交并推送，ArgoCD 会自动发现并部署

## 命名空间

所有应用统一部署在 `nq` 命名空间下。
