# CI/CD Workflows — framecast-worker

## Fluxo geral

```
push to develop
      │
      ▼
  [release.yml] ──── PR existe? ────► update-pr (sync + changelog)
                          │
                          NO
                          ▼
                     create-pr (versão + branch + draft PR)
                          │
      merge PR to main ◄──┘
              │
              ▼
         [deploy.yml]
              │
              ▼
      docker-push (build + GHCR)
              │
              ▼
      deploy-dev (development) ──► aprovação manual
              │
              ▼
      deploy-prod (production)
              │
              ▼
         tag + GitHub Release
```

## Workflows

| Workflow | Trigger | Descrição |
|---|---|---|
| `ci.yml` | PR para `develop`/`main`, push em `develop` | Lint → Test (unit, mocks/fakes + coverage ≥85%) → Build |
| `release.yml` | Push em `develop`, `workflow_dispatch` | Cria ou atualiza PR de release |
| `deploy.yml` | PR de `release/*` mergeado em `main`, `workflow_dispatch` | Docker push → dev → prod (gate manual) → GitHub Release |
| `rollback.yml` | `workflow_dispatch` (versão + ambiente) | Redeployment da imagem de uma versão anterior |

## Composite Actions

```
.github/actions/
├── ci/
│   ├── go-lint/        go vet + golangci-lint
│   ├── go-test/        testes unitários (mocks/fakes, sem infra) + coverage gate + vulnerability check
│   └── go-build/       build binary + docker smoke test
├── release/
│   ├── create-pr/      calcula versão (conventional commits), cria branch e draft PR
│   ├── update-pr/      sincroniza branch de release com develop, atualiza changelog
│   └── finalize-tag/   cria tag anotada (chamada por create-release)
└── deploy/
    ├── docker-push/    GHCR login + build + push (tags: sha + latest)
    ├── k8s-deploy/     kubectl set image + rollout status + health check
    └── create-release/ finalize-tag + GitHub Release com changelog
```

## Configuração no repositório

Configure em **Settings → Secrets and Variables → Variables / Secrets**.
Veja [`variables.env.example`](variables.env.example) para a lista completa.

### Variáveis obrigatórias (vars.*)

| Variável | Valor esperado |
|---|---|
| `AWS_REGION` | `us-east-1` |
| `GO_VERSION` | `1.25` |
| `EKS_CLUSTER` | `framecast` |
| `K8S_NAMESPACE` | `framecast` |
| `K8S_NAMESPACE_DEV` | `framecast-dev` |
| `COVERAGE_GATE` | `85` |
| `HEALTH_ENDPOINT` | `/health` |
| `EMAIL_NOTIFICATIONS_ENABLED` | `true` — liga/desliga o envio de e-mail sem mudar o backend (`false` usa `NoOpNotifier`) |
| `NOTIFIER_BACKEND` | `smtp` (padrão) ou `ses` |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USERNAME` / `SMTP_FROM` | usados quando `NOTIFIER_BACKEND=smtp` (`SMTP_PASSWORD` é Secret) |

### Secrets obrigatórios

| Secret | Descrição |
|---|---|
| `AWS_ACCESS_KEY_ID` | Credencial AWS |
| `AWS_SECRET_ACCESS_KEY` | Credencial AWS |
| `AWS_SESSION_TOKEN` | Sessão temporária (AWS Academy / LabRole) |
| `GHCR_PAT` | PAT clássico com escopo `write:packages` — usado para push da imagem e como pull secret (`ghcr-secret`) no cluster |

## Peculiaridades do worker vs api

- **Testes 100% unitários:** todas as dependências externas (Postgres via GORM, S3/SQS/SES, FFmpeg) são mockadas via interfaces estreitas + `go-sqlmock`; o `go-test` action não precisa instalar FFmpeg nem subir infraestrutura.
- **Coverage gate:** mínimo de 85% de cobertura (`COVERAGE_GATE`), calculado sobre todos os pacotes exceto `cmd/` (wiring de bootstrap).
- **Deploy em 2 stages:** dev → prod com gate de aprovação manual no GitHub environment `production`.

## Versionamento

Versão calculada automaticamente via [Conventional Commits](https://www.conventionalcommits.org):

| Padrão de commit | Bump |
|---|---|
| `feat!:` ou `BREAKING CHANGE:` no corpo | `major` |
| `feat:` | `minor` |
| `fix:`, `chore:`, `docs:`, … | `patch` |

Tags seguem o padrão `v1.2.3` — repositórios têm namespaces de tags independentes.

## Tag e GitHub Release

A tag e o GitHub Release são criados **após** o deploy em produção ser confirmado. O `workflow_dispatch` redeploya a versão existente sem criar nova tag.

## Notas de deploy

- **Concorrência:** `group: deploy` — nunca executa dois deploys em paralelo.
- **KEDA:** o ScaledObject no EKS escala o worker conforme a profundidade da fila SQS (`framecast-processing`). `minReplicaCount: 1` garante 1 pod sempre pronto.
- **Pré-requisito:** `framecast-infra` deve estar aplicado (provê EKS, SQS, S3, SES).
- **Rollback:** usa o SHA do commit da tag para reconstruir a imagem GHCR exata (`ghcr.io/<owner>/<repo>:<commit-sha>`).
