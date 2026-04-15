# EngineeringRAG

Система обработки инженерных документов.

## Архитектура

```
EngineeringRAG/
├── preprocess/               # сервис обработки файлов (PDF -> MinIO -> векторная БД)
│   ├── app/                  # код FastAPI
│   └── k8s/
│       └── deployment.yaml
│
├── minio/                    # внутреннее S3 хранилище
│   └── k8s/
│       └── minio.yaml
│
├── qdrant-db/                # векторная БД (Qdrant или Milvus)
│   └── k8s/
│       └── qdrant-db.yaml
│
├── llm‑service/              # Основной RAG-сервис
│   ├── app/                  # FastAPI‑сервис, который:
│   │                         #   - ходит в векторную БД,
│   │                         #   - собирает контекст,
│   │                         #   - вызывает LLM
│   ├── Dockerfile
│   └── k8s/
│       └── deployment.yaml
│
└── k8s/                      # общие манифесты
    ├── namespace.yaml
    ├── minio.yaml
    ├── qdrant.yaml
    └── mineru.yaml
```

## MinerU - PDF Extraction
```bash
docker compose -f compose.yaml build

# Запуск одного сервиса
docker compose -f compose.yaml --profile api up -d
docker compose -f compose.yaml --profile openai-server up -d
docker compose -f compose.yaml --profile gradio up -d

# Запуск всех сервисов
docker compose -f compose.yaml --profile all up -d
```
