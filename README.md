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
└── k8s/                      # общий манифест
    └── namespaces.yaml
```
