# RAG System for Construction Norms

Система семантического поиска по нормативно-техническим документам (СП, СНиП, ГОСТ).

## Компоненты

| Компонент | Описание | Документация |
|---|---|---|
| Ingestion Pipeline | PDF → Qdrant через MinerU + Docling | [docs/ingestion-pipeline.md](docs/ingestion-pipeline.md) |
| Retriever | Гибридный поиск(dense + sparse + ColBERT) + граф знаний | [docs/retriever.md](docs/retriever.md) |
| LLM Service | RAG-ответы на вопросы по нормам | [docs/llm-service.md](docs/llm-service.md) |

## Быстрый старт


## Архитектура
