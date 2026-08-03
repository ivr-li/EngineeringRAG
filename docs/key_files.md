# Ключевые файлы для ревью кода

## Online-поиск

- [services/retriever/app/main.py](../services/retriever/app/main.py) -
  FastAPI поинт для поиска, переформулирования запроса и сборки ответа.
- [services/retriever/app/pipeline/search_pipeline.py](../services/retriever/app/pipeline/search_pipeline.py) -
  оркестрация этапов rewrite, retrieval, context packing и generation.
- [services/retriever/app/pipeline/services/retriever.py](../services/retriever/app/pipeline/services/retriever.py) -
  Qdrant retrieval, BGE-M3 encoding, hybrid search и reference expansion.
- [services/retriever/app/pipeline/services/context_packer.py](../services/retriever/app/pipeline/services/context_packer.py) -
  выбор контекста и учет лимита токенов.
- [services/retriever/app/pipeline/services/llm_tools.py](../services/retriever/app/pipeline/services/llm_tools.py) -
  вызовы OpenAI vLLM для rewrite и answer generation.

## Обработка данных

- [airflow/dags/batch_pipline.py](../airflow/dags/batch_pipline.py) - основной
  Airflow DAG для OCR, chunking, embedding и загрузки в Qdrant.
- [airflow/dags/batch_pipline_separated.py](../airflow/dags/batch_pipline_separated.py) -
  разделенные режимы полного пайплайна и Docling-only обработки.
- [airflow/dags/common/txt_feature/cleaner.py](../airflow/dags/common/txt_feature/cleaner.py) -
  очистка Markdown, метаданные чанков, ссылки и якори.
- [airflow/dags/common/txt_feature/table_repair.py](../airflow/dags/common/txt_feature/table_repair.py) -
  восстановление и разбиение таблиц.

## Оценка

- [services/retriever/app/eval/runner.py](../services/retriever/app/eval/runner.py) -
  запуск offline-оценки.
- [services/retriever/app/metrics/retrieval.py](../services/retriever/app/metrics/retrieval.py) -
  петрики поиска.
- [services/retriever/app/metrics/generation.py](../services/retriever/app/metrics/generation.py) -
  метрики генерации.
- [services/retriever/app/eval/configs/retrieval_baseline.json](../services/retriever/app/eval/configs/retrieval_baseline.json) -
  автоматизированный сбор метрик.

## UI и пользовательское состояние

- [ui/app.py](../ui/app.py) - основное Streamlit-приложение.
- [ui/components/retrieval](../ui/components/retrieval) - компоненты UI поиска.
- [services/user_api/app/main.py](../services/user_api/app/main.py) -
  авторизация, сессии, настройки и история поиска.

## Планирование

- [../ai_plane/current_plan.md](../ai_plane/current_plan.md) - планируемая
  архитектура workspace assistant.
- [../ai_plane/implementation_tz.md](../ai_plane/implementation_tz.md) -
  подробный план реализации следующего продуктового слоя.

