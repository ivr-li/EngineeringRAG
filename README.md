# EngineeringRAG

Локальная RAG-система, ориентированная на production-подход, для поиска по
российской нормативно-технической документации в строительстве: OCR,
структурный чанкинг, гибридный поиск, ColBERT reranking, локальная
LLM-генерация, трассировка и offline-оценка качества.

Проект решает прикладную задачу поиска по СП, СНиП, ГОСТ и внутренним
инженерным материалам. При этом основной инженерный фокус шире: построение
надежного локального RAG-сервиса для сложных технических документов без
обращения к внешним API.

## Красткое описание

Репозиторий показывает практическую разработку LLM/RAG-системы:

| Область | Что реализовано |
|---------|-----------------|
| Пайплайн данных | MinIO -> Airflow -> MinerU OCR -> Docling chunking -> enriched chunks |
| Поиск | BAAI/bge-m3 dense + sparse-векторы, Qdrant hybrid search, ColBERT rerank |
| Контекст | Расширение по `anchor_refs`, `cross_refs`, таблицам и соседним разделам |
| Генерация | Локальная Qwen3-4B-AWQ через vLLM, ответ только по найденному контексту |
| Бэкенд | FastAPI-сервис поиска, асинхронное логирование в PostgreSQL, trace artifacts в MinIO |
| UI | Streamlit-интерфейс с авторизацией, историей поиска и обратной связью |
| Оценка | Offline-метрики поиска и генерации на доменном QA-наборе |

Куда смотреть:
- [Архитектурные решения](docs/architecture_decisions.md) - ключевые технические
  компромиссы.
- [Оценка качества](docs/eval_note.md) - метрики, ограничения и как читать
  результаты.
- [Ключевые файлы](docs/key_files.md) - где смотреть реализацию.
- [План AI-assistant слоя](ai_plane/current_plan.md) - планируемое расширение до
  ai аудита документов.

## Текущий статус

Рабочая end-to-end локальная RAG-система:

- пайплайн индексации PDF/Markdown документов;
- FastAPI retriever с переформулированием запросов, гибридным поиском,
  reranking и генерацией ответа;
- Streamlit UI и user API;
- трассировка запросов в PostgreSQL и MinIO;
- offline-инструменты оценки качества.

Планируемое расширение: workspace-first AI assistant для проверки
пользовательских документов. Пользовательские файлы будут проверяться по
локальному корпусу НТД, но не будут смешиваться с индексом нормативной базы.

## Зачем это нужно

Инженерам часто нужно найти конкретный пункт, таблицу или ограничение в больших
технических нормативах. Обычный поиск по PDF требует почти точного совпадения
формулировки, а универсальные LLM без привязки к базе могут уверенно ссылаться
на несуществующие пункты.

EngineeringRAG ищет подтверждения в локальном корпусе документов и формирует
ответ с проверяемыми источниками.

Система не является юридическим источником истины и не заменяет эксперта. Ее
задача - быстро найти основание в локальной нормативной базе и показать, на
какие документы и фрагменты опирается ответ.

## Что реализовано

| Возможность | Назначение |
|-------------|------------|
| Обработка PDF из MinIO | Нормативы можно добавлять в локальное хранилище |
| MinerU OCR | Извлечение текста, формул и таблиц из сложных PDF |
| Docling chunking | Структурные чанки с заголовками и метаданными |
| Обработка таблиц | Крупные и разбитые таблицы получают отдельные поисковые окна |
| BAAI/bge-m3 embeddings | Dense, sparse и ColBERT-представления |
| Qdrant hybrid search | Dense + sparse prefetch с ColBERT MaxSim rerank |
| Reference expansion | Связанные таблицы, разделы и приложения подтягиваются в контекст |
| Query rewriting | Пользовательский вопрос переформулируется под технический поиск |
| Answer composition | Markdown-ответ формируется только по найденному контексту |
| Trace logging | Поиск, контекст и ответ сохраняются для анализа |
| Offline eval | Метрики поиска и генерации для регрессионной проверки |

## Архитектура

```text
PDF / Markdown
    |
    v
MinIO -> Airflow DAG -> MinerU OCR -> Docling chunking
    |                                      |
    |                                      v
    +------------------------------> enriched chunks
                                           |
                                           v
                               BAAI/bge-m3 embeddings
                         dense + sparse + ColBERT vectors
                                           |
                                           v
                                        Qdrant
                                           |
                                           v
User -> Streamlit UI -> retriever_api -> hybrid search + ref expansion
                            |              |
                            |              v
                            |         vllm-light / Qwen3-4B-AWQ
                            v
                         user_api / PostgreSQL
```

Основные компоненты:

| Компонент | Роль |
|-----------|------|
| `airflow/dags/batch_pipline.py` | Основной DAG индексации |
| `airflow/dags/batch_pipline_separated.py` | Разделенные режимы полного пайплайна и Docling-only обработки |
| `services/retriever` | FastAPI-сервис поиска и генерации ответа |
| `services/user_api` | Авторизация, сессии, настройки и история поиска |
| `ui` | Streamlit-интерфейс |
| `compose.yaml` | Локальная инфраструктура: Airflow, MinIO, Qdrant, vLLM, PostgreSQL |

## Как работает поиск

1. Пользователь задает вопрос.
2. LLM при необходимости переформулирует вопрос под технический поиск.
3. Retriever строит dense, sparse и ColBERT-представления запроса.
4. Qdrant выполняет гибридный поиск.
5. ColBERT MaxSim пересортировывает кандидатов.
6. Пайплайн расширяет контекст по структурным ссылкам: `anchor_refs`,
   `cross_refs`, `table_id`.
7. Контекст упаковывается с учетом лимита токенов модели.
8. LLM генерирует ответ только по переданному контексту.
9. Результаты, контекст и трассировка сохраняются для анализа.

## Оценка качества

Текущий eval-набор содержит 200 доменных вопросов по архитектурным и
конструктивным темам. Набор используется в первую очередь для регрессионного
сравнения конфигураций поиска и генерации.

| Метрика | Значение |
|---------|----------|
| Вопросов | 200 |
| `de_recall@5` | 0.85 |
| `de_recall@10` | 0.92 |
| `ee_recall` | 0.85 |
| `ce_recall` | 0.83 |
| `MRR` | 0.69 |
| `faithfulness` | 0.93 |
| `answer_relevance` | 0.95 |

Важное ограничение: набор данных поддерживается внутри проекта и должен
рассматриваться как инженерный regression set, а не как внешний отраслевой
benchmark. Подробнее: [docs/eval_note.md](docs/eval_note.md).

## Ревью без запуска

Полный запуск требует GPU, локальных моделей и набора сервисов. Основная логика реализование в файлах ниже.

- [services/retriever/app/pipeline/search_pipeline.py](services/retriever/app/pipeline/search_pipeline.py)
- [services/retriever/app/pipeline/services/retriever.py](services/retriever/app/pipeline/services/retriever.py)
- [services/retriever/app/pipeline/services/context_packer.py](services/retriever/app/pipeline/services/context_packer.py)
- [services/retriever/app/pipeline/services/llm_tools.py](services/retriever/app/pipeline/services/llm_tools.py)
- [airflow/dags/batch_pipline.py](airflow/dags/batch_pipline.py)
- [airflow/dags/common/txt_feature/cleaner.py](airflow/dags/common/txt_feature/cleaner.py)
- [services/retriever/app/metrics](services/retriever/app/metrics)

Скриншоты и примеры ответов:

- [docs/view1.png](docs/view1.png)
- [docs/view2.png](docs/view2.png)
- [docs/example-answer-masonry-bond.png](docs/example-answer-masonry-bond.png)
- [docs/example-answer-pile-boreholes.png](docs/example-answer-pile-boreholes.png)
- [docs/example-answer-masonry-joints.png](docs/example-answer-masonry-joints.png)

## Технический стек

- Python 3.12
- FastAPI, Pydantic, SQLAlchemy async
- Streamlit
- Airflow 3.2
- MinIO / S3-compatible storage
- Qdrant
- PostgreSQL
- MinerU
- Docling Serve
- BAAI/bge-m3
- Qwen/Qwen3-4B-AWQ через vLLM OpenAI-compatible API
- Docker Compose

## Системные требования

Рекомендуемое локальное окружение:

| Ресурс | Рекомендация |
|--------|--------------|
| GPU | NVIDIA RTX 3060 или выше |
| VRAM | 12 GB+ |
| RAM | 32 GB+ |
| Disk | 100 GB+ SSD |
| OS | Linux с Docker и NVIDIA Container Toolkit |

На одной GPU с 12 GB VRAM обработку документов и online-генерацию лучше
разводить по времени.

## Быстрый старт

### 1. Склонировать репозиторий

```bash
git clone https://github.com/dvedd/EngineeringRAG
cd EngineeringRAG
```
### 2. Подготовить Linux-хост

```bash
sudo bash scripts/setup-linux.sh
```

Скрипт устанавливает Docker, NVIDIA Container Toolkit, CUDA-зависимости и
создает локальную структуру `data/`.


### 3. Создать `.env`

Минимальный пример для локального запуска:

```env
AIRFLOW_UID=1000
AIRFLOW_GID=1000
AIRFLOW__API__SECRET_KEY=change-me

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

TRACE_MINIO_ACCESS_KEY=trace_logger
TRACE_MINIO_SECRET_KEY=change-me
TRACE_MINIO_BUCKET=ragfiles

HF_TOKEN=
LIGHT_MODEL=Qwen/Qwen3-4B-AWQ
```

Для постоянного окружения замените значения `change-me` на реальные секреты.

### 4. Запустить backend-сервисы поиска

```bash
# Запустить базовые сервисы и сервисы поиска
docker compose --profile base --profile search up -d

# Запустить только базовые сервисы
docker compose --profile base up -d

# Запустить только сервисы поиска
docker compose --profile search up -d

# Запустить только сервисы обработки данных
docker compose --profile base --profile processing up -d
```

### 5. Запустить Streamlit UI

UI запускается локально поверх backend-сервисов:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

streamlit run ui/app.py --server.port 8501
```

После запуска UI будет доступен на `http://localhost:8501`.


## Адреса сервисов

| Сервис | URL |
|--------|-----|
| Streamlit UI | `http://localhost:8501` |
| Retriever API | `http://localhost:9123` |
| User API | `http://localhost:9130` |
| vLLM light | `http://localhost:8020` |
| Qdrant | `http://localhost:6333` |
| MinIO API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |
| Airflow UI | `http://localhost:8080` |
| Docling Serve | `http://localhost:5001` |

## Пример API-запроса

```bash
curl -X POST http://localhost:9123/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Как располагать стыки рабочей арматуры внахлестку?",
    "rewrite_system_prompt": "",
    "compose_system_prompt": "",
    "use_rewriter": false,
    "top_k": 4,
    "prefetch_k": 40,
    "mode": "hybrid"
  }'
```

## План развития

EngineeringRAG планируется развивать из вопростно ответной системы в полноценного ассисистента для инженерных документов:

- document audit для устаревших, отсутствующих или неподтвержденных ссылок на
  нормативы;
- проверка по внутренним библиотекам проектных решений;
- структурированные отчеты;

Подробный план лежит в [ai_plane](ai_plane).

## Ограничения

- Качество зависит от полноты и актуальности локального корпуса НТД.
- Система не заменяет экспертную проверку первоисточников.
- Таблицы остаются самым сложным типом контента, несмотря на отдельную
  обработку.
- Одна GPU с 12 GB VRAM требует аккуратного планирования OCR, embedding и
  LLM-нагрузки.
