EngineeringRAG - локальная RAG-система для поиска по нормативно-техническим документам (СП, СНиП, ГОСТ). Позволяет задавать вопросы на естественном языке и получать ответы с точными ссылками на пункты нормативной документации. Работает полностью офлайн, оптимизирована под GPU-сервер с 12 ГБ VRAM и 32 RAM.

Решает проблему поиска по нормам и использования LLM в чистом виде. Поиск по нормам сложен и не интуитивен, особенно если не знать, где именно искать, а использование LLM часто приводит к галлюцинациям (мы в практике столкнулись с тем, что нам отказали в принятии конструктивного решения на основе несуществующего пункта в СП).

---
## Примеры
![Вопрс 1](docs/view1.png)  
![Вопрс 2](docs/view2.png)
---

## Архитектура

### Data Pipeline — Airflow DAG `batch_pipline`

| Компонент | Детали |
|-----------|--------|
| Оркестрация | Airflow 3.2 |
| Цепочка обработки | MinIO (PDF) → MinerU (OCR) → Docling (chunking) → Qdrant |
| MinerU | Распознавание текста, формул и таблиц из PDF |
| Docling | Иерархические Markdown-чанки |
| Обогащение | Извлечение ссылок на нормативы (СП/СНиП/ГОСТ), таблиц |
| Векторизация | `BAAI/bge-m3`: dense (1024d) + sparse (BM25) + ColBERT |

> Иерархический чанкинг с плавающим окном для работы с избыточностью.

### Retriever Service — Streamlit + Qdrant

| Параметр | Описание |
|----------|----------|
| Гибридный поиск | dense + sparse → ColBERT rerank |
| Режимы | `hybrid`, `dense`, `sparse` |
| `top_k` | Количество финальных результатов |
| `prefetch_k` | Кандидаты для ColBERT rerank (default: `top_k × 4`) |
| `only_tables` | Фильтр только по чанкам-таблицам |
| `use_rewriter` | Переформулирование запроса через LLM |

> Основной метод поиска — `hybrid`.

### LLM Service — `vllm-light`

| Параметр | Значение |
|----------|----------|
| Модель | `Qwen/Qwen3-4B` |
| Роль | Query rewriter + Answer composer |
| Endpoint | `localhost:8020` |

### Инфраструктура

Docker Compose: Airflow 3.2, MinIO, Qdrant, Docling Serve, vllm-light. GPU: CUDAExecutionProvider.

---

## Установка и запуск

### 1. Подготовка системы (первичный запуск)

```bash
sudo bash scripts/setup-linux.sh
```

Устанавливает Docker, NVIDIA Container Toolkit, CUDA 12.9, Python и создает структуру `data/`.

### 2. Установка репозитория 
```bash
git clone https://github.com/dvedd/EngineeringRAG
cd EngineeringRAG
```
### 3. Настройка окружения

```bash
# Создать виртуальное окружение
python -m venv venv

# Активировать окружение
source venv/bin/activate

# Установить зависимости
pip install -r requirements.txt
```

Создать `.env` файл с необходимыми секретами для локального dev-окружения (сгенерировать можно самостоятельно):

```env
# Airflow
AIRFLOW_UID=1000
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

# Secrets
AIRFLOW__API__SECRET_KEY=<your-secret-key>
AIRFLOW__CORE__FERNET_KEY=<your-fernet-key>
AIRFLOW__API_AUTH__JWT_SECRET=<your-jwt-secret>

# Database
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Superset
SUPERSET_SECRET_KEY=<your-superset-secret>

# Warehouse PostgreSQL
WAREHOUSE_PG_USER=postgres
WAREHOUSE_PG_PASSWORD=postgres
WAREHOUSE_PG_DB=warehouse

# Client PostgreSQL
CLIENT_PG_USER=postgres
CLIENT_PG_PASSWORD=postgres
CLIENT_PG_DB=postgres

# PGAdmin
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin
```

### 4. Запуск компонентов

```bash
docker compose up -d
```

Сервисы:
- Airflow UI: `http://localhost:8080` (admin:admin)
- MinIO: `http://localhost:9000` (minioadmin:minioadmin)
- MinIO Console: `http://localhost:9001`
- Qdrant: `http://localhost:6333`
- vllm-light: `http://localhost:8020`
- Docling: `http://localhost:5001`

### 5. Запуск UI сервиса

```bash
cd retriever_service
streamlit run app_v2.py
```

Адрес сервиса: http://localhost:8501

---

## Планируемые улучшения:

### Retriever Service
- [ ] Система логирования запроса и ответа с метаданными
- [ ] Метрика качества для A/B тестов
- [x] Разделение app_v2.py на модули
- [x] Тестирование размеров контекста (RTX3060, 12GB VRAM) - для квантированноый модели на 6гб 10000 свободно
- [ ] Декомпозиция запросов вместо переформулирования
- [ ] A/B тесты для проверки изменений
- [x] Поведение при отсутствии ответа в базе
- [x] FastAPI сервис для поиска
- [ ] Отладка фильтрации по имени файла
- [ ] Передача изображений из Payload в контекст
- [ ] История запросов
### Data Pipeline
- [ ] Metadata database для отслеживания файлов в MinIO (PostgreSQL)
- [ ] Переход MinerU на Redis (проблема OOM Killer)
- [ ] Терминологический словарь по строительным нормам
- [ ] Автоматическое обновление векторной БД при изменении документов в MinIO
- [ ] Очистка памяти системы (MinerU держит модели в VRAM)
- [ ] Разгрузка save_docling_results
- [ ] Оптимизация docker images
- [ ] Вынос Qdrant в отдельный сервис
- [ ] Сохранение изображений из MinerU в MinIO и запись в Payload
- [x] Обьединять разибитые в pdf таблици
