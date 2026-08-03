# ТЗ на реализацию Workspace AI Assistant

## 1. Цель

Построить поверх текущей EngineeringRAG системы новый слой `assistant_api`,
который превращает вопросно-ответный retriever в ядро инженерного workspace:

- проектное хранилище папок и файлов;
- folder-scoped чаты;
- controlled task orchestration;
- Celery-based исполнение долгих задач;
- document audit по НТД-ссылкам;
- сохранение структурированных и человекочитаемых artifacts.

Существующий `retriever_api` остается сервисом поиска и ответа по НТД. Его не
нужно превращать в orchestrator.

## 2. Архитектурные инварианты

- Пользовательские файлы не индексируются в корпус НТД.
- LLM использует общий vLLM endpoint, а не отдельную модель на чат.
- Чат хранит историю для UI, но LLM не получает всю историю как prompt.
- Каждая задача исполняется только через зарегистрированный workflow.
- LLM возвращает только typed tool calls и structured decisions.
- Executor валидирует все входы/выходы tools.
- Долгие задачи идут через Celery workers.
- Redis не хранит продуктовую истину, только доставляет задачи.
- PostgreSQL хранит статусы, связи, ACL, summary и ссылки на artifacts.
- MinIO хранит исходные файлы, промежуточные данные и отчеты.
- По умолчанию задача видит только direct files текущей папки.
- Рекурсивный доступ к вложенным папкам запрещен без отдельного явного режима.
- Первый лимит параллелизма: 1 target file.

## 3. Итоговая структура репозитория

Добавить новые директории и файлы:

```text
services/assistant_api/
  Dockerfile
  requirements.txt
  app/
    __init__.py
    main.py
    config.py
    database.py
    dependencies.py
    schemas.py
    auth/
      __init__.py
      client.py
      schemas.py
    workspace/
      __init__.py
      repositories.py
      service.py
      schemas.py
    files/
      __init__.py
      storage.py
      repositories.py
      service.py
      schemas.py
    chat/
      __init__.py
      repositories.py
      service.py
      schemas.py
    jobs/
      __init__.py
      repositories.py
      service.py
      schemas.py
      state.py
    orchestration/
      __init__.py
      router.py
      resolver.py
      registry.py
      executor.py
      schemas.py
    tools/
      __init__.py
      registry.py
      retriever.py
      mineru.py
      llm.py
      report.py
    workflows/
      __init__.py
      qa.py
      document_audit.py
    audit/
      __init__.py
      refs.py
      verifier.py
      report.py
      schemas.py
    worker/
      __init__.py
      celery_app.py
      tasks.py

ui/
  workspace_app.py
  components/
    workspace/
      __init__.py
      tree.py
      files.py
      chat.py
      jobs.py
      artifacts.py

ai_plane/
  current_plan.md
  dialog_history.md
  implementation_tz.md
```

Дополнить существующие файлы:

- `compose.yaml`: добавить `assistant_api`, `assistant_worker`,
  `assistant_redis`.
- `services/retriever/app/main.py`: добавить внутренние tool endpoints для
  точной проверки документа/пункта НТД, если текущего `/search` недостаточно.
- `services/retriever/app/pipeline/services/retriever.py`: вынести/добавить
  методы exact norm lookup по `filename` и `anchor_refs`.

## 4. Схема сервисов

```text
Streamlit workspace UI
  -> assistant_api
      -> user_api       # auth/introspection
      -> PostgreSQL     # workspace, ACL, jobs, chat metadata
      -> MinIO          # files and artifacts
      -> Celery/Redis   # async jobs
      -> retriever_api  # NTD search and exact lookup
      -> mineru-api     # document parsing/OCR
      -> vllm-light     # router, decisions, report text
```

## 5. PostgreSQL модель

Создать отдельную schema `assistant`.

Таблицы:

```text
assistant.workspaces
- id uuid pk
- name text not null
- owner_user_id uuid not null
- created_at timestamptz not null
- updated_at timestamptz not null
- deleted_at timestamptz null

assistant.folders
- id uuid pk
- workspace_id uuid not null
- parent_id uuid null
- name text not null
- path text not null
- created_by uuid not null
- created_at timestamptz not null
- updated_at timestamptz not null
- deleted_at timestamptz null
- unique(workspace_id, parent_id, name)

assistant.files
- id uuid pk
- workspace_id uuid not null
- folder_id uuid not null
- name text not null
- mime_type text not null
- size_bytes bigint not null
- content_hash text not null
- minio_bucket text not null
- minio_key text not null
- status text not null
- created_by uuid not null
- created_at timestamptz not null
- deleted_at timestamptz null

assistant.chat_threads
- id uuid pk
- workspace_id uuid not null
- folder_id uuid not null
- owner_user_id uuid not null
- title text not null
- visibility text not null default 'private'
- created_at timestamptz not null
- updated_at timestamptz not null
- archived_at timestamptz null

assistant.chat_messages
- id uuid pk
- thread_id uuid not null
- user_id uuid null
- role text not null
- content text not null
- metadata_json jsonb not null default '{}'
- created_at timestamptz not null

assistant.jobs
- id uuid pk
- parent_job_id uuid null
- workspace_id uuid not null
- folder_id uuid not null
- thread_id uuid not null
- target_file_id uuid null
- created_by uuid not null
- workflow_type text not null
- status text not null
- stage text not null
- progress int not null default 0
- request_text text not null
- summary text not null default ''
- error text null
- metadata_json jsonb not null default '{}'
- created_at timestamptz not null
- started_at timestamptz null
- finished_at timestamptz null

assistant.subtasks
- id uuid pk
- job_id uuid not null
- name text not null
- resource_queue text not null
- status text not null
- attempt int not null default 0
- progress int not null default 0
- input_json jsonb not null default '{}'
- output_json jsonb not null default '{}'
- error text null
- created_at timestamptz not null
- started_at timestamptz null
- finished_at timestamptz null

assistant.artifacts
- id uuid pk
- workspace_id uuid not null
- folder_id uuid not null
- job_id uuid null
- source_file_id uuid null
- artifact_type text not null
- name text not null
- mime_type text not null
- minio_bucket text not null
- minio_key text not null
- metadata_json jsonb not null default '{}'
- created_by uuid not null
- created_at timestamptz not null

assistant.acl_entries
- id uuid pk
- resource_type text not null
- resource_id uuid not null
- subject_type text not null default 'user'
- subject_id uuid not null
- role text not null
- created_by uuid not null
- created_at timestamptz not null

assistant.tool_calls
- id uuid pk
- job_id uuid null
- subtask_id uuid null
- tool_name text not null
- input_json jsonb not null
- output_json jsonb null
- status text not null
- error text null
- latency_ms int null
- created_at timestamptz not null
```

Базовые роли ACL:

- `owner`: полный доступ;
- `admin`: управление папками, файлами, ACL, jobs;
- `editor`: загрузка файлов, запуск jobs, свои threads;
- `viewer`: просмотр файлов, artifacts и разрешенных read-only threads.

Файлы, jobs, artifacts и threads наследуют ACL папки.

## 6. MinIO layout

Использовать один bucket для assistant artifacts, например `assistant-files`.

Ключи:

```text
assistant/{workspace_id}/folders/{folder_id}/files/{file_id}/source/{filename}
assistant/{workspace_id}/folders/{folder_id}/jobs/{job_id}/parse/{artifact_id}.md
assistant/{workspace_id}/folders/{folder_id}/jobs/{job_id}/audit/{artifact_id}.json
assistant/{workspace_id}/folders/{folder_id}/jobs/{job_id}/audit/{artifact_id}.md
assistant/{workspace_id}/folders/{folder_id}/jobs/{job_id}/audit/{artifact_id}.pdf
```

Правило: путь в MinIO не является источником прав. Права проверяются только через
PostgreSQL ACL.

## 7. API `assistant_api`

Все endpoints требуют bearer token. Auth проверяется через `user_api`.

Workspace:

```text
GET    /workspaces
POST   /workspaces
GET    /workspaces/{workspace_id}
PATCH  /workspaces/{workspace_id}
```

Folders:

```text
GET    /workspaces/{workspace_id}/folders/{folder_id}/children
POST   /workspaces/{workspace_id}/folders
PATCH  /folders/{folder_id}
DELETE /folders/{folder_id}
```

Files:

```text
GET    /folders/{folder_id}/files
POST   /folders/{folder_id}/files
GET    /files/{file_id}
GET    /files/{file_id}/download
DELETE /files/{file_id}
```

Chat:

```text
GET    /folders/{folder_id}/threads
POST   /folders/{folder_id}/threads
GET    /threads/{thread_id}/messages
POST   /threads/{thread_id}/messages
```

Jobs:

```text
GET    /jobs/{job_id}
GET    /jobs/{job_id}/subtasks
GET    /jobs/{job_id}/artifacts
POST   /jobs/{job_id}/cancel
```

Artifacts:

```text
GET    /artifacts/{artifact_id}
GET    /artifacts/{artifact_id}/download
```

## 8. Auth integration

Первый вариант:

- `assistant_api` принимает `Authorization: Bearer <token>`;
- вызывает `GET user_api/me`;
- получает `user_id`, `email`, `display_name`;
- кэширует успешный результат на короткое время, например 60 секунд.

Если нагрузки станет больше, добавить internal endpoint в `user_api`:

```text
POST /internal/auth/introspect
```

Он должен возвращать `user_id`, `session_id`, `is_active`, `expires_at`.

## 9. Tool registry

Каждый tool описывается в коде, а не в prompt.

Минимальные поля:

```text
name
description
allowed_workflows
input_schema
output_schema
resource_queue
timeout_seconds
retry_policy
requires_acl
```

Первый набор tools:

```text
retriever.search
retriever.resolve_norm_doc
retriever.get_norm_section
mineru.parse_file
llm.route_intent
llm.extract_audit_refs
llm.verify_claim
report.render_markdown
report.export_pdf
artifact.save
```

LLM видит только имя, описание и JSON schema. Секреты и URLs хранятся в config.

## 10. Router и context resolver

Context resolver работает перед router.

Вход:

```text
thread_id
folder_id
message_text
selected_file_ids
last_active_job_id
last_active_artifact_id
```

Выход:

```text
folder_id
visible_direct_file_ids
target_file_id | null
referenced_job_id | null
referenced_artifact_id | null
short_memory
```

Правила:

- если выбран один файл, он становится `target_file_id`;
- если выбрано несколько файлов, создать batch parent job с отдельными target jobs;
- если файл не выбран и в папке один подходящий direct file, можно использовать его;
- если файлов несколько и цель неясна, вернуть clarification вместо запуска job;
- не читать содержимое всех файлов папки до запуска конкретного workflow.

Router возвращает structured JSON:

```json
{
  "intents": ["document_audit"],
  "needs_clarification": false,
  "clarification_question": "",
  "target_file_id": "uuid",
  "reason": "User asks to analyze selected document against norms"
}
```

Разрешенные intents первого этапа:

- `normative_qa`;
- `document_audit`;
- `unsupported`;
- `clarification`.

## 11. Workflow: normative QA

Назначение: сохранить текущий Q&A сценарий внутри нового folder-scoped чата.

Шаги:

1. Принять сообщение пользователя.
2. Context resolver определяет, нет ли ссылки на прошлый artifact/evidence.
3. Router выбирает `normative_qa`.
4. `assistant_api` вызывает `retriever_api /search`.
5. Ответ сохраняется в `chat_messages`.
6. Краткая запись job не обязательна для простого синхронного Q&A, но tool call
   trace сохраняется.

Важно: Q&A не должен автоматически читать все файлы папки.

## 12. Workflow: document audit по НТД

### 12.1. Parent job

Создать job:

```text
workflow_type = document_audit
status = queued
stage = created
target_file_id = выбранный файл
```

Subtasks:

```text
parse_file           -> document_parse
extract_refs         -> llm_short
resolve_and_search   -> retrieval
verify_refs          -> llm_short
compose_report       -> llm_long
export_pdf           -> report_export
save_artifacts       -> quick_io
```

### 12.2. parse_file

MinerU-first стратегия:

- скачать target file из MinIO во временную директорию worker;
- если файл PDF/image, отправить в `mineru-api /tasks`;
- если файл DOCX, сначала экспортировать в PDF через LibreOffice headless, затем
  отправить PDF в MinerU;
- использовать параметры текущего Airflow pipeline:
  - `lang_list=east_slavic`;
  - `backend=pipeline`;
  - `parse_method=ocr`;
  - `formula_enable=true`;
  - `table_enable=true`;
  - `return_md=true`;
- polling результата MinerU;
- сохранить Markdown в MinIO как parse artifact;
- обновить progress job.

Защита:

- timeout на весь parse;
- timeout на polling;
- retry с ограничением attempts;
- при повторном запуске проверять, есть ли уже parse artifact;
- если MinerU зависает или падает, subtask получает `failed`, job получает
  понятную ошибку.

### 12.3. extract_refs

Комбинировать regex и LLM:

- regex извлекает кандидаты ссылок: СП, ГОСТ, СНиП, СанПиН, пункт, таблица;
- LLM decision node нормализует кандидаты и локальные утверждения;
- для каждого кандидата сохранить source span и excerpt.

Схема `AuditRef`:

```text
id
source_text
source_excerpt
norm_ref_raw
norm_doc
section_ref
table_ref
quoted_text
claim_text
page_number null
confidence
```

### 12.4. resolve_and_search

Для каждого `AuditRef`:

1. `retriever.resolve_norm_doc(norm_doc)`.
2. Если документ не найден, verdict candidate = `document_not_found`.
3. Если есть `section_ref` или `table_ref`, вызвать
   `retriever.get_norm_section`.
4. Если section/table не найден, verdict candidate = `section_not_found`.
5. Если exact evidence найден, передать его в verifier.
6. Если exact evidence не найден, сделать semantic search по:
   - `norm_doc`;
   - `section_ref`;
   - `quoted_text`;
   - `claim_text`.

Для `retriever_api` добавить внутренние endpoints:

```text
POST /internal/norms/resolve
POST /internal/norms/section
```

`resolve` ищет документ по нормализованному номеру и filename/title metadata.
`section` ищет по `filename` и `anchor_refs` вида `section:x`, `table:x`.

### 12.5. verify_refs

Verifier получает:

```text
AuditRef
retrieved evidence
exact section evidence if any
```

Выход `AuditVerdict`:

```text
ref_id
verdict
reason
confidence
evidence_items
suggested_fix
```

Правила:

- если документ не найден: `document_not_found`;
- если документ найден, но пункт/таблица нет: `section_not_found`;
- если пункт найден, но quoted_text не совпадает: `quote_mismatch`;
- если цитата совпадает, но claim_text не поддерживается: `claim_unsupported`;
- если evidence слабый или неоднозначный: `insufficient_context`;
- если evidence подтверждает ссылку и утверждение: `confirmed`.

LLM verifier не имеет права ссылаться на знания вне evidence.

### 12.6. compose_report

Создать `audit_result.json`:

```text
job_id
source_file
checked_at
corpus_label = current
qdrant_collection
summary
stats
findings[]
```

Создать Markdown:

```text
# Проверка НТД-ссылок

## Сводка

## Таблица проверок

| Статус | Ссылка | Фрагмент документа | Причина | Evidence |

## Проблемные места

## Подтвержденные ссылки

## Ограничения проверки
```

PDF экспортировать из Markdown/HTML. LaTeX не использовать на первом этапе.

## 13. Streamlit UI

Первый UI может быть грубым, но должен позволять проверить сценарий.

Экран:

```text
left: folder tree
middle: direct files текущей папки
right: folder-scoped chat
bottom/right: artifacts and completed reports
```

Поведение:

- при выборе папки загрузить direct files и threads;
- файл можно загрузить в текущую папку;
- один файл можно выбрать как active target;
- сообщение отправляется в текущий thread;
- если router запускает job, UI показывает progress через polling;
- после завершения progress block скрывается/сворачивается;
- финальный Markdown показывается как ответ в чате;
- artifacts доступны рядом с файлом и в истории job.

Не делать сейчас:

- drag-and-drop сложного дерева;
- recursive folder analysis;
- context files;
- совместное редактирование одного thread;
- красивый production frontend.

## 14. Celery конфигурация

Добавить queues:

```text
quick_io
document_parse
retrieval
llm_short
llm_long
report_export
```

Настройки по умолчанию:

```text
ASSISTANT_MAX_TARGET_FILES=1
ASSISTANT_DOCUMENT_PARSE_CONCURRENCY=1
ASSISTANT_LLM_SHORT_CONCURRENCY=1
ASSISTANT_LLM_LONG_CONCURRENCY=1
ASSISTANT_RETRIEVAL_CONCURRENCY=2
ASSISTANT_JOB_TIMEOUT_SECONDS=7200
ASSISTANT_MINERU_TIMEOUT_SECONDS=5400
```

Для маленького клиента все concurrency можно держать равным 1. Для крупного
клиента увеличить workers/queues без изменения workflow.

## 15. Docker Compose

Добавить profile `assistant`.

Сервисы:

```text
assistant_redis
assistant_api
assistant_worker
```

`assistant_api` зависит от:

- postgres;
- minio;
- user_api;
- retriever_api;
- assistant_redis.

`assistant_worker` зависит от:

- assistant_redis;
- postgres;
- minio;
- retriever_api;
- mineru-api;
- vllm-light.

Важно: `assistant_worker` должен иметь доступ к временной директории, но не
хранить результат локально после завершения subtask.

## 16. Гайд добавления нового режима

Каждый новый режим добавлять строго по шагам:

1. Добавить workflow type в registry.
2. Описать input/output schema.
3. Описать allowed tools.
4. Описать resource queues.
5. Добавить decision nodes, если они нужны.
6. Добавить Celery subtasks.
7. Добавить artifact schema.
8. Добавить UI entrypoint.
9. Добавить eval формат.
10. Добавить acceptance criteria.

Запрещено:

- давать LLM произвольный список действий;
- давать LLM секреты;
- разрешать LLM читать все файлы workspace;
- добавлять новый режим как набор prompt'ов без typed schemas.

## 17. Пошаговый roadmap реализации

### Этап 1. Фундамент `assistant_api`

- Создать `services/assistant_api`.
- Добавить FastAPI app, config, database metadata.
- Добавить auth client к `user_api /me`.
- Добавить таблицы `assistant.*`.
- Добавить базовые repositories/services.
- Добавить Dockerfile и requirements.
- Добавить сервис в `compose.yaml`.

Готово, если:

- `/health` отвечает;
- bearer token проверяется через `user_api`;
- можно создать workspace/folder;
- данные сохраняются в PostgreSQL.

### Этап 2. Workspace files

- Добавить upload/download файлов в папку.
- Сохранять файл в MinIO.
- Сохранять metadata в `assistant.files`.
- Добавить soft delete.
- Добавить ACL checks.

Готово, если:

- пользователь загружает файл в папку;
- другой пользователь без прав файл не видит;
- MinIO key и PostgreSQL metadata связаны.

### Этап 3. Folder-scoped chat

- Добавить threads и messages.
- Реализовать own-write/read ACL.
- Привязать thread к folder.
- Не подавать всю историю в LLM.

Готово, если:

- пользователь создает личный thread в папке;
- пишет только в свой thread;
- видит чужие read-only threads только при наличии прав.

### Этап 4. Celery runtime

- Добавить `assistant_redis`.
- Добавить Celery app.
- Добавить queues.
- Добавить job/subtask state transitions.
- Добавить polling endpoint `/jobs/{job_id}`.

Готово, если:

- job создается в PostgreSQL;
- worker забирает dummy task;
- progress обновляется;
- UI/API может читать status.

### Этап 5. Tool registry и typed calls

- Добавить tool registry.
- Добавить Pydantic input/output schemas.
- Добавить trace `assistant.tool_calls`.
- Реализовать clients для `retriever_api`, `mineru-api`, vLLM и MinIO artifacts.

Готово, если:

- executor может вызвать registered tool;
- неизвестный tool отклоняется;
- невалидный input отклоняется до вызова сервиса;
- tool call trace сохраняется.

### Этап 6. Router и context resolver

- Реализовать bounded context resolver.
- Реализовать LLM router со structured JSON.
- Добавить fallback на clarification/unsupported.
- Добавить intent `normative_qa` и вызов текущего retriever.

Готово, если:

- обычный вопрос по НТД работает через новый чат;
- запрос с выбранным файлом маршрутизируется в `document_audit`;
- неоднозначный запрос возвращает уточнение.

### Этап 7. MinerU-first parser worker

- Реализовать `mineru.parse_file`.
- Для DOCX добавить export через LibreOffice headless в parser worker image.
- Повторить параметры OCR из Airflow pipeline.
- Сохранять Markdown artifact.
- Добавить timeout/retry/idempotency.

Готово, если:

- PDF и DOCX дают Markdown artifact;
- падение MinerU переводит subtask/job в понятный failed state;
- повторный запуск не дублирует уже готовый parse artifact.

### Этап 8. Retriever norm tools

- Добавить в `retriever_api` internal endpoints для:
  - resolve norm document;
  - get section/table by anchor_refs.
- Использовать текущие поля payload: `filename`, `anchor_refs`, `section_path`,
  `headings`, `text`.
- Не менять общий `/search` контракт без необходимости.

Готово, если:

- `СП 63.13330` резолвится в локальный файл, если он есть;
- `section:10.3` ищется exact lookup;
- если exact lookup не сработал, semantic search остается fallback.

### Этап 9. Document audit workflow

- Реализовать extraction refs.
- Реализовать resolve/search по каждой ссылке.
- Реализовать verifier и verdict schema.
- Реализовать JSON result.
- Реализовать Markdown report.
- Реализовать PDF export.

Готово, если:

- один target file проходит полный audit;
- по каждой ссылке есть verdict;
- artifacts сохранены в MinIO и видны в UI;
- финальный Markdown появляется в чате.

### Этап 10. Streamlit workspace UI

- Добавить `ui/workspace_app.py`.
- Добавить дерево папок.
- Добавить список direct files.
- Добавить upload.
- Добавить active target selection.
- Добавить threads/messages.
- Добавить job polling.
- Добавить отображение artifacts.

Готово, если:

- пользователь проходит сценарий end-to-end без ручных API-запросов;
- текущий Q&A UI не сломан;
- workspace UI можно заменить позже через backend API.

### Этап 11. Eval foundation

- Создать формат gold dataset для audit.
- Добавить runner для extraction/verdict/evidence метрик.
- Сначала поддержать маленький набор ручных примеров.

Готово, если:

- можно прогнать audit eval на 1-2 подготовленных файлах;
- отчет показывает ошибки extraction, retrieval и verdict отдельно.

## 18. Acceptance criteria первого вертикального среза

- Пользователь создает workspace и папку.
- Пользователь загружает PDF или DOCX.
- Пользователь выбирает один файл как target.
- Пользователь пишет в чат естественный запрос на проверку.
- Система создает `document_audit` job.
- UI показывает progress polling.
- Worker парсит документ через MinerU-first pipeline.
- Система извлекает НТД-ссылки.
- Система проверяет их через локальный корпус НТД.
- По каждой ссылке есть строгий verdict.
- JSON, MD и PDF artifacts сохранены в MinIO.
- Markdown отчет показан в чате.
- Progress скрыт или свернут после завершения.
- Follow-up вопрос по НТД в том же чате маршрутизируется в `retriever_api`.

## 19. Риски

- MinerU может зависать или не освобождать ресурсы. Нужны timeouts, retries и
  worker restart policy.
- Без реестра НТД нельзя честно говорить об актуальности во внешнем мире.
  Формулировка отчета должна ссылаться на локальный корпус.
- Различение `document_not_found` и `section_not_found` зависит от качества
  document resolver по filenames/metadata.
- Streamlit будет ограничивать UX дерева и чатов. Это допустимо для первого
  слоя.
- ACL нужно заложить сразу, иначе потом будет сложная миграция данных и прав.

## 20. Что не делать в первом слое

- Не делать свободный agent loop.
- Не делать отдельную LLM на чат.
- Не скармливать LLM всю историю чата.
- Не индексировать пользовательские файлы в НТД.
- Не делать recursive folder analysis.
- Не добавлять context files.
- Не делать проектную библиотеку решений.
- Не делать орфографию внутри первого audit workflow.
- Не делать production React frontend.
- Не строить полноценное версионирование НТД до появления данных и процесса
  обновления корпуса.

