FROM python:3.12-slim

WORKDIR /service

COPY services/retriever/requirements.txt .
RUN pip install --no-cache-dir uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

COPY services/retriever/app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9123"]
