FROM python:3.12-slim

WORKDIR /service

COPY services/user_api/requirements.txt .
RUN pip install --no-cache-dir uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

COPY services/user_api/app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9130"]
