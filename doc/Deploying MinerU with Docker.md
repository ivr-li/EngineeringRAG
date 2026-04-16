
## Build Docker Image using Dockerfile
```bash
wget https://gcore.jsdelivr.net/gh/opendatalab/MinerU@master/docker/global/Dockerfile
docker build -t mineru:latest -f Dockerfile .
```
## Save Docker Image to the Hub
```bash
docker tag mineru:latest dvmed/mineru:v3.0.9
docker push dvmed/mineru:v3.0.9
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
