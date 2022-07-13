FROM tiangolo/uvicorn-gunicorn:python3.8-slim

ARG MODEL_VERSION

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive
ENV MODULE_NAME=app

COPY requirements.txt .
COPY artifacts/pipeline-model-$MODEL_VERSION.pkl .
COPY ./app /app/app

RUN pip install -r requirements.txt \
    && rm -rf /root/.cache

#ENTRYPOINT ["uvicorn", "main:app"]