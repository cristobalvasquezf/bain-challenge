FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

ARG MODEL_VERSION

WORKDIR /app

COPY requirements_app.txt .
COPY artifacts/pipeline-model-$MODEL_VERSION.joblib .

RUN pip install -r requirements_app.txt

COPY ./app /app/app
COPY ./src /app/src
