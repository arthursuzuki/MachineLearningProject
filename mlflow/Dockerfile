FROM python:3.10

WORKDIR /mlflow

RUN pip install mlflow boto3

CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000", "--backend-store-uri", "sqlite:///mlflow.db", "--default-artifact-root", "/mlflow"]
