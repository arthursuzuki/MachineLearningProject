from fastapi import FastAPI, UploadFile, File, HTTPException
from minio import Minio
from minio.error import S3Error

app = FastAPI()

# Conexão correta com MinIO usando keyword arguments (nova versão do SDK)
client = Minio(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

BUCKET = "raw-data"

# Verificação correta do bucket (nova assinatura)
if not client.bucket_exists(bucket_name=BUCKET):
    client.make_bucket(bucket_name=BUCKET)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Recebe um CSV ou JSON e salva no MinIO.
    """
    try:
        data = await file.read()

        client.put_object(
            bucket_name=BUCKET,
            object_name=file.filename,
            data=data,
            length=len(data),
            content_type=file.content_type
        )

        return {"status": "success", "filename": file.filename}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/list")
def list_files():
    """
    Lista arquivos no bucket.
    """
    try:
        objects = client.list_objects(BUCKET)
        files = [obj.object_name for obj in objects]
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/download/{filename}")
def download_file(filename: str):
    """
    Baixa arquivo do MinIO.
    """
    try:
        response = client.get_object(BUCKET, filename)
        return response.read()
    except S3Error:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
