import json
import boto3
import csv
import urllib3
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):

    # 1. URL del API
    url = "https://www.datos.gov.co/resource/gt2j-8ykr.json?$limit=50000"

    http = urllib3.PoolManager()
    response = http.request("GET", url)
    data = json.loads(response.data.decode("utf-8"))

    # 2. Obtener todas las llaves presentes en el dataset
    all_keys = set()
    for row in data:
        all_keys.update(row.keys())

    # 3. Convertir JSON a CSV usando StringIO
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=list(all_keys))
    writer.writeheader()

    # Escribir una fila a la vez, ignorando campos faltantes
    for row in data:
        writer.writerow(row)

    # 4. Nombre del archivo con fecha actual
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"raw/covid_api/{today}/data.csv"

    # 5. Subir a S3
    s3 = boto3.client("s3")
    bucket = "s3-proyecto3-sd"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

    return {
        "statusCode": 200,
        "body": f"Archivo almacenado correctamente en {key}"
    }