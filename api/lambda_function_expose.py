import json
import boto3
import csv
import io

BUCKET = "s3-proyecto3-sd"
BASE_PREFIX = "refined/indicadores/"

s3 = boto3.client("s3")

def read_csv_folder(prefix):
    """Lee todos los CSV de una carpeta y devuelve lista de dicts."""
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

        if "Contents" not in response:
            return []

        all_rows = []

        for obj in response["Contents"]:
            key = obj["Key"]

            # Solo procesar archivos CSV
            if not key.endswith(".csv"):
                continue

            file_obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = file_obj["Body"].read().decode("utf-8")

            reader = csv.DictReader(io.StringIO(data))
            rows = list(reader)
            all_rows.extend(rows)

        return all_rows

    except Exception as e:
        return {"error": str(e)}


def lambda_handler(event, context):
    try:
        # Leer todas las carpetas analíticas
        result = {
            "casos_por_departamento": read_csv_folder(BASE_PREFIX + "casos_por_departamento/"),
            "casos_por_fecha": read_csv_folder(BASE_PREFIX + "casos_por_fecha/"),
            "casos_por_municipio": read_csv_folder(BASE_PREFIX + "casos_por_municipio/"),
            "tasa_mortalidad": read_csv_folder(BASE_PREFIX + "tasa_mortalidad/"),
            "tasa_recuperacion": read_csv_folder(BASE_PREFIX + "tasa_recuperacion/"),
            "top_10_municipios": read_csv_folder(BASE_PREFIX + "top_10_municipios/")
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(result)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }