import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime

# Inicializar contexto
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Nombre del bucket
bucket = "s3-proyecto3-sd"
base_path = f"s3://{bucket}/raw/rds/"

# Nombre de la conexión creada en Glue
connection_name = "Postgresql-connection-proyecto3"

# Lista de tablas a extraer
tables = ["municipios", "hospitales", "covid_extra"]

print("\n===== INICIANDO EXTRACCIÓN DESDE POSTGRESQL =====\n")

for table in tables:
    print(f"--> Leyendo tabla: {table}")

    # Leer tabla con JDBC usando la conexión de Glue
    df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db-proyecto3-sd.czku8jydrc1g.us-east-1.rds.amazonaws.com:5432/postgres") \
    .option("dbtable", table) \
    .option("user", "postgres") \
    .option("password", "Proyecto3-rds") \
    .option("driver", "org.postgresql.Driver") \
    .load()

    print("Schema de la tabla:")
    df.printSchema()
    
    print("Número de filas:")
    print(df.count())

    # Path de salida en S3
    output_path = f"{base_path}{table}"
    
    print(f"--> Guardando CSV en: {output_path}")
    
    # Guardar CSV único (coalesce a 1 archivo)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

print("\n===== PROCESO COMPLETADO EXITOSAMENTE =====\n")
