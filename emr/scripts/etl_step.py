from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date
from pyspark.sql.types import *

spark = SparkSession.builder.appName("etl-covid-step").getOrCreate()

# ----------------------------------------------------------------------------
# 1. RUTAS EN S3 (ajustar bucket)
# ----------------------------------------------------------------------------
bucket = "s3-proyecto3-sd"

path_api = f"s3://{bucket}/raw/covid_api/*/*.csv"
path_rds_municipios = f"s3://{bucket}/raw/rds/municipios/*.csv"
path_rds_departamentos = f"s3://{bucket}/raw/rds/departamentos/*.csv"
path_rds_hospitales = f"s3://{bucket}/raw/rds/hospitales/*.csv"
path_rds_covidextra = f"s3://{bucket}/raw/rds/covid_extra/*.csv"

# ----------------------------------------------------------------------------
# 2. LEER DATOS RAW
# ----------------------------------------------------------------------------
df_api = spark.read.option("header", True).csv(path_api)
df_municipios = spark.read.option("header", True).csv(path_rds_municipios)
df_departamentos = spark.read.option("header", True).csv(path_rds_departamentos)
df_hospitales = spark.read.option("header", True).csv(path_rds_hospitales)
df_covidextra = spark.read.option("header", True).csv(path_rds_covidextra)

# ----------------------------------------------------------------------------
# 3. LIMPIEZA BÁSICA
# ----------------------------------------------------------------------------
def clean_text(df, colname):
    return df.withColumn(colname, trim(upper(col(colname))))

df_api = clean_text(df_api, "departamento_nom")
df_api = clean_text(df_api, "municipio_nom")
df_api = df_api.withColumn("fecha_reporte_web", to_date(col("fecha_reporte_web")))

df_municipios = clean_text(df_municipios, "nombre")
df_departamentos = clean_text(df_departamentos, "nombre")

# Convertir códigos a numéricos si es necesario
df_api = df_api.withColumn("codigo_dane", col("codigo_dane").cast("string"))
df_municipios = df_municipios.withColumn("codigo_dane", col("codigo_dane").cast("string"))

# ----------------------------------------------------------------------------
# 4. JOIN API + MUNICIPIOS
# ----------------------------------------------------------------------------
df_api_mun = df_api.join(
    df_municipios,
    df_api.codigo_dane == df_municipios.codigo_dane,
    "left"
)

# ----------------------------------------------------------------------------
# 5. JOIN API + DEPARTAMENTOS
# ----------------------------------------------------------------------------
df_api_dep = df_api_mun.join(
    df_departamentos,
    df_api_mun.departamento_nom == df_departamentos.nombre,
    "left"
)

# ----------------------------------------------------------------------------
# 6. JOIN CON TABLA COVID EXTRA (RDS)
# ----------------------------------------------------------------------------
df_final = df_api_dep.join(
    df_covidextra,
    df_api_dep.codigo_dane == df_covidextra.codigo_dane,
    "left"
)

# ----------------------------------------------------------------------------
# 7. ESCRITURA A TRUSTED
# ----------------------------------------------------------------------------
output = f"s3://{bucket}/trusted/covid_merged/"

df_final.write.mode("overwrite").parquet(output)

# ----------------------------------------------------------------------------
# FIN
# ----------------------------------------------------------------------------
print("ETL terminado correctamente.")
