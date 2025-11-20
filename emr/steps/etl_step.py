from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, lpad
from pyspark.sql.types import *

print("==== INICIANDO JOB ETL COVID ====")

spark = SparkSession.builder.appName("etl-covid-step").getOrCreate()

# ----------------------------------------------------------------------------
# 1. RUTAS EN S3 (SEGÚN TU BUCKET REAL)
# ----------------------------------------------------------------------------
bucket = "s3-proyecto3-sd"

path_api = f"s3://{bucket}/raw/covid_api/*/*.csv"
path_rds_municipios = f"s3://{bucket}/raw/rds/municipios/*.csv"
path_rds_hospitales = f"s3://{bucket}/raw/rds/hospitales/*.csv"
path_rds_covidextra = f"s3://{bucket}/raw/rds/covid_extra/*.csv"

print("Leyendo archivos desde S3...")

# ----------------------------------------------------------------------------
# 2. LEER ARCHIVOS
# ----------------------------------------------------------------------------
df_api = spark.read.option("header", True).csv(path_api)
df_municipios = spark.read.option("header", True).csv(path_rds_municipios)
df_hospitales = spark.read.option("header", True).csv(path_rds_hospitales)
df_covidextra = spark.read.option("header", True).csv(path_rds_covidextra)

print("Archivos cargados correctamente.")
df_api.printSchema()

# ----------------------------------------------------------------------------
# 3. LIMPIEZA DE TEXTO
# ----------------------------------------------------------------------------
def clean_if_exists(df, colname):
    if colname in df.columns:
        return df.withColumn(colname, trim(upper(col(colname))))
    else:
        print(f"⚠ Advertencia: La columna '{colname}' NO existe.")
        return df

print("Limpiando columnas...")

df_api = clean_if_exists(df_api, "departamento_nom")
df_api = clean_if_exists(df_api, "ciudad_municipio_nom")
df_municipios = clean_if_exists(df_municipios, "nombre")

if "fecha_reporte_web" in df_api.columns:
    df_api = df_api.withColumn("fecha_reporte_web", to_date(col("fecha_reporte_web")))

# ----------------------------------------------------------------------------
# 4. RENOMBRAR COLUMNAS (PREFIJOS)
# ----------------------------------------------------------------------------
def prefix_columns(df, prefix):
    return df.select([col(c).alias(f"{prefix}_{c}") for c in df.columns])

print("Aplicando prefijos a las tablas...")

df_api_p = prefix_columns(df_api, "api")
df_mun_p = prefix_columns(df_municipios, "mun")
df_hosp_p = prefix_columns(df_hospitales, "hosp")
df_extra_p = prefix_columns(df_covidextra, "extra")

# ----------------------------------------------------------------------------
# 5. NORMALIZAR CÓDIGOS DANE CON PREFIJOS
# ----------------------------------------------------------------------------

# API: ciudad_municipio → codigo_dane_fixed
df_api_p = df_api_p.withColumn(
    "api_codigo_dane_fixed",
    lpad(col("api_ciudad_municipio"), 5, "0")
)

# Municipios
df_mun_p = df_mun_p.withColumn(
    "mun_codigo_dane_norm",
    lpad(col("mun_codigo_dane"), 5, "0")
)

# Hospitales: columna real = municipio_codigo
df_hosp_p = df_hosp_p.withColumn(
    "hosp_codigo_dane_norm",
    lpad(col("hosp_municipio_codigo"), 5, "0")
)

# Extra: normalizar + renombrar
df_extra_p = (
    df_extra_p
    .withColumn("extra_codigo_dane_norm", lpad(col("extra_codigo_dane"), 5, "0"))
)

# ----------------------------------------------------------------------------
# 6. JOINS
# ----------------------------------------------------------------------------

print("Unión API + Municipios...")
df_step1 = df_api_p.join(
    df_mun_p,
    df_api_p["api_codigo_dane_fixed"] == df_mun_p["mun_codigo_dane_norm"],
    "left"
)

print("Unión con Hospitales...")
df_step2 = df_step1.join(
    df_hosp_p,
    df_api_p["api_codigo_dane_fixed"] == df_hosp_p["hosp_codigo_dane_norm"],
    "left"
)

print("Unión con Covid Extra...")
df_final = df_step2.join(
    df_extra_p,
    df_api_p["api_codigo_dane_fixed"] == df_extra_p["extra_codigo_dane_norm"],
    "left"
)

print("Uniones completas. Registros finales:", df_final.count())

# ----------------------------------------------------------------------------
# 7. GUARDAR EN PARQUET
# ----------------------------------------------------------------------------

output = f"s3://{bucket}/trusted/covid_merged/"
print("Escribiendo datos en formato Parquet...")

df_final.write.mode("overwrite").parquet(output)

print("==== JOB ETL FINALIZADO EXITOSAMENTE ====")
