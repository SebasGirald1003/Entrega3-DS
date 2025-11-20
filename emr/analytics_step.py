from pyspark.sql import SparkSession
import datetime

print("===== INICIANDO ANALÍTICA COVID =====")

spark = (SparkSession.builder
         .appName("analytics-covid-step")
         .enableHiveSupport()
         .getOrCreate())

bucket = "s3-proyecto3-sd"

date = datetime.date.today().strftime("%Y-%m-%d")

# Ruta del trusted (CSV)
trusted_path = f"s3://{bucket}/trusted/covid_merged/{date}/*.csv"

print("Cargando dataset trusted desde CSV...")
df = spark.read.option("header", True).csv(trusted_path)

print("Dataset cargado. Filas:", df.count())

# Registrar tabla temporal
df.createOrReplaceTempView("covid")

# =====================================================
# 1. CASOS TOTALES POR DEPARTAMENTO
# Campo: api_departamento_nom
# =====================================================

sql_dep = """
    SELECT api_departamento_nom AS departamento,
           COUNT(*) AS casos_totales
    FROM covid
    WHERE api_departamento_nom IS NOT NULL AND api_departamento_nom <> ''
    GROUP BY api_departamento_nom
    ORDER BY casos_totales DESC
"""

df_dep = spark.sql(sql_dep)
df_dep.write.mode("overwrite").csv(
    f"s3://{bucket}/refined/indicadores/casos_por_departamento/",
    header=True
)


# =====================================================
# 2. CASOS POR MUNICIPIO
# Campo: api_ciudad_municipio_nom
# =====================================================

sql_mun = """
    SELECT api_ciudad_municipio_nom AS municipio,
           COUNT(*) AS casos
    FROM covid
    WHERE api_ciudad_municipio_nom IS NOT NULL AND api_ciudad_municipio_nom <> ''
    GROUP BY api_ciudad_municipio_nom
    ORDER BY casos DESC
"""

df_mun = spark.sql(sql_mun)
df_mun.write.mode("overwrite").csv(
    f"s3://{bucket}/refined/indicadores/casos_por_municipio/",
    header=True
)


# =====================================================
# 3. CASOS POR FECHA
# Campo: api_fecha_reporte_web
# =====================================================

sql_fecha = """
    SELECT api_fecha_reporte_web AS fecha,
           COUNT(*) AS casos
    FROM covid
    WHERE api_fecha_reporte_web IS NOT NULL AND api_fecha_reporte_web <> ''
    GROUP BY api_fecha_reporte_web
    ORDER BY fecha
"""

df_fecha = spark.sql(sql_fecha)
df_fecha.write.mode("overwrite").csv(
    f"s3://{bucket}/refined/indicadores/casos_por_fecha/",
    header=True
)


# =====================================================
# 4. TASA DE MORTALIDAD POR DEPARTAMENTO
# Campo de evento: api_estado ('Fallecido')
# Campo de grupo: api_departamento_nom
# =====================================================

sql_mortalidad = """
    SELECT api_departamento_nom AS departamento,
           SUM(CASE WHEN lower(api_estado) = 'fallecido' THEN 1 ELSE 0 END)
               * 100.0 / COUNT(*) AS tasa_mortalidad
    FROM covid
    WHERE api_departamento_nom IS NOT NULL AND api_departamento_nom <> ''
    GROUP BY api_departamento_nom
"""

df_mortalidad = spark.sql(sql_mortalidad)
df_mortalidad.write.mode("overwrite").csv(
    f"s3://{bucket}/refined/indicadores/tasa_mortalidad/",
    header=True
)


# =====================================================
# 5. TASA DE RECUPERACIÓN
# Campo de evento: api_recuperado ('Recuperado')
# =====================================================

sql_recuperacion = """
    SELECT api_departamento_nom AS departamento,
           SUM(CASE WHEN lower(api_recuperado) = 'recuperado' THEN 1 ELSE 0 END)
               * 100.0 / COUNT(*) AS tasa_recuperacion
    FROM covid
    WHERE api_departamento_nom IS NOT NULL AND api_departamento_nom <> ''
    GROUP BY api_departamento_nom
"""

df_recuperacion = spark.sql(sql_recuperacion)
df_recuperacion.write.mode("overwrite").csv(
    f"s3://{bucket}/refined/indicadores/tasa_recuperacion/",
    header=True
)


# =====================================================
# 6. TOP 10 MUNICIPIOS MÁS AFECTADOS
# Campo: api_ciudad_municipio_nom
# =====================================================

sql_top10 = """
    SELECT api_ciudad_municipio_nom AS municipio,
           COUNT(*) AS casos
    FROM covid
    WHERE api_ciudad_municipio_nom IS NOT NULL AND api_ciudad_municipio_nom <> ''
    GROUP BY api_ciudad_municipio_nom
    ORDER BY casos DESC
    LIMIT 10
"""

df_top10 = spark.sql(sql_top10)
df_top10.write.mode("overwrite").csv(
    f"s3://{bucket}/refined/indicadores/top_10_municipios/",
    header=True
)

print("===== STEP ANALYTICS FINALIZADO =====")