CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.casos_por_municipio (
  municipio STRING,
  total_casos BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://s3-proyecto3-sd/refined/indicadores/casos_por_municipio/'
TBLPROPERTIES ('skip.header.line.count'='1');

----------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.casos_por_departamento (
  departamento STRING,
  total_casos BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://s3-proyecto3-sd/refined/indicadores/casos_por_departamento/'
TBLPROPERTIES ('skip.header.line.count'='1');

----------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.casos_por_fecha (
  fecha STRING,
  total_casos BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://s3-proyecto3-sd/refined/indicadores/casos_por_fecha/'
TBLPROPERTIES ('skip.header.line.count'='1');

----------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.tasa_mortalidad (
  departamento STRING,
  tasa_mortalidad DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://s3-proyecto3-sd/refined/indicadores/tasa_mortalidad/'
TBLPROPERTIES ('skip.header.line.count'='1');

----------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.tasa_recuperacion (
  departamento STRING,
  tasa_recuperacion DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://s3-proyecto3-sd/refined/indicadores/tasa_recuperacion/'
TBLPROPERTIES ('skip.header.line.count'='1');

----------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.top_10_municipios (
  municipio STRING,
  total_casos BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://s3-proyecto3-sd/refined/indicadores/top_10_municipios/'
TBLPROPERTIES ('skip.header.line.count'='1');

