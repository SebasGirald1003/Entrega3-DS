# Entrega 3 - Sistemas Distribuidos
# Integrantes: 
Juan José Vasquez Gómez
Santiago Álvarez Peña
Sebastián Giraldo Álvarez
## Automatización del proceso de captura, ingesta, procesamiento y generación de datos para el análisis de Covid-19 en Colombia  
### ST0263 – Tópicos Especiales en Telemática — Universidad EAFIT

## Introducción

Este proyecto implementa una arquitectura batch para el manejo de datos a gran escala, siguiendo el ciclo completo de un proceso analítico moderno. El objetivo principal es capturar datos provenientes de fuentes públicas del Ministerio de Salud de Colombia, combinarlos con datos complementarios almacenados en una base relacional, procesarlos mediante un clúster de cómputo distribuido y finalmente exponer los resultados en formatos consultables.

El desarrollo integra extracción automática de datos, almacenamiento distribuido, procesamiento con Spark sobre AWS EMR, analítica con SparkSQL y mecanismos de consulta basados en Athena y servicios AWS. Todos los procesos están diseñados para ejecutarse sin intervención manual una vez configurados, replicando el comportamiento de un pipeline real de ingeniería de datos.

# Arquitectura General

La solución está basada en tres zonas de almacenamiento dentro de Amazon S3: Raw, Trusted y Refined.

## Zona Raw  
Recibe todos los datos en su formato original. Aquí se almacenan tanto los archivos que provienen de la API pública de Covid como las exportaciones incrementales generadas desde la base de datos relacional PostgreSQL alojada en AWS RDS.

## Zona Trusted  
Contiene los datos procesados y unificados. Mediante un primer paso de procesamiento en Spark dentro de un clúster EMR, se realizan tareas de limpieza, normalización de fechas, estandarización de nombres y códigos, así como la unión entre los datos abiertos y las tablas complementarias del RDS.

## Zona Refined  
Almacena los resultados finales del proceso analítico, ya sea en formato CSV o Parquet. Esta zona permite que los indicadores calculados puedan ser consultados desde Athena o bien expuestos mediante una API.

El flujo completo está automatizado mediante Lambdas, Glue Jobs, Steps de EMR y scripts en AWS CLI. Esto garantiza que todo el proceso, desde la captura hasta la exposición de datos, pueda ser ejecutado de forma programática.

# Componentes Principales

## Captura e Ingesta

### Captura desde la API del Ministerio de Salud  
La captura de datos del Ministerio de Salud se realiza a través de un servicio Lambda programado mediante EventBridge. Este servicio consulta periódicamente el endpoint público de Datos Abiertos y almacena los registros obtenidos en la carpeta correspondiente dentro del bucket S3 en la zona Raw.

### Captura desde PostgreSQL en AWS RDS  
La segunda fuente del pipeline es una base de datos PostgreSQL creada en AWS RDS. En esta base se incluyen datos complementarios simulados, tales como municipios, EPS, hospitales y otros elementos contextuales. Su extracción se realiza mediante AWS Glue, utilizando un job de Python Shell que se conecta al RDS, lee las tablas requeridas y las exporta a S3 de forma incremental. Esta ingesta complementaria permite enriquecer los datos provenientes de la API oficial.

## Procesamiento ETL en Spark

El primer proceso ejecutado dentro de EMR corresponde al paso ETL. Aquí se lee la información proveniente de ambas fuentes y se aplican transformaciones necesarias para su correcta integración. El script realiza limpieza de campos, tratamiento de fechas, normalización de valores inconsistentes y creación de columnas estandarizadas. En esta etapa también se realiza la unión entre los datos de Covid y la información contextual proveniente del RDS.

El resultado de este paso se almacena en formato Parquet en la zona Trusted del bucket S3, optimizando así las operaciones posteriores de consulta y análisis.

Este proceso se ejecuta como un Step automático dentro del clúster EMR, de manera que pueda ser llamado desde la línea de comandos sin necesidad de abrir el clúster manualmente.

## Analítica y Generación de Indicadores

El segundo Step del clúster EMR está dedicado a la generación de indicadores. Para ello se emplean DataFrames y consultas SparkSQL, siguiendo las metodologías vistas en laboratorio.

Entre los análisis generados se encuentran agregaciones por municipio, por departamento, por fecha y cálculos de indicadores como tasas de mortalidad y recuperación, así como rankings de localidades con mayor impacto. También se generan vistas temporales para analizar tendencias semanales y mensuales.

Los resultados se escriben en la zona Refined de S3 en formatos adecuados para análisis posteriores y para consulta externa utilizando Athena.

# Exposición y Consulta del Resultado

## Consulta mediante Athena  
Se crea una base de datos en Glue Catalog y se definen las tablas correspondientes sobre la zona Refined.

## Exposición mediante API  
Se implementa una API sobre Lambda y se expone mediante API Gateway (o mediante Function URL en entornos restringidos). La API consulta Athena o directamente los archivos procesados y devuelve los indicadores en formato JSON. Esto permite demostrar cómo un sistema analítico puede integrarse a aplicaciones externas o dashboards.

# Automatización del Pipeline

Además de diseñar cada componente, se implementó un sistema de automatización que permite ejecutar el flujo completo sin intervención humana. Este sistema utiliza scripts Bash basados en AWS CLI que se encargan de:

- Crear el clúster EMR.
- Enviar los Steps de ETL y analítica.
- Esperar la finalización del proceso.
- Apagar el clúster automáticamente.

El objetivo de esta sección es demostrar la capacidad de orquestar un proceso analítico completo de manera programática.

# Tecnologías Utilizadas

El proyecto se desarrolló utilizando diversos servicios de AWS, entre los cuales destacan Lambda, Glue, RDS, EMR, S3, Athena y API Gateway. También se utilizó Spark como motor de procesamiento distribuido, Python para la implementación de los scripts y Bash para la automatización de la infraestructura.

