#!/bin/bash

echo "===== INICIANDO CREACIÓN DE EMR AUTOMATIZADO (ETL) ====="

# VARIABLES IMPORTANTES
BUCKET="s3-proyecto3-sd"
REGION="us-east-1"
LOGS_PATH="s3://s3-proyecto3-sd/emr/logs/"
SCRIPT_PATH="s3://s3-proyecto3-sd/scripts/etl/etl_step.py"

# 1. CREAR CLÚSTER EMR CON AUTO-TERMINATE Y CON EL STEP ETL
CLUSTER_ID=$(aws emr create-cluster \
    --name "EMR-COVID-ETL" \
    --release-label emr-7.1.0 \
    --applications Name=Hadoop Name=Spark \
    --use-default-roles \
    --instance-type m5.xlarge \
    --instance-count 2 \
    --region $REGION \
    --log-uri $LOGS_PATH \
    --auto-terminate \
    --steps Type=Spark,Name="ETL-Step",ActionOnFailure=CONTINUE,Args=["spark-submit","--deploy-mode","client","--master","yarn","$SCRIPT_PATH"] \
    --query "ClusterId" --output text
)

echo "Cluster creado con ID: $CLUSTER_ID"
echo "Esperando inicio del cluster..."

aws emr wait cluster-running --cluster-id $CLUSTER_ID

echo "Cluster en ejecución. Esperando finalización del step ETL..."

aws emr wait step-complete \
    --cluster-id $CLUSTER_ID \
    --step-id $(aws emr list-steps --cluster-id $CLUSTER_ID --query "Steps[0].Id" --output text)

echo "ETL COMPLETO. El cluster se apagará automáticamente."

echo "===== PIPELINE ETL FINALIZADO ====="

