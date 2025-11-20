#!/bin/bash

# ========= CONFIGURACIÓN =========
REGION="us-east-1"
BUCKET="s3-proyecto3-sd"
ETL_SCRIPT="s3://s3-proyecto3-sd/scripts/etl/etl_step.py"

# ========= 1. CREAR CLÚSTER EMR =========
echo "Creando cluster EMR..."

CLUSTER_ID=$(aws emr create-cluster \
    --name "Cluster-ETL" \
    --release-label emr-7.3.0 \
    --applications Name=Hadoop Name=Spark \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
    --instance-type m4.xlarge \
    --instance-count 3 \
    --use-default-roles \
    --region $REGION \
    --log-uri s3://$BUCKET/logs/ \
    --query 'ClusterId' \
    --output text)

echo "Cluster creado con ID: $CLUSTER_ID"
echo "Esperando a que esté en estado WAITING..."

aws emr wait cluster-running --cluster-id $CLUSTER_ID

echo "Cluster listo."

# ========= 2. AÑADIR STEP DEL ETL =========
echo "Añadiendo Step del ETL..."

STEP_ID=$(aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps Type=CUSTOM_JAR,\
Name=ETLJob,\
ActionOnFailure=CONTINUE,\
Jar=command-runner.jar,\
Args=["spark-submit","--deploy-mode","client","--master","yarn","'$ETL_SCRIPT'"] \
    --region $REGION \
    --query 'StepIds[0]' \
    --output text)

echo "Step creado con ID: $STEP_ID"
echo "Esperando a que el Step termine..."

aws emr wait step-complete \
    --cluster-id $CLUSTER_ID \
    --step-id $STEP_ID

echo "ETL terminado correctamente."

# ========= 3. APAGAR EL CLÚSTER =========
echo "Apagando cluster..."

aws emr terminate-clusters --cluster-id $CLUSTER_ID

echo "Cluster terminado."
echo "===== PIPELINE ETL COMPLETADO ====="
