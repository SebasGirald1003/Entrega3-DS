#!/bin/bash
set -e

echo ""
echo "==============================================="
echo "   PIPELINE COVID PROYECTO 3 - EAFIT / SD"
echo "   Ejecución automática de punta a punta"
echo "==============================================="
echo ""

REGION="us-east-1"
BUCKET="s3-proyecto3-sd"
SUBNET="subnet-0edd6248decaefa88"

# Lambda ARNs
LAMBDA_INGESTA="arn:aws:lambda:us-east-1:848044389739:function:lambda-api-proyecto3-sd"
LAMBDA_EXPOSICION="arn:aws:lambda:us-east-1:848044389739:function:lambda-url-gateway"

# Glue Job
GLUE_JOB="job-rds-to-s3"

echo ""
echo "===== 1. LIMPIANDO PIPELINE (raw, trusted, refined) ====="

aws s3 rm s3://$BUCKET/raw/ --recursive --region $REGION
aws s3 rm s3://$BUCKET/trusted/covid_merged/ --recursive --region $REGION
aws s3 rm s3://$BUCKET/refined/ --recursive --region $REGION

echo "Limpieza completada."
sleep 2

echo ""
echo "===== 2. EJECUTANDO LAMBDA DE INGESTA API ====="

aws lambda invoke \
  --function-name "$LAMBDA_INGESTA" \
  --region $REGION \
  response_ingesta.json >/dev/null 2>&1

echo "Lambda de ingesta ejecutada."
sleep 2

echo ""
echo "===== 3. EJECUTANDO JOB DE GLUE (RDS → S3) ====="

RUN_ID=$(aws glue start-job-run \
  --job-name "$GLUE_JOB" \
  --region $REGION \
  --query 'JobRunId' --output text)

echo "Glue Job ID: $RUN_ID"

echo "Esperando que Glue Job termine..."
while true; do
  STATUS=$(aws glue get-job-run \
    --job-name "$GLUE_JOB" \
    --run-id "$RUN_ID" \
    --region $REGION \
    --query "JobRun.JobRunState" \
    --output text)

  echo "Estado actual del Glue Job: $STATUS"

  case $STATUS in
      SUCCEEDED)
          echo "Glue Job finalizado exitosamente."
          break
          ;;
      FAILED|TIMEOUT)
          echo "Glue Job falló con estado: $STATUS"
          exit 1
          ;;
      *)
          echo "Aún en progreso... esperando 30 segundos"
          sleep 30
          ;;
  esac
done
sleep 2

echo ""
echo "===== 4. CREANDO CLÚSTER EMR PARA ETL + ANALYTICS ====="

CLUSTER_ID=$(aws emr create-cluster \
  --region $REGION \
  --name "pipeline-proyecto3" \
  --release-label emr-7.12.0 \
  --applications Name=Spark \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --ec2-attributes SubnetId=$SUBNET \
  --auto-terminate \
  --steps Type=CUSTOM_JAR,Name="ETL-step",ActionOnFailure=CONTINUE,Jar="command-runner.jar",Args=["spark-submit","--deploy-mode","client","--master","yarn","s3://s3-proyecto3-sd/scripts/etl/etl_step.py"] \
        Type=CUSTOM_JAR,Name="ANALYTICS-step",ActionOnFailure=CONTINUE,Jar="command-runner.jar",Args=["spark-submit","--deploy-mode","client","--master","yarn","s3://s3-proyecto3-sd/scripts/etl/analytics_step.py"] \
  --query "ClusterId" --output text)

echo "Cluster creado: $CLUSTER_ID"
echo "Esperando a que inicie..."
aws emr wait cluster-running --cluster-id "$CLUSTER_ID" --region $REGION

echo ""
echo "Cluster en ejecución. Procesando Steps..."
STEP_IDS=$(aws emr list-steps \
  --cluster-id "$CLUSTER_ID" \
  --region $REGION \
  --query "Steps[*].Id" --output text)

echo "Steps detectados:"
echo "$STEP_IDS"

# Convertir a array
read -ra STEPS_ARRAY <<< "$STEP_IDS"

# Validación
if [ ${#STEPS_ARRAY[@]} -lt 2 ]; then
    echo "ERROR: No se detectaron los dos steps (ETL y Analytics)."
    exit 1
fi

ETL_STEP_ID=${STEPS_ARRAY[0]}
ANALYTICS_STEP_ID=${STEPS_ARRAY[1]}

echo "ETL Step ID: $ETL_STEP_ID"
echo "Analytics Step ID: $ANALYTICS_STEP_ID"

echo "===== ESPERANDO FINALIZACIÓN DEL ETL ====="
aws emr wait step-complete \
  --cluster-id "$CLUSTER_ID" \
  --step-id "$ETL_STEP_ID" \
  --region $REGION
echo "ETL completado correctamente."

echo "===== ESPERANDO FINALIZACIÓN DEL ANALYTICS ====="
aws emr wait step-complete \
  --cluster-id "$CLUSTER_ID" \
  --step-id "$ANALYTICS_STEP_ID" \
  --region $REGION
echo "Analítica completada correctamente."

echo ""
echo "===== EMR FINALIZADO (ETL + ANALYTICS OK) ====="
sleep 2

echo ""
echo "===== 5. INVOCANDO LAMBDA DE EXPOSICIÓN (API) ====="

aws lambda invoke \
  --function-name "$LAMBDA_EXPOSICION" \
  --region $REGION \
  /dev/null >/dev/null 2>&1

echo "Lambda de exposición ejecutada."
sleep 1

echo ""

echo "===== 6. OBTENIENDO URL PÚBLICA DE LA LAMBDA ====="

echo ""
echo "URL pública de la API (Lambda Function URL):"
aws lambda get-function-url-config \
  --function-name lambda-url-gateway \
  --region $REGION \
  --query "FunctionUrl" \
  --output text

echo ""
echo "===== PIPELINE COMPLETO ====="
echo "Los resultados están en: s3://$BUCKET/refined/indicadores/"
echo "API disponible en la URL pública de la Lambda Function URL"
echo ""
echo "==============================================="
