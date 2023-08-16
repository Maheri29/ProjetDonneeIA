import pandas as pd
import boto3
from extract import extract_data  # Importez la fonction d'extraction depuis le fichier extract.py
from transform import transform_data  # Importez la fonction de transformation depuis le fichier transform.py
from airflow.operators.python_operator import PythonOperator

# Paramètres pour l'accès à S3
AWS_ACCESS_KEY_ID = 'AKIAUZINARKQEQNWUKNW'
AWS_SECRET_ACCESS_KEY = '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'
BUCKET_NAME = 'bucketia-donnee'

def load_data_to_s3(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_data')  # Récupérer les données transformées depuis XCom
    # Initialiser le client S3
    s3_client = boto3.client('s3', aws_access_key_id=AKIAUZINARKQEQNWUKNW, aws_secret_access_key=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)

    # Convertir le DataFrame en CSV au format texte
    csv_buffer = transformed_data.to_csv(index=False)

    # Charger le CSV dans le bucket S3
    s3_client.put_object(Body=csv_buffer, Bucket=bucketia-donnee, Key='data.csv')

def create_load(dag):
    return PythonOperator(task_id='load', python_callable=load_data_to_s3, provide_context=True, dag=dag)
