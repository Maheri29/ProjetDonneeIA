import pandas as pd
from airflow.operators.python_operator import PythonOperator

def extract_data():

    # Charger le fichier CSV dans un DataFrame avec l'encodage spécifié
    data = pd.read_csv('https://bucketia-donnee.s3.eu-west-3.amazonaws.com/books.csv')
    
    return data

def create_extract(dag):
    return PythonOperator(task_id='extract', python_callable=extract_data, dag=dag)
