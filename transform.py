import pandas as pd
from extract import extract_data  # Importer la fonction d'extraction depuis le fichier extract.py
from airflow.operators.python_operator import PythonOperator

def transform_data(**kwargs):
    ti = kwargs['ti']  # Récupérer l'instance de la tâche courante
    extracted_data = ti.xcom_pull(task_ids='extract', key='return_value')  # Remplacer 'return_value' par la clé correcte si nécessaire
    langues_codes = {
        'English': 'EN',
        'French': 'FR',
        'Chinese': 'ZH',
        'Hindi': 'HI',
        'Portuguese': 'PT',
        'Spanish': 'ES',
        'German': 'DE',
        'Italian': 'IT',
        'Norwegian': 'NO',
        'Russian': 'RU',
        'Dutch': 'NL',
        'Swedish': 'SV',
        'Japanese': 'JA',
        'Czech': 'CS',
        'Yiddish': 'YI',
        'Gujarati': 'GU'
    }

     # Appliquer la transformation sur les données extraites
    transformed_data = extracted_data.copy()
    transformed_data['Language code'] = transformed_data['Original language'].map(langues_codes)

    return transformed_data

def create_transform(dag):
    return PythonOperator(task_id='transform', python_callable=transform_data, provide_context=True, dag=dag)
