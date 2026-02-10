from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['lucasghidini.ad@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_nyc_taxi_s3_pipeline',
    default_args=default_args,
    description='Pipeline de limpeza de dados NYC Taxi (Bronze -> Silver -> Gold)',
    schedule='@daily',
    catchup=False,
    tags=['nyc', 'taxi', 's3', 'pipeline']

) as dag:

    # Task 1: Sensor para verificar a chegada do arquivo no S3 (Bronze)
    check_s3_for_bronze = S3KeySensor(
        task_id='check_s3_for_bronze',
        bucket_name='amzn-s3-projetos',
        bucket_key='2026/taxi_pipiline/raw_data/yellow_tripdata_2025-01.parquet',
        aws_conn_id='aws_s3_conn',
        timeout=18*60*60
    )


    # Task 2: Função para processar os dados (Bronze -> Silver)
    def process_bronze_to_silver():
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        # Baixar o arquivo do S3
        file_path = s3_hook.download_file(
            bucket_name='amzn-s3-projetos',
            key='2026/taxi_pipiline/raw_data/yellow_tripdata_2025-01.parquet',
            local_path='/tmp'
        )

        import pandas as pd
        df= pd.read_parquet(file_path)
        # Limpeza e transformação dos dados
        df_clenand = df.dropna(subset=['passenger_count', 'trip_distance'])
        df_clenand = df_clenand[df_clenand['trip_distance'] > 0]
        df_clenand['tpep_pickup_datetime'] = pd.to_datetime(df_clenand['tpep_pickup_datetime'])
        df_clenand['tpep_dropoff_datetime'] = pd.to_datetime(df_clenand['tpep_dropoff_datetime'])

        #cria a coluna de duração da viagem
        df_clenand['trip_duration'] = (df_clenand['tpep_dropoff_datetime'] - df_clenand['tpep_pickup_datetime']).dt.total_seconds() / 60
        df_clenand = df_clenand[df_clenand['trip_duration'] > 0]


        output_path = '/tmp/yellow_tripdata_2025-01_silver.parquet'
        df_clenand.to_parquet(output_path, index=False)

        # Enviar o arquivo processado de volta para o S3 (Silver)
        s3_hook.load_file(
            filename=output_path,
            key='2026/taxi_pipiline/processed_data/yellow_tripdata_2025-01_silver.parquet',
            bucket_name='amzn-s3-projetos',
            replace=True
        )
    transform_bronze_to_silver = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=process_bronze_to_silver
    )

    # task 3: Função para processar os dados (Silver -> Gold)
    def process_silver_to_gold():
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        # Baixar o arquivo do S3
        file_path = s3_hook.download_file(
            bucket_name='amzn-s3-projetos',
            key='2026/taxi_pipiline/processed_data/yellow_tripdata_2025-01_silver.parquet',
            local_path='/tmp'
        )

        import pandas as pd
        df = pd.read_parquet(file_path)
        # Agregação dos dados para criar a camada Gold
        df_gold = df.groupby('payment_type').agg({
            'fare_amount': 'mean',
            'total_amount': 'mean',
            'trip_distance': 'mean',
            'trip_duration': 'mean'
        }).reset_index()

        # Renomear as colunas para refletir que são médias
        df_gold.rename(columns={
            'fare_amount': 'avg_fare_amount',
            'total_amount': 'avg_total_amount',
            'trip_distance': 'avg_trip_distance',
            'trip_duration': 'avg_trip_duration'
        }, inplace=True)

        # Salvar o arquivo Gold localmente e enviar para o S3
        output_path = '/tmp/yellow_tripdata_2025-01_gold.parquet'
        df_gold.to_parquet(output_path, index=False)

        s3_hook.load_file(
            filename=output_path,
            key='2026/taxi_pipiline/gold_data/yellow_tripdata_2025-01_gold.parquet',
            bucket_name='amzn-s3-projetos',
            replace=True
        )
        
    
    aggregate_silver_to_gold = PythonOperator(
        task_id='aggregate_silver_to_gold',
        python_callable=process_silver_to_gold
    )

    # Task 4: Enviar email de notificação após a conclusão do pipeline
    send_email_notification = EmailOperator(
        task_id='send_email_notification',
        to='lucasghidini.ad@gmail.com',
        subject='Pipeline NYC Taxi: Camada Gold Gerada com Sucesso',
        html_content="<h3>O processamento foi concluído!</h3>"
    )
        
    # Definir a ordem das tarefas
    check_s3_for_bronze >> transform_bronze_to_silver >> aggregate_silver_to_gold >> send_email_notification