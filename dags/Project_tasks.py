from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from CryptoScripts.API_Call import apiCall
from CryptoScripts.producer import produceTopic
#from CryptoScripts.consumer import basic_consume_loop
from CryptoScripts.Spark_Processing import processData
from confluent_kafka import Consumer

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    #'retries': 1,
}


# Define the DAG
with DAG(
    dag_id='CryptoCurrecny_Proj',
    default_args=default_args,
    description='A simple cryptoCurrecny Proj to transform from USD to Other Curenccies',
    schedule_interval=timedelta(minutes=40),  # Run once a day
    catchup=False,  # to prevent the dag from trying to run agian and catch days it didnt run
) as dag:

    # Define the Bash task
    task1 = PythonOperator(
        task_id='Produce_data',
        python_callable=apiCall,
    )


    task2 = PythonOperator(
        task_id='Write_data_in_kafka',
        python_callable=produceTopic,
        
    )
    #op_args=["hello", 123],           # positional args
    #op_kwargs={"name": "Ali", "age": 25},  kwargs
    """ task3 = PythonOperator(
        task_id='Consumer_File_From_topic',
        python_callable=basic_consume_loop,
        op_args=[consumer,["Topic_1","Topic_2","Topic_3","Topic_4"]]
    ) """


    task4 = PythonOperator(
        task_id='Spark_Processing',
        python_callable=processData,
    )

    """ task4 = SparkSubmitOperator(
        task_id='Spark_Processing',
        application='/opt/airflow/dags/CryptoScripts/Spark_Processing.py',
        conn_id='spark_default',  # لازم تعرّف Conn ID في Airflow
        verbose=True,
        conf={"spark.master": "spark://spark:7077"},
    ) """
    
    task1 >> task2 >> task4

    

    # You can add more tasks here and set dependencies

# You can add dependencies between tasks like this:
# task1 >> task2
""" conf = {'bootstrap.servers': 'kafka:9092',
        'group.id': 'my-app-group1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False}

consumer = Consumer(**conf) """