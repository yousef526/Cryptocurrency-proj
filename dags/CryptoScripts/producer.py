import json
from confluent_kafka import Producer
import socket
import time

def produceTopic():
    time.sleep(10)
    num = 0
    with open("/opt/airflow/dags/CryptoScripts/partNum.txt","r") as f:
        num = int(f.readline()) 
    file_name = f"/opt/airflow/dags/CryptoScripts/demo{num}.json"

    conf = {'bootstrap.servers': 'kafka:9092',}
    producer = Producer(**conf)
    with open(f"{file_name}", 'r',encoding="utf-8") as file:
        reader = json.load(file)
        reader = json.dumps(reader).encode()
        producer.produce(key="key22",topic="Topic_1",value=reader)

            


    producer.flush()
    import os
    os.remove(file_name)

