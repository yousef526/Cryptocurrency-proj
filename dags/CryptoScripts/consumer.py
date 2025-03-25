import json
import sys
from confluent_kafka import Consumer
from confluent_kafka import KafkaError,KafkaException
import time



def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        running = True
        
        while running:
            

            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                running = False
                consumer.commit(msg)
            
                

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def msg_process(msg):
    # Convert bytes to string
    json_string = msg.value().decode()
    num = 0
    with open("/opt/airflow/dags/CryptoScripts/partNum.txt","r") as f:
        num = int(f.readline()) 
    file_name = f"/opt/airflow/dags/CryptoScripts/GeneratedData/demo{num}.json"
    # Convert string to dictionary
    #data_dict = json.loads(json_string)
    with open(file_name,"w") as f:
        f.write(json_string)

""" conf = {'bootstrap.servers': 'kafka:9092',
        'group.id': 'my-app-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False}

consumer = Consumer(**conf)
basic_consume_loop(consumer,["Topic_1"]) """