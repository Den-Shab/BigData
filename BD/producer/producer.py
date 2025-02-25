from confluent_kafka import Producer
import pandas as pd
import time
import random
import json
from datetime import datetime
import os

class HospitalProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'hospital-producer',
            'acks': 'all'
        })
        self.topic = topic
        self.df = pd.read_csv('diabetes.csv')
        self._preprocess_data()
        
    def _preprocess_data(self):
        self.df['readmitted'] = self.df['readmitted'].map(
            lambda x: 1 if x != "NO" else 0
        )
        self.df = self.df.sample(frac=1).reset_index(drop=True)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.value()}]')

    def start_producing(self, delay=0.1, mode='static'):
        for _, row in self.df.iterrows():
            data = {
                'time_in_hospital': row['time_in_hospital'],
                'num_lab_procedures': row['num_lab_procedures'],
                'number_diagnoses': row['number_diagnoses'],
                'A1Cresult': 1 if row['A1Cresult'] in [">7", ">8"] else 0,
                'insulin': 1 if row['insulin'] == "Yes" else 0,
                'readmitted': row['readmitted'],
                'timestamp': datetime.now().isoformat()
            }
            
            self.producer.produce(
                self.topic,
                key=str(random.randint(1, 1000)),
                value=json.dumps(data),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            if mode == 'random':
                time.sleep(random.uniform(delay*0.5, delay*1.5))
            else:
                time.sleep(delay)
        self.producer.flush()

if __name__ == "__main__":
    producer = HospitalProducer(
        bootstrap_servers=os.getenv('KAFKA_BROKERS', 'kafka1:9092,kafka2:9093'),
        topic='patient_data'
    )
    producer.start_producing(delay=0.1, mode='random')