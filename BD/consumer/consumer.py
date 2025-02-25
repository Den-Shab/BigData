from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import joblib
import pandas as pd
import time
import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from threading import Lock
import os
from pathlib import Path

parent_dir = os.path.join(os.path.dirname(__file__), '..')
class OnlineLearner:
    def __init__(self, bootstrap_servers, topic):
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'online_learner_group',
            'auto.offset.reset': 'earliest'
        }
        print("Connected")
        self.batch_size = 1000
        self.batch_buffer = []
        self.lock = Lock()
        parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
        models_dir = os.path.join(parent_dir, 'models')
        
        os.makedirs(models_dir, exist_ok=True)
        
        self.model_path = '/models/online_model111.pkl'
        self.model = SGDClassifier(
            loss='log_loss',
            warm_start=True,
            eta0=0.01,  
            learning_rate='invscaling',  
            power_t=0.5,  
            random_state=42
        )
        self.scaler = StandardScaler()
        self.is_initialized = False
        
        if os.path.exists('models/online_model.pkl'):
            self._load_model()
        
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'online_learner_group',
            #'auto.offset.reset': 'earliest',
            # 'session.timeout.ms': 300,
            # 'max.poll.interval.ms': 600,
            # 'heartbeat.interval.ms': 500,
            # 'enable.auto.commit': True,
            # 'auto.commit.interval.ms': 500
        })
        self.consumer.subscribe([topic])

    def _load_model(self):
        state = joblib.load('models/online_model.pkl')
        self.model = state['model']
        self.scaler = state['scaler']
        self.is_initialized = True

    def _save_model(self):
        state = {
            'model': self.model,
            'scaler': self.scaler
        }
        
        joblib.dump(state, 'models/online_model.pkl')

    def process_stream(self):
        no_mes_counter = 0
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    no_mes_counter += 1
                    if no_mes_counter >= 40:
                        if len(self.batch_buffer) > 0:
                           self._process_batch()
                        break
                    print("No message...")
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("The end...")
                        continue
                    else:
                        print(f"Error: {msg.error()}")
                        continue
                
                with self.lock:
                    print(msg.value())
                    no_mes_counter = 0
                    data = json.loads(msg.value())
                    self.batch_buffer.append(data)
                    
                    if len(self.batch_buffer) >= self.batch_size:
                        self._process_batch()
                        
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def _process_batch(self):
        df = pd.DataFrame(self.batch_buffer)
        X = df[['time_in_hospital', 'num_lab_procedures',
               'number_diagnoses', 'A1Cresult', 'insulin']]
        y = df['readmitted']
        
        if not self.is_initialized:
            self.scaler.partial_fit(X)
            self.model.partial_fit(
                self.scaler.transform(X),
                y,
                classes=[0, 1]
            )
            self.is_initialized = True
        else:
            X_scaled = self.scaler.transform(X)
            self.model.partial_fit(X_scaled, y)
            
        self._save_model()
        self.batch_buffer.clear()

if __name__ == "__main__":
    learner = OnlineLearner(
        bootstrap_servers=os.getenv('KAFKA_BROKERS', 'kafka1:9092,kafka2:9093'),
        topic='patient_data'
    )
    learner.process_stream()