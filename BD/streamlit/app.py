import streamlit as st
import plotly.express as px
from kafka import KafkaConsumer
import json
import pandas as pd
import joblib
from datetime import datetime
import time
from sklearn.metrics import precision_score, recall_score, f1_score

st.set_page_config(page_title="Hospital Analytics")

def load_model():
    try:
        state = joblib.load('../models/online_model.pkl')
        return state['model'], state['scaler']
    except:
        return None, None

model, scaler = load_model()

window_size = st.sidebar.slider("Window", 8, 60, 15)

st.title("Patient Readmission")

def get_kafka_data():
    consumer = KafkaConsumer(
        'patient_data',
        bootstrap_servers=['kafka1:9092', 'kafka2:9093'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=1000
    )
    
    data = []
    for msg in consumer:
        data.append(msg.value)
        if len(data) > 1000:
            break
    return pd.DataFrame(data)

def update_dashboard():
    df = get_kafka_data()
    if df.empty:
        return -1
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    recent = df[df['timestamp'] > datetime.now() - pd.Timedelta(minutes=window_size)]
    
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Feature Distributions")
            fig1 = px.histogram(recent, x='time_in_hospital', 
                              color='readmitted', nbins=20)
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            st.subheader("Model Metrics")
            if model and scaler and not recent.empty:
                X = recent[['time_in_hospital', 'num_lab_procedures',
                          'number_diagnoses', 'A1Cresult', 'insulin']]
                X_scaled = scaler.transform(X)
                preds = model.predict(X_scaled)
                
                accuracy = (preds == recent['readmitted']).mean()
                precision = precision_score(recent['readmitted'], preds, zero_division=0)
                recall = recall_score(recent['readmitted'], preds, zero_division=0)
                f1 = f1_score(recent['readmitted'], preds, zero_division=0)
                
                st.metric("Accuracy", f"{accuracy:.1%}")
              
                fig2 = px.bar(
                    x=["Precision", "Recall", "F1-score"],
                    y=[precision, recall, f1],
                    text=[f"{precision:.2f}", f"{recall:.2f}", f"{f1:.2f}"],
                    labels={"x": "Metric", "y": "Score"},
                    title="Precision, Recall & F1-score"
                )
                fig2.update_traces(textposition='outside')
                st.plotly_chart(fig2, use_container_width=True, key=f"fig{datetime.now()}_chart")

while True:
    status = update_dashboard()
    if status == -1:
        break
    time.sleep(5)