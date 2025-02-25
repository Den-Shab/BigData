FROM python:3.9-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/st
WORKDIR /app/st


EXPOSE 8501
CMD ["streamlit", "run", "app.py"] 
