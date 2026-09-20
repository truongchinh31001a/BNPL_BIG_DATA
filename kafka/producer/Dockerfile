FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fake_bnpl_producer.py .

CMD ["python", "fake_bnpl_producer.py"]
