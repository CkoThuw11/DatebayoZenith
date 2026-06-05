FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5001

CMD ["gunicorn", "webhook_receiver:app", "--bind", "0.0.0.0:5001", "--workers", "1", "--timeout", "30"]