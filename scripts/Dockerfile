FROM python:3.9-slim

WORKDIR /app

COPY requirements-generator.txt .
RUN pip install --no-cache-dir -r requirements-generator.txt

COPY data_generator.py .

CMD ["python", "data_generator.py"]
