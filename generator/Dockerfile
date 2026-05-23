FROM python:3.11-slim

WORKDIR /app

# Install dos2unix để xử lý CRLF từ Windows
RUN apt-get update && apt-get install -y --no-install-recommends dos2unix \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-generator.txt .
RUN pip install --no-cache-dir -r requirements-generator.txt

COPY data_generator.py .
# Fix CRLF → LF (file được edit trên Windows)
RUN dos2unix /app/data_generator.py

CMD ["python", "data_generator.py"]