FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r backend/requirements.txt
COPY . .
CMD ["python", "scripts/supervisor.py"]