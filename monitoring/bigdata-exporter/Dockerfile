FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY exporter.py .
COPY tests/ ./tests/

RUN useradd --create-home exporter

USER exporter

EXPOSE 9200

CMD ["python", "exporter.py"]