FROM python:3.10-slim

WORKDIR /app

COPY extractor/requirements.txt .

RUN pip install -r requirements.txt

COPY extractor /app/extractor
COPY utils /app/utils

CMD ["python", "-m" , "extractor.src.main"]