FROM python:3.10-slim

WORKDIR /app

COPY persister/requirements.txt .

RUN pip install -r requirements.txt

COPY persister /app/persister
COPY utils /app/utils

CMD ["python", "-m" , "persister.src.main"]