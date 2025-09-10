FROM python:3.10-slim

WORKDIR /app

COPY stt_service/requirements.txt .

RUN pip install -r requirements.txt

COPY stt_service /app/stt_service
COPY utils /app/utils

CMD ["python", "-m" , "stt_service.src.main"]