FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn

COPY app.py .

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
