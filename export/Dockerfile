FROM python:3.13-slim-bookworm

WORKDIR /app/export

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt


COPY . .

CMD ["python3", "main.py"]


