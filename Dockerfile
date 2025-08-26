FROM --platform=arm64 python:3.13.7-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN rm -fr .env.example

COPY . .

EXPOSE 3000

CMD ["python", "main.py"]