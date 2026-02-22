FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y redis-server && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN python -m playwright install chromium

COPY . .

CMD ["python", "main.py", "start"]
