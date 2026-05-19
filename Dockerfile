FROM python:3.11-slim

WORKDIR /app
COPY server.py .

EXPOSE 8765
CMD ["python", "server.py", "--host", "0.0.0.0", "--port", "8765"]
