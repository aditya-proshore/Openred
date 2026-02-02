# Use the official Python slim image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and ensure logs are sent to Cloud Logging
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED True

# Set the working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .

# Run the web service using gunicorn
# -b 0.0.0.0:$PORT tells gunicorn to listen on the port provided by Cloud Run
# main:app refers to the 'app' object inside 'main.py'
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app