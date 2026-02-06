# Use the official lightweight Python image.
FROM python:3.11-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED=True

# Set the working directory in the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the local code to the container
COPY . .

# Run the web service on container startup.
# We use 1 worker with 8 threads to handle multiple PDF requests per container instance.
# Timeout is set to 0 to allow Cloud Run to manage request timeouts.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
