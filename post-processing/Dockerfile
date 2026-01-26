# Use a lightweight Python image
FROM python:3.11-slim

# Install system dependencies (required for Shapely/Geos)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- SECRETS COPY ---
# Copy the service account key into the image
COPY sa.json .

# Copy application code
COPY main.py .

# Expose the port (informative only)
EXPOSE 8080

# Run with Gunicorn
CMD ["gunicorn", "--bind", ":8080", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]