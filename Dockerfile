# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies (if any) and Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app sources
COPY . .

# Expose the server port
EXPOSE 8000

# Start the HTTP server
CMD ["python", "server.py"]
