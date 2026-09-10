# Use official Python image
FROM python:3.11.9-slim-bookworm

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --retries 10 --default-timeout 180 -r requirements.txt

# Copy the dashboard script
COPY dashboard.py .

# Expose port
EXPOSE 8050

# Run the dashboard
CMD ["streamlit", "run", "dashboard.py", "--server.port=8050", "--server.address=0.0.0.0"]