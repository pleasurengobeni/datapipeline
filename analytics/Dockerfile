FROM python:3.11.9-slim-bookworm

WORKDIR /app

# Install dependencies from requirements file at build time
COPY analytics/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8501

# Project is volume-mounted at /app; run from there
CMD ["streamlit", "run", "analytics/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
