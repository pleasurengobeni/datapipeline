# Pinned — update deliberately, never use :slim or :latest without a version
FROM python:3.10.14-slim-bookworm

WORKDIR /app

# Install Python dependencies into the image layer so they are cached.
# This layer is only rebuilt when web_ui/requirements.txt changes.
COPY web_ui/requirements.txt /tmp/web_ui_requirements.txt
RUN pip install --no-cache-dir -r /tmp/web_ui_requirements.txt

# The rest of the project is mounted as a volume at runtime so code changes
# are picked up without rebuilding the image.
EXPOSE 5001
CMD ["python", "-m", "flask", "run", "--host", "0.0.0.0", "--port", "5001"]
