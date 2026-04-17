FROM python:3.11-slim

WORKDIR /app

COPY streamlit-dashboard/pyproject.toml /app/pyproject.toml


# --no-cache-dir tells pip not to store the download cache inside the image,
RUN pip install --no-cache-dir .

# We do NOT copy the application code here.
# The actual streamlit/ folder will be mounted as a volume at runtime,
CMD ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]