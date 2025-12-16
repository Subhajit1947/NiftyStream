FROM python:3.9-slim

WORKDIR /app  # Base directory

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create dashboard directory structure
RUN mkdir -p /app/dashboard

# Copy requirements first for better caching
COPY dashboard/requirements.txt /app/dashboard/

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/dashboard/requirements.txt

# Create non-root user
RUN useradd -m -u 1000 streamlit && \
    chown -R streamlit:streamlit /app

USER streamlit

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Default command (can be overridden by docker-compose)
WORKDIR /app/dashboard
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]