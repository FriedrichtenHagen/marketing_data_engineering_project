# Base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy application code
COPY ./app /app/app
COPY ./requirements.txt /app/
COPY ./facebook_insights_pipeline.duckdb /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8000

# Run FastAPI app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]