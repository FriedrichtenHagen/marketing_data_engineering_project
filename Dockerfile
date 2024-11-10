# Use a base Python image
FROM python:3.9-slim

# Set up working directory
WORKDIR /app

# install git
RUN apt-get update && apt-get install -y git

# Install dlt and other dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files
COPY . /app