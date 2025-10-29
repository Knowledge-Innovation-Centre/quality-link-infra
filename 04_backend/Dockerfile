# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Install PostgreSQL development dependencies
RUN apt-get update && apt-get install -y libpq-dev gcc

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file
COPY requirements.txt /app/

# Install the dependencies, including psycopg2-binary
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the FastAPI app's code including the config directory
COPY app /app

# Change the working directory to /app/app
WORKDIR /app

# Expose port 8000 for FastAPI
EXPOSE 8000

# Update the command to point to the correct module
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]