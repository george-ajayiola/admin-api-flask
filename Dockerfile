# Builder stage
FROM python:3.13.0-alpine AS builder

# Install system dependencies
RUN apk update && \
    apk add --no-cache musl-dev libpq-dev gcc

# Create a virtual environment
RUN python -m venv /opt/venv

# Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# Set the working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Operational stage
FROM python:3.13.0-alpine

# Install runtime dependencies
RUN apk update && apk add --no-cache libpq-dev

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PATH="/opt/venv/bin:$PATH"

# Set the working directory
WORKDIR /app

# Copy the application code
COPY . .

# Expose the application port
EXPOSE 5000


# Run the application with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "api:app"]