FROM python:3.12-slim

# Install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install make
RUN apt-get update && \
    apt-get install -y --no-install-recommends make && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy
COPY requirements.txt Makefile ./
COPY .env ./
COPY src/ ./src/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set default environment variable
ENV DSNAME=taxitrips
ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV API_LOCAL_URL="http://localhost:8080"
ENV SLEEP_TIME_SEC=5
# Run the application
# ENTRYPOINT ["make"]
# CMD ["run"]

# dev
ENTRYPOINT ["python"]
CMD ["src/main.py"]