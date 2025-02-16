FROM python:3.12-slim

# Install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy the application into the container.
COPY . /app

# Install the application dependencies.
WORKDIR /app
#RUN uv sync --frozen --no-cache

# install dependencies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Run the application.
CMD ["fastapi", "run", "app/main.py", "--port", "8080", "--host", "0.0.0.0"]
#CMD ["make", "run"]