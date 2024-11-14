# Use a lightweight Python image as the base
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install dependencies directly from requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Set CMD to run Python; the specific script will be provided in docker-compose.yml
CMD ["python"]
