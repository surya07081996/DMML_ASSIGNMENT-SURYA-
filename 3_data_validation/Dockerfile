# Use an official Python base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy all files from the current folder into the container
COPY . /app

# Install dependencies if a requirements.txt exists
RUN pip install --no-cache-dir -r requirements.txt || true

# Set the entrypoint to execute index.py
CMD ["python", "index.py"]
