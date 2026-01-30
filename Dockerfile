FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy dependency list
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the analytics script
COPY analysis.py .

# Run script on container start
CMD ["python", "analysis.py"]