# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install dependencies for Chrome and ChromeDriver
RUN apt-get update && \
    apt-get install -y git wget curl gnupg unzip cron && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Install ChromeDriver
RUN CHROME_DRIVER_VERSION=$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE) && \
    wget -O /tmp/chromedriver.zip "https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip" && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    rm /tmp/chromedriver.zip

RUN rm -rf /usr/local/bin/chromedriver

# Copy the current directory contents into the container at /app/backend
COPY . /app

# Install any necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Selenium
RUN pip install selenium requests bs4 python-Levenshtein

# Expose port 8000 to the outside world
EXPOSE 8000

# Define environment variable
ENV PYTHONUNBUFFERED=1

WORKDIR /app/backend

# Command to run the FastAPI application using uvicorn
CMD ["uvicorn", "app.main:app","--reload", "--host", "0.0.0.0", "--port", "8000"]
# CMD ["sh", "-c", "cd /backend && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000"]
