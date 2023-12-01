FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the lcal code to the container
COPY . /app
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port that Streamlit will run on
EXPOSE 8501

# Define the command to run on container start
CMD ["streamlit", "run", "src/app.py"]