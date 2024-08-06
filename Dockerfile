FROM public.ecr.aws/lambda/python:3.11

# Copy the function code and requirements file into the container
COPY config.ini contraction_mapping.json gather.py process.py refresh.py main.py utils.py requirements.txt ./

# Install the Python dependencies from requirements.txt
RUN python3.11 -m pip install -r requirements.txt

# Download NLTK data and store it in a known directory inside the Docker image  
RUN python3.11 -m nltk.downloader -d /nltk_data stopwords

# Ensure the NLTK data directory is in the environment's NLTK_DATA path
ENV NLTK_DATA=/nltk_data

# Set the CMD to your handler
CMD ["main.lambda_handler"]