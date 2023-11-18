FROM amazon/aws-lambda-python:3.9

# Installs python, removes cache file to make things smaller
RUN yum update -y && \
    yum install -y python3 python3-dev python3-pip gcc && \
    rm -Rf /var/cache/yum

# Copies requirements.txt file into the container
COPY requirements.txt ./
# Installs dependencies found in your requirements.txt file
RUN pip install -r requirements.txt

COPY extract_data.py ./
COPY lambda_utils/ ./lambda_utils/
COPY constants.py ./

# Points to the handler function of your lambda function
CMD ["extract_data.extract_data"]
