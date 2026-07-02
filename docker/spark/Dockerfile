FROM spark:3.5.1-java17-python3

USER root

# Install Python dependencies required by the jobs package
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER spark
