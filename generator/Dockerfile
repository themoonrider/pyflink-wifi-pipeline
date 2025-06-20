FROM python:3.9-alpine

RUN set -ex; \
  python -m pip install --upgrade pip;

# Copy requirements and install Python packages
COPY ./generator/requirements.txt /opt/requirements.txt

COPY ./generator/data_generator.py /opt/data_generator.py
COPY ./src/config.py /opt/config.py

RUN pip install -r /opt/requirements.txt


CMD ["python", "-u", "/opt/data_generator.py"]
