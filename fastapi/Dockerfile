FROM python:3.8-alpine
ENV PYTHONUNBUFFERED=1
WORKDIR /fastapi-solver
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip3 install -r requirements.txt
COPY . /fastapi-solver//
EXPOSE 8000