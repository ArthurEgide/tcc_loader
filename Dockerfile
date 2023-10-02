FROM python:3.8.18-alpine3.18

WORKDIR /sanic

COPY . /sanic

RUN pip install --upgrade pip
RUN pip install -r requirements.txt