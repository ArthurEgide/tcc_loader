FROM python:3.8.18-alpine3.18

WORKDIR /sanic

COPY . /sanic

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# sanic main:app --host=0.0.0.0 --port=1997 --workers=1 --no-coffe --no-motd --no-access-logs