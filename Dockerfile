FROM python:3.8.8

RUN apt-get update \
    && apt-get install python-dev -y \
    && apt-get install unixodbc-dev -y
COPY ./requirements.txt ./

RUN pip install -r requirements.txt
RUN pip install aiohttp
RUN pip install asyncpg

COPY ./app /app

WORKDIR /app
EXPOSE 80