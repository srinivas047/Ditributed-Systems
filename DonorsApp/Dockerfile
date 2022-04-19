# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster
# EXPOSE 8000
# EXPOSE 8001
# EXPOSE 8002

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]