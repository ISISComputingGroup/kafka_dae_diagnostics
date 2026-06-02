FROM python:3.13-trixie

WORKDIR /usr/src/app

COPY . .

RUN pip install --no-cache-dir .

ENTRYPOINT ["kdaediag"]
