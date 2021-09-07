FROM python:3.6

RUN pip install --upgrade pip

WORKDIR /src
COPY . .

RUN pip install -r src/requirements.txt

#ENTRYPOINT ["/bin/bash", "-c", "./scripts/entrypoint.sh"]
ENTRYPOINT ["sh", "./scripts/entrypoint.sh"]