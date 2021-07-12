FROM python:3.6

RUN pip install --upgrade pip

WORKDIR /src
COPY . .

RUN pip install -r src/requirements.txt
CMD [ "python", "./src/twitter_connector.py" ]
