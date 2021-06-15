FROM python:3.5

RUN pip install --upgrade pip

#WORKDIR /src
COPY src/requirements.txt /requirements.txt
COPY src/twitter_client.py /twitter_client.py
RUN pip install -r /requirements.txt
CMD [ "python", "./twitter_client.py" ]