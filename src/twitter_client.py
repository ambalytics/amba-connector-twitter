import socket
import time

import requests
import json
import logging

# old

from kafka import KafkaProducer
from requests.exceptions import ChunkedEncodingError

# todo into own file
bearer_token = 'AAAAAAAAAAAAAAAAAAAAACUdPAEAAAAAb23uL%2F0Joyiy9q8ELW6q14y1nIA%3DMVOg6aRZFDesoK1Q6DFl0nCzR7kWDRBEXhAs7nBj6ljD28sO5E'

# todo clean up what we need
tweet_fields_key = 'tweet.fields'
tweet_fields_value = ['created_at', 'author_id', 'conversation_id', 'in_reply_to_user_id', 'referenced_tweets', 'attachments', 'geo', 'context_annotations', 'entities', 'public_metrics', 'lang', 'source', 'reply_settings']

tweet_expansion_key = 'expansions'
tweet_expansion_value = 'referenced_tweets.id', 'author_id'

# todo config
kafka_topic_name = 'tweets'
kafka_key = 'parsed'

# todo rules loading/updating
# todo logging

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        logging.warning('Message published successfully.')
    except Exception as ex:
        logging.warning('Exception in publishing message')
        logging.warning(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 10)) # todo port
    except Exception as ex:
        logging.warning('Exception while connecting Kafka')
        logging.warning(str(ex))
    finally:
        return _producer


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    logging.warning(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    logging.warning(json.dumps(response.json()))


def set_rules(headers):
    rules = [
        {"value": '(url:"https://doi.org" OR url:"dx.doi.org")', "tag": "doi"},
        {"value": '(url: "emerald.com/insight/content/")', "tag": "General"},  # General
        {"value": '(url: "sciencedirect.com/science/article" OR url: "degruyter.com/document/doi/" OR url: '
                  '"link.springer.com/article/" OR url: "journals.elsevier.com" OR url: "onlinelibrary.wiley.com" OR '
                  'url: "nature.com/articles" OR url: "sciencemag.org" OR url: "iop.org" OR url: '
                  '"journals.sagepub.com" OR url: "journals.plos.org" OR url: "frontiersin.org/journals" OR url: '
                  '"tandfonline.com/doi/" OR url: "mdpi.com/journal")', "tag": "Natural sciences"},  # Natural sciences
        {"value": '(url: "journals.aps.org")', "tag": "Physics"},  # Physics
        {"value": '(url: "cochranelibrary.com/cdsr" OR url: "nejm.org/doi" OR url: "thelancet.com/journals" OR url: '
                  '"bmj.com/content" OR url: "pnas.org/content" OR url: "jamanetwork.com/journals" OR url: '
                  #'"sciencemag.org/content" OR url: '
                  '"acpjournals.org/doi" OR url: ' # doppel OR url: "journals.plos.org"
                  '"n.neurology.org/content")', "tag": "Medicine"},  # Medicine
        {"value": '(url: "doi.apa.org/record")', "tag": "Psychology"},  # Psychology
        {"value": '(url: "biorxiv.org/content" OR url: "arxiv.org/" OR url: "academic.oup.com" OR url: '
                  '"jmcc-online.com/article")', "tag": "Repositories"},  # Repositories
        {"value": '(url: "ieeexplore.ieee.org/document" OR url: "dl.acm.org/doi" OR url: "jmir.org")', "tag": "Computer science"},  # Computer Science
    ]    # You can adjust the rules if needed

    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    logging.warning(json.dumps(response.json()))


def send_data(kafka_producer):
    try:
        headers = create_headers(bearer_token)
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream",
            params={tweet_expansion_key: ','.join(tweet_expansion_value), tweet_fields_key: ','.join(tweet_fields_value)},
            headers=headers,
            stream=True,
        )
        logging.warning(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        logging.warning('twitter client kafka %s' % kafka_topic_name)
        i = 0
        for line in response.iter_lines():
            i += 1
            logging.warning(i)
            if line:
                # print(line)
                publish_message(kafka_producer, kafka_topic_name, kafka_key, line.decode('utf-8'))

    # reconnect if there are network issues
    except ChunkedEncodingError:
        logging.warning('NETWORK')
        send_data(producer)


if __name__ == '__main__':
    logging.warning('start twitter client')
    time.sleep(15)
    logging.warning('connect')

    producer = connect_kafka_producer()
    send_data(producer)

