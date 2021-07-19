import logging
import time
import uuid
import requests
import json

from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


class TwitterConnector(EventStreamProducer):
    state = "unlinked"
    log = "TwitterConnector "

    bearer_token = 'AAAAAAAAAAAAAAAAAAAAACUdPAEAAAAAb23uL%2F0Joyiy9q8ELW6q14y1nIA%3DMVOg6aRZFDesoK1Q6DFl0nCzR7kWDRBEXhAs7nBj6ljD28sO5E'
    tweet_fields_key = 'tweet.fields'
    tweet_fields_value = ['created_at', 'author_id', 'conversation_id', 'in_reply_to_user_id', 'referenced_tweets',
                          'attachments', 'geo', 'context_annotations', 'entities', 'public_metrics', 'lang', 'source',
                          'reply_settings']

    tweet_expansion_key = 'expansions'
    tweet_expansion_value = 'referenced_tweets.id', 'author_id'

    def send_data(self):

        headers = create_headers(self.bearer_token)
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream",
            params={self.tweet_expansion_key: ','.join(self.tweet_expansion_value),
                    self.tweet_fields_key: ','.join(self.tweet_fields_value)},
            headers=headers,
            stream=True,
        )
        # logging.warning(response.status_code)

        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        i = 0
        for line in response.iter_lines():
            if line:
                i += 1
                logging.warning(self.log + "got new message " + str(i))
                # line.decode('utf-8')
                twitter_json = json.loads(line.decode('utf-8'))
                twitter_json['state'] = self.state
                # logging.warning(twitter_json)

                # todo
                e = Event()

                # todo check for duplicate key? try catch duplicate key error, if catch check
                # todo the element with the id existing, compare and if different new id and save
                e.set('id', str(uuid.uuid4()))

                e.set('subj_id', twitter_json['data']['id'])
                e.set('relation_type', 'discusses')
                e.set('occurred_at', twitter_json['data']['created_at'])
                e.set('source_id', 'twitter')
                e.set('state', 'unlinked')

                e.data['subj']['pid'] = twitter_json['data']['id']
                e.data['subj']['url'] = "todo"
                e.data['subj']['title'] = "todo"
                e.data['subj']['issued'] = "todo"
                e.data['subj']['author'] = "todo"
                e.data['subj']['original-tweet-url'] = "todo"
                e.data['subj']['original-tweet-author'] = "todo"
                e.data['subj']['alternative-id'] = "todo"

                # todo why state in data?
                e.data['subj']['data'] = twitter_json['data']
                e.data['subj']['data']['includes'] = twitter_json['includes']
                e.data['subj']['data']['matching_rules'] = twitter_json['matching_rules']

                self.publish(e)


if __name__ == '__main__':
    t = TwitterConnector(1)

    logging.warning('start twitter client ... connect in %s' % t.kafka_boot_time)
    for i in range(t.kafka_boot_time, 0):
        time.sleep(1)
        logging.debug('%ss left' % i)

    logging.warning('connect twitter client')
    t.send_data()
