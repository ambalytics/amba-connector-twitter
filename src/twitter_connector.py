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

    user_expansion_key = 'user.fields'
    user_expansion_value = ['created_at', 'description', 'entities', 'id', 'location', 'name', 'pinned_tweet_id',
                            'profile_image_url', 'protected', 'public_metrics', 'url', 'username', 'verified',
                            'withheld']

    tweet_expansion_key = 'expansions'
    tweet_expansion_value = 'referenced_tweets.id', 'author_id'

    def send_data(self):

        headers = create_headers(self.bearer_token)
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream",
            params={self.tweet_expansion_key: ','.join(self.tweet_expansion_value),
                    self.tweet_fields_key: ','.join(self.tweet_fields_value),
                    self.user_expansion_key: ','.join(self.user_expansion_value)},
            headers=headers,
            stream=True,
        )
        logging.warning({self.tweet_expansion_key: ','.join(self.tweet_expansion_value),
                    self.tweet_fields_key: ','.join(self.tweet_fields_value),
                    self.user_expansion_key: ','.join(self.user_expansion_value)})
        logging.warning(response.status_code)
        logging.warning(response)

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
                # logging.warning(twitter_json)

                if 'data' in twitter_json and 'includes' in twitter_json and 'matching_rules' in twitter_json \
                        and 'id' in twitter_json['data'] and 'created_at' in twitter_json['data'] \
                        and 'referenced_tweets' in twitter_json['data'] and 'author_id' in twitter_json['data']:
                    e = Event()

                    # todo check for duplicate key? try catch duplicate key error, if catch check
                    # todo the element with the id existing, compare and if different new id and save
                    # todo check if twitter id has been used to avoid counting tweets twice
                    e.set('id', str(uuid.uuid4()))

                    e.set('subj_id', twitter_json['data']['id'])
                    e.set('relation_type', 'discusses')
                    e.set('occurred_at', twitter_json['data']['created_at'])
                    e.set('source_id', 'twitter')
                    e.set('state', 'unlinked')

                    basePid = "twitter://status?id="
                    baseAuthorUrl = "twitter://user?screen_name="
                    e.data['subj']['pid'] = basePid + twitter_json['data']['id']
                    e.data['subj']['url'] = basePid + twitter_json['data']['id']
                    e.data['subj']['title'] = "Tweet " + twitter_json['data']['id']
                    e.data['subj']['issued'] = twitter_json['data']['created_at']
                    e.data['subj']['author'] = {
                                                   "url": baseAuthorUrl + get_author_name(
                                                       twitter_json['data']['author_id'],
                                                       twitter_json['includes']['users'])
                                               }
                    if 'users' in twitter_json['includes'] and 'id' in twitter_json['data']['referenced_tweets'][0]:
                        e.data['subj']['original-tweet-url'] = basePid + twitter_json['data']['referenced_tweets'][0][
                            'id']
                        e.data['subj']['original-tweet-author'] = baseAuthorUrl + get_author_name(
                            twitter_json['data']['author_id'], twitter_json['includes']['users'], True)
                    e.data['subj']['alternative-id'] = twitter_json['data']['id']

                    e.data['subj']['data'] = twitter_json['data']
                    e.data['subj']['data']['includes'] = twitter_json['includes']
                    e.data['subj']['data']['matching_rules'] = twitter_json['matching_rules']

                    self.publish(e)


def get_author_name(author_id, users, original=False):
    for user in users:
        if ('id' in user and 'username' in user) and (
                (user['id'] == author_id and not original) or user['id'] != author_id and original):
            return user['username']
    return ""


if __name__ == '__main__':
    t = TwitterConnector(1)

    logging.warning('start twitter client ... connect in %s' % t.kafka_boot_time)
    for i in range(t.kafka_boot_time, 0):
        time.sleep(1)
        logging.debug('%ss left' % i)

    logging.warning('connect twitter client')

    # todo make this error restart save
    # todo add shutdown operation
    t.send_data()
