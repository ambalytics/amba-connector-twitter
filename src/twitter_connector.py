import logging
import os
import sys
import threading
import time
import uuid
import requests
import json
import sentry_sdk

import urllib3
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


class TwitterConnector(EventStreamProducer):
    state = "unlinked"
    log = "TwitterConnector "

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

    counter = 0

    def throughput_statistics(self, time_delta, i):
        """statistic tools

        Arguments:
            - time_delta: how often to run this
        """
        logging.warning("THROUGHPUT: %d / %d" % (self.counter, time_delta))
        if self.counter == 0:
            i += 1
        if i == 5:
            sys.exit() # end so it will restart clean

        self.counter = 0

        threading.Timer(time_delta, self.throughput_statistics, args=[time_delta, i]).start()

    def send_data(self):
        """open the stream to twitter and send the data as events to kafka
        """
        time_delta = 30
        self.counter = 0
        threading.Timer(time_delta, self.throughput_statistics, args=[time_delta, 0]).start()

        bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        headers = create_headers(bearer_token)
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream",
            params={self.tweet_expansion_key: ','.join(self.tweet_expansion_value),
                    self.tweet_fields_key: ','.join(self.tweet_fields_value),
                    self.user_expansion_key: ','.join(self.user_expansion_value)},
            headers=headers,
            stream=True,
        )

        logging.debug('twitter response data')
        logging.debug({self.tweet_expansion_key: ','.join(self.tweet_expansion_value),
                       self.tweet_fields_key: ','.join(self.tweet_fields_value),
                       self.user_expansion_key: ','.join(self.user_expansion_value)})
        logging.debug(response.status_code)
        logging.debug(response)

        if response.status_code != 200:
            raise ConnectionError(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        i = 0
        for line in response.iter_lines():
            if line:
                i += 1
                logging.debug(self.log + "got new message " + str(i))
                # line.decode('utf-8')
                twitter_json = json.loads(line.decode('utf-8'))
                # logging.warning(twitter_json)

                if 'data' in twitter_json and 'includes' in twitter_json and 'matching_rules' in twitter_json \
                        and 'id' in twitter_json['data'] and 'created_at' in twitter_json['data'] \
                        and 'author_id' in twitter_json['data']:
                    e = Event()

                    self.counter += 1

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

                    if 'users' in twitter_json['includes'] and 'referenced_tweets' in twitter_json['data'] \
                            and 'id' in twitter_json['data']['referenced_tweets'][0]:
                        e.data['subj']['original-tweet-url'] = basePid + twitter_json['data']['referenced_tweets'][0][
                            'id']
                        e.data['subj']['original-tweet-author'] = baseAuthorUrl + get_author_name(
                            twitter_json['data']['author_id'], twitter_json['includes']['users'], True)
                    e.data['subj']['alternative-id'] = twitter_json['data']['id']

                    e.data['subj']['data'] = twitter_json['data']
                    e.data['subj']['data']['includes'] = twitter_json['includes']
                    e.data['subj']['data']['matching_rules'] = twitter_json['matching_rules']

                    self.publish(e)
                else:
                    logging.warning(twitter_json)
            else:
                logging.debug('keep alive')

    def run(self):
        while self.running:
            try:
                self.send_data()
            except (requests.Timeout, ValueError,
                    requests.exceptions.ChunkedEncodingError, urllib3.exceptions.InvalidChunkLength,
                    urllib3.exceptions.ReadTimeoutError, urllib3.exceptions.ProtocolError):
                logging.exception(self.log)
            except requests.ConnectionError:
                logging.exception(self.log)
                time.sleep(10)  # sleep a little to avoid issues with max connections
            finally:
                logging.warning('stream restart in 10')
                time.sleep(10)

    @staticmethod
    def start(i=0):
        """start the consumer, wait 5 sec in case its a restart

        Arguments:
        - i: id to use
        """
        time.sleep(10)
        tc = TwitterConnector(i)
        logging.debug(TwitterConnector.log + 'Start %s' % str(i))
        tc.running = True
        tc.run()


def get_author_name(author_id, users, original=False):
    """get the name of the author from the includes

    Arguments:
        - author_id: the author_id
        - users: the users part of the includes
        - original:
    """
    for user in users:
        if ('id' in user and 'username' in user) and (
                (user['id'] == author_id and not original) or user['id'] != author_id and original):
            return user['username']
    return ""


if __name__ == '__main__':
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    SENTRY_TRACE_SAMPLE_RATE = os.environ.get('SENTRY_TRACE_SAMPLE_RATE')
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=SENTRY_TRACE_SAMPLE_RATE
    )

    TwitterConnector.start(1)
