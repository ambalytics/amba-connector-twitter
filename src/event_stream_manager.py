import logging

#from kafka import KafkaConsumer, KafkaProducer


# this is main
class EventStreamManager(object):
    event_string: str = "events"
    state_separator: str = "_"
    relation_type_separator: str = "-"

    config_states = {
        'raw': {
            'own_topic': ['discussed', 'crossref']
        },
        'linked': {
            'own_topic': ['discussed']
        },
        'unknown': {
        },
        'processed': {
            'own_topic': ['discussed']
        },
        'aggregated': {
        }}

    topics = []

    def build_topic_list(self):
        result = []

        for c_state in self.config_states:
            result.append(self.build_topic_name(c_state))
            # print(c_state)
            # print(self.config_states[c_state])
            if 'own_topic' in self.config_states[c_state]:
                for c_o_topic in self.config_states[c_state]['own_topic']:
                    result.append(self.build_topic_name(c_state, c_o_topic))

        self.topics = result
        logging.warning("current topics for events: %s" % self.topics)
        return result

    def build_topic_name(self, state, relation_type=''):
        result = self.event_string + self.state_separator + state

        if relation_type != '':
            result = result + self.relation_type_separator + relation_type
        return result

    # def load_config(self):
    #     #data = yaml.safe_load(open('defaults.yaml'))
    #     #data['url']
    #
    # def connect_kafka_producer(self):
    #     _producer = None
    #     try:
    #         _producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 10))
    #     except Exception as ex:
    #         logging.warning('Exception while connecting Kafka')
    #         logging.warning(str(ex))
    #     finally:
    #         return _producer

        def publish_message(producer_instance, topic_name, key, value):
            try:
                key_bytes = bytes(key, encoding='utf-8')
                value_bytes = value
                producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
                producer_instance.flush()
                logging.warning('Message published successfully.')
            except Exception as ex:
                logging.warning('Exception in publishing message')
                print(str(ex))

    def resolveEvent(self, event):
        topic_name = self.build_topic_name(event['state'], event['relation_type'])
        if topic_name in self.topics:
            return topic_name

        logging.warning("Unable to resolve event, topic_name %s not found" % topic_name)
        return False

    # def get_consumer(self):
    #     topic_name = 12
    #     return KafkaConsumer(topic_name, group_id='worker',
    #                          bootstrap_servers=[self.kafka_server], api_version=(0, 10), consumer_timeout_ms=5000)

    # def get_producer(self):
    #     self.producer = None
    #     try:
    #         self.producer = KafkaProducer(bootstrap_servers=[self.kafka_server], api_version=(0, 10))
    #     except Exception as ex:
    #         logging.warning('Exception while connecting Kafka %s', (str(ex)))
    #     finally:
    #         return _producer

    def kafka_server(self):
        return 'kafka:9092'

    def publish(event):
        print(event)

    def consume(state):
        print(state)


if __name__ == '__main__':
    e = EventStream()
    print(e.build_topic_list())
