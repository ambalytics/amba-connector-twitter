# amba-connector-twitter

The twitter connector is the main source of event data. It connects the
Twitter API [^1] with the Amba Analysis Stream platform. Therefore, the
connector uses a combination of keys, and access to a *filtered stream*
with a defined set of attributes. The filtered stream, a Twitter API
feature, is configured per account prior to accessing the stream.
Filters may contain a list of rules, in our case a list of domains that
contain the most popular publisher as well as plain doi.org URLs. 
If tweets match these rules, they
are added to the filtered stream and are accessible by the twitter
connector. The Twitter connector is implemented in python and uses the
amba-event-streams package.

For a successful connection and initiation of a tweet stream, a
so-called *Bearer Token* that identifies the account is necessary. The
token further ensures that the correct rules are applied. The Twitter
API account used is an academic account provided by Twitter, that allows
retrieving up to 10 million tweets a month. The data retrieved from
Twitter can be adjusted by setting “keys” in the request. These keys
allow to extend the retrieved Twitter data in JSON format and may add
data about the tweet author, referenced tweets and additional tweet
data.

Once established, the connection persists and every received tweet is
decoded and transformed into an “amba-event” object. At this point, the
subject of the amba-event, i.e., the tweet, is filled with data
extracted from the JSON. The state of this event is set to *unlinked*
since the connection to the object, the publication, has not been made
yet. Once completed setting the subject and general attributes, the
event is published to Kafka.

In case of connection issues or any sorts of error, the connector is
able to recognize such errors and restarts the connection. In order to
ensure reliability, a processing thread periodically checks how many
tweets have been received. If there are no tweets for multiple
consecutive checks, the twitter connector container is restarted to
reset to working state.

[^1]: <https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/introduction>, APIv2 