
# Set the API Invoke URLs for each Kafka topic
PIN_INVOKE_URL_KAFKA = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/topics/124f8314c0a1.pin"
GEO_INVOKE_URL_KAFKA = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/topics/124f8314c0a1.geo"
USER_INVOKE_URL_KAFKA = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/topics/124f8314c0a1.user"


# Set the API Invoke URLs for each Kinesis topic
PIN_INVOKE_URL_KINESIS = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-124f8314c0a1-pin/record"
GEO_INVOKE_URL_KINESIS = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-124f8314c0a1-geo/record"
USER_INVOKE_URL_KINESIS = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-124f8314c0a1-user/record"


KAFKA_HEADERS = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
KINESIS_HEADERS = {'Content-Type': 'application/json'}