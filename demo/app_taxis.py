import os
import uuid
import random

from flask import Flask, render_template, Response, json, request, send_file
from pykafka import KafkaClient
from pykafka.common import OffsetType
from datetime import datetime


def get_kafka_client():
    return KafkaClient(hosts='localhost:9092', broker_version="3.0.0")

app = Flask(__name__)

@app.route('/static/images/<file_name>')
def get_image(file_name):
    img_dir = "static/images"
    img_path = os.path.join(img_dir, file_name)
    return send_file(img_path, mimetype='image/png')


@app.route('/')
def index():
    return(render_template('indexTaxis.html'))

#Consumer API
@app.route('/topic/<topicname>')
def get_messages(topicname):
    client = get_kafka_client()
    def events():

        consumer = client.topics[topicname].get_simple_consumer(consumer_group="mygroup",
                                                                auto_offset_reset=OffsetType.LATEST)
        consumer.consume()

        for i in consumer:
            consumer.commit_offsets()
            yield 'data:{0}\n\n'.format(i.value.decode())
    return Response(events(), mimetype="text/event-stream")

#Producer API
@app.route('/topic/<topicname>', methods=['POST'])
def post_message(topicname):
    r_json = json.loads(request.data)
    rnd = random.Random()
    rnd.seed(100)
    p_uuid = str(uuid.UUID(int=rnd.getrandbits(128), version=4))
    data = {
        "personId": {
            "id": p_uuid
        },
        "position": {
            "type": "Point",
            "coordinates": [r_json['lon'], r_json['lat']]
        }
    }
    key = {
        "id": p_uuid
    }
    msgKey = json.dumps(key)
    message = json.dumps(data)
    print(msgKey)
    print(message)
    client = get_kafka_client()
    topic = client.topics['people']
    producer = topic.get_sync_producer()
    producer.produce(message.encode('utf8'), partition_key=msgKey.encode('utf8'), timestamp=datetime.now())
    return {}

if __name__ == '__main__':
    app.run(debug=True, port=6002)