import concurrent.futures
from os import listdir
from os.path import isfile, join

from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time




#CONSTRUCT MESSAGE AND SEND IT TO KAFKA
data = {
    "id": {
        "id": None
    },
    "position": {
        "type": "Point",
        "coordinates": None
    }
}

def generate_checkpoint(lines):
    # KAFKA PRODUCER
    client = KafkaClient(hosts="localhost:9092", broker_version="3.0.0")
    topic = client.topics['taxiTopic']
    producer = topic.get_sync_producer()

    i = 0
    while i < len(lines):
        line = lines[i]
        line_fields = line.split(',')
        data['id']['id'] = line_fields[0]
        data['position']['coordinates'] = [float(line_fields[1]), float(line_fields[2])]

        message = json.dumps(data)
        print(message)
        key = {
            "id": line_fields[0]
        }
        msgKey = json.dumps(key)
        producer.produce(message.encode('utf8'), partition_key=msgKey.encode('utf8'), timestamp=datetime.now())
        time.sleep(1)

        #if bus reaches last coordinate, start from beginning
        if i == len(lines)-1:
            i = 0
        else:
            i += 1


def process_file(file_name):
    with open(file_name, 'r') as f:
        lines = f.readlines()
    generate_checkpoint(lines)


if __name__ == '__main__':
    # READ COORDINATES FROM GEOJSON

    file_names = [f'data/{f}' for f in listdir('data') if isfile(join('data', f))]

    print(f'found {len(file_names)} files')

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(file_names)) as executor:
        executor.map(process_file, file_names)





