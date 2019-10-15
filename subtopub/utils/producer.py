import msgpack
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error(exc_into=excp)


def create_producer():
    server_list = ['192.168.1.152:9092', '192.168.1.163:9092']
    producer = KafkaProducer(bootstrap_servers=server_list, value_serializer=msgpack.dumps) #.add_callback(on_send_success).add_errback(on_send_error)
    return producer



if __name__ == '__main__':
    topic = 'tes-t'
    content = b'0&2019-10-14 14:50:57.342&COMEX&F&GC&1912&1492.1'
    producer = create_producer()
    producer.send(topic,content)


