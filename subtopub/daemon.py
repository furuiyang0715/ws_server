from utils.consumer import create_consumer
from utils.producer import create_producer

class Sync:

    def __init__(self):
        pass

    @property
    def swsp(self):
        # define consumer by total
        TOPIC = "total"
        HOSTS = "192.168.1.152:9092,192.168.1.163:9092"
        GROUP = "total"
        CNID = "1"
        consumer = create_consumer(HOSTS, TOPIC, GROUP, CNID)

        # define producer
        producer = create_producer()

        for msg in consumer:
            trunc = msg.value.decode().split('&')
            ms = '-'.join(trunc[2:6])
            producer.send(ms, msg.value)



if __name__ == '__main__':
    instance = Sync()
    instance.swsp

