from pykafka import KafkaClient
from pykafka.common import OffsetType
from multiprocessing import Process
import msgpack
import time


def KafkaDownloader(host_, topic_, group_id_=None, consumer_id_=None, pack=False):
    client = KafkaClient(hosts=host_)
    _topic = client.topics[bytes(topic_.encode())]
    if group_id_ and consumer_id_:
        if pack:
            consumer = _topic.get_simple_consumer(
                consumer_group=bytes(group_id_.encode()),
                consumer_id=bytes(consumer_id_.encode()),
                auto_offset_reset=OffsetType.LATEST,
                auto_commit_enable=True,
                reset_offset_on_start=False,
                auto_commit_interval_ms=1,
                deserializer=msgpack.dumps
            )
        else:
            consumer = _topic.get_simple_consumer(
                consumer_group=bytes(group_id_.encode()),
                consumer_id=bytes(consumer_id_.encode()),
                auto_offset_reset=OffsetType.LATEST,
                auto_commit_enable=True,
                reset_offset_on_start=False,
                auto_commit_interval_ms=1
            )
    else:
        consumer = _topic.get_simple_consumer(
            auto_offset_reset=OffsetType.LATEST,
            reset_offset_on_start=False
        )
    if consumer:
        for msg in consumer:
            if msg:
                yield msg

def tmp_sub(HOSTS, TOPIC, GROUP, CNID):
    consumer = build_consumer(HOSTS, TOPIC, GROUP, CNID)
    [one for one in consumer]


def build_consumer(host_, topic_, group_id_=None, consumer_id=None):
    return KafkaDownloader(host_, topic_, group_id_, consumer_id)

def create_consumer(host_, topic_, group_id_=None, consumer_id=None):
    p = Process(target=tmp_sub, args=(host_, topic_, group_id_, consumer_id,))
    p.start()
    p.join(timeout=2)
    lds = build_consumer(host_, topic_, group_id_, consumer_id)
    return lds

if __name__ == '__main__':
    TOPIC = "total"
    HOSTS = "192.168.1.152:9092,192.168.1.163:9092"
    GROUP = "total2"
    CNID = "2"
    lds = create_consumer(HOSTS, TOPIC, GROUP, CNID)
    for msg in lds:
        print(msg.value)

