from pykafka import KafkaClient
from pykafka.common import OffsetType
from box import Box

from datas_src.rds import RDS
from datas_src.spliter import spliter


def KafkaDownloader(host_, topic_, group_id_=None, consumer_id_=None, pack=False, simple=None):
    client = KafkaClient(hosts=host_)
    _topic = client.topics[bytes(topic_.encode())]
    if group_id_ and consumer_id_:
        if simple:
            consumer = _topic.get_simple_consumer(
                consumer_group=bytes(group_id_.encode()),
                consumer_id=bytes(consumer_id_.encode()),
                auto_offset_reset=OffsetType.EARLIEST,
                auto_commit_enable=True,
                reset_offset_on_start=True,
                auto_commit_interval_ms=1)
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


def tmp_sub(HOSTS, TOPIC, GROUP, CNID, pack=None):
    consumer = build_consumer(HOSTS, TOPIC, GROUP, CNID)
    [one for one in consumer]


def build_consumer(host_, topic_, group_id_=None, consumer_id=None, pack=None, simple=None):
    return KafkaDownloader(host_, topic_, group_id_, consumer_id, pack=pack, simple=simple)


def create_consumer(host_, topic_, group_id_=None, consumer_id=None, pack=None, simple=False):
    # if not simple:
    #     p = Process(target=tmp_sub, args=(host_, topic_, group_id_, consumer_id, pack))
    #     p.start()
    #     p.join(timeout=2)
    if simple:
        group_id_ = "all"
        consumer_id = "1"
    lds = build_consumer(host_, topic_, group_id_, consumer_id, pack=pack, simple=simple)
    return lds


def subscribe(topic):
    last_ts = ""
    rds_instance = RDS()
    subs = rds_instance.subscribe(topic)
    while True:
        msg = subs.get_message()
        if msg and msg['type'] != 'subscribe':
            r_msg = spliter(Box(msg).data)
            # print(r_msg)
            if not last_ts:
                last_ts = r_msg.ts
                yield r_msg
                # continue
            else:
                if r_msg.ts > last_ts:
                    # print("check", r_msg.ts, last_ts)
                    last_ts = r_msg.ts
                    yield r_msg
                else:
                    # print("uncheck", r_msg.ts, last_ts)
                    continue
            # yield r_msg


if __name__ == '__main__':
    msg = subscribe("CME-F-JY-1912")
    for one in msg:
        print(one)
