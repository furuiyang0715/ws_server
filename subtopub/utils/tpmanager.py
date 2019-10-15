from kafka import KafkaConsumer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

servers = ["192.168.1.152:9092", "192.168.1.163:9092"]

# 获取所有topic
def retrieve_topics():
    consumer = KafkaConsumer(bootstrap_servers=servers)
    return consumer.topics()

# 获取topic的分区列表
def retrieve_partitions(topic):
    consumer = KafkaConsumer(bootstrap_servers=servers)
    print(consumer.partitions_for_topic(topic))

# 获取Consumer Group对应的分区的当前偏移量
def retrieve_partition_offset(topic_name_, group_id_):
    consumer = KafkaConsumer(bootstrap_servers=servers,
                             group_id=group_id_)
    tp = TopicPartition(topic_name_, 0)
    consumer.assign([tp])
    print("starting offset is ", consumer.position(tp))

# 创建topic
def create_topic(topic_name_):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        new_topic = NewTopic(topic_name_, 8, 3)
        admin.create_topics([new_topic])
    except TopicAlreadyExistsError as e:
        print(e.message)

# 删除topic
def delete_topic(topic_name_):
    admin = KafkaAdminClient(bootstrap_servers=servers)
    admin.delete_topics([topic_name_])

# 获取消费组信息    
def get_consumer_group(group_id_):
    # 显示所有的消费组
    print(admin.list_consumer_groups())

    # 显示消费组的offsets
    print(admin.list_consumer_group_offsets(group_id_))


if __name__ == '__main__':
    res = retrieve_topics()
    print(res)

