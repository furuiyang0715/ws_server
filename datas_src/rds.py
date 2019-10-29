import redis

from configs import REDIS_URL


class RDS:

    def __init__(self):
        self.__conn = redis.Redis(host=REDIS_URL, port=6377, decode_responses=False, db=3)

    def publish(self, chan_sub, msg):
        self.__conn.publish(chan_sub, msg)
        return True
    
    def subscribe(self, chan_sub):
        pub = self.__conn.pubsub()
        pub.subscribe(chan_sub)
        return pub


class RDSBase:

    def __init__(self):
        self.rds3 = redis.Redis(host=REDIS_URL, port=6377, decode_responses=False, db=3)
        self.rds4 = redis.Redis(host=REDIS_URL, port=6377, decode_responses=False, db=4)
        self.rds5 = redis.Redis(host=REDIS_URL, port=6377, decode_responses=False, db=5)
        self.rds3_pipe = self.rds3.pipeline(transaction=True)
        self.rds4_pipe = self.rds4.pipeline(transaction=True)
        self.rds5_pipe = self.rds5.pipeline(transaction=True)
