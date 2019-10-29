import datetime

from datas_src.rds import RDSBase


class API(RDSBase):

    def select(self, exchangeno: str, commoditytype: str, commodityno: str, contractno: str,
               begin: str = "", end: str = ""):
        topic = '-'.join([exchangeno, commoditytype, commodityno, contractno])
        tmp = self.check_exists_topic(topic)
        if not tmp:
            yield b"Topic Not Exists."
            exit()
        self.rds4_pipe.zrange(f"ts:{topic}", 0, 0)
        self.rds4_pipe.zrevrange(f"ts:{topic}", 0, 0)
        result = self.rds4_pipe.execute()
        min_one = result[0][0].decode()
        max_one = result[-1][0].decode()
        if begin:
            format_begin_datetime = datetime.datetime.strptime(begin, '%Y-%m-%d %H:%M:%S')
            format_begin_str = format_begin_datetime.strftime("%Y%m%d%H%M")
            if format_begin_str > min_one:
                start = format_begin_str
            else:
                start = min_one
        else:
            start = min_one
        if end:
            format_end_datetime = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
            format_end_str = format_end_datetime.strftime("%Y%m%d%H%M")
            if format_end_str < max_one:
                stop = format_end_str
            else:
                stop = max_one
        else:
            stop = max_one
        # print(start, stop)
        target_range = self.rds4.zrangebyscore(f"ts:{topic}", start, stop)
        # print(target_range)
        for one in target_range:
            result = self.rds3.hget(f"data:mins:{topic}", one)
            yield result

    # 获取60s内活跃的topic（即60s有数据推送），可订阅，有历史数据
    @property
    def get_active_topic(self):
        ac_list = self.rds5.keys()
        ac_c_list = [one.decode().split(':')[-1] for one in ac_list]
        return ac_c_list

    # 获取存在数据的topic，不一定能订阅，有历史数据
    @property
    def get_exists_topic(self):
        ac_list = self.rds3.keys()
        ac_c_list = [one.decode().split(':')[-1] for one in ac_list]
        return ac_c_list

    # 检查topic是否存在
    def check_exists_topic(self, topic:str):
        result = self.rds3.exists(f"data:mins:{topic}")
        if result:
            return True
        return False
