import json

from tap_tools import _process_value, Reports

if __name__ == "__main__":
    # # （1）获取60s内活跃的topic（即60s有数据推送），可订阅，有历史数据
    # my_request_params1 = {"type": "get", "method": "active_topics"}
    #
    # def my_on_msg1(ws, message):
    #     """
    #     自定义拿到数据之后的处理
    #     """
    #     msg = json.loads(message)
    #     print(msg)
    #
    # Reports(params=my_request_params1, on_message=my_on_msg1)

    # # （2） 获取存在数据的topic，不一定能订阅，有历史数据
    # my_request_params2 = {"type": "get", "method": "exists_topics"}
    #
    # def my_on_msg2(ws, message):
    #     msg = json.loads(message)
    #     print(msg)
    #
    # Reports(params=my_request_params2, on_message=my_on_msg2)

    # （3）查询历史数据
    my_request_params3 = {
        "type": "get",              # 必填 str 类型
        "method": "select_topics",  # 必填 str 类型
        "exchangeno": "NYMEX",      # 必填 str 类型
        "commoditytype": "F",       # 必填 str 类型
        "commodityno": "CL",        # 必填 str 类型
        "contractno": "2112",       # 必填 str 类型
        # "begin": "2019-9-20 12:08:00",  # 可选 str 类型 默认为 "" 取起止时间与已有数据的交集
        # "end": "2019-9-28 13:08:00",   # 可选 str 类型 默认为 "" 取起止时间与已有数据的交集
    }

    lst = []
    def my_on_msg3(ws, message):
        msg = json.loads(message)
        for key, value in msg.items():
            msg[key] = _process_value(key, value)
            print(msg)
            lst.append(msg)

    report3 = Reports(params=my_request_params3, on_message=my_on_msg3)
    print(len(lst))

    # # （4) 单个或者批量订阅
    # my_request_params4 = {
    #     "type": "sub",
    #     "topics": ['NYMEX-F-CL-2112', 'CBOT-F-C-2009', 'HKEX-F-LUA-1911'],  # 批量
    #     # "topics": ['NYMEX-F-CL-2112'],                                    # 单个
    # }
    #
    # def my_on_msg4(ws, message):
    #     msg = json.loads(message)
    #     for key, value in msg.items():
    #         msg[key] = _process_value(key, value)
    #     print(msg)
    #     print()
    #
    # report4 = Reports(params=my_request_params4, on_message=my_on_msg4)
    #

    pass
