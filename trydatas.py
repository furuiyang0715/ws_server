from datas_src.apis import API
from datas_src.con2 import subscribe


# 查询近 60s 活跃 topic （即60s有数据推送），可订阅，有历史数据
instance = API()
# print(instance.get_active_topic)


# 获取存在数据的topic，不一定能订阅，有历史数据
# print(instance.get_exists_topic)


# 检查topic是否存在
# print(instance.check_exists_topic('NYMEX-F-CL-2101'))


# 查询
ret = instance.select("NYMEX", "F", "CL", "2112", "2018-9-20 12:08:00", "2019-9-28 13:08:00")
for r in ret:
    print(r)

# print(len(ret))


# 批量订阅
# msgs = subscribe(['NYMEX-F-CL-2112', 'CBOT-F-C-2009', 'HKEX-F-LUA-1911'])
# print(msgs)
# for one in msgs:
#     print(one)
#     print()


# 批量订阅
{
    "type": "sub",
    "topics": ['NYMEX-F-CL-2112', 'CBOT-F-C-2009', 'HKEX-F-LUA-1911'],
}

# 单个订阅
{
    "type": "sub",
    "topics": ['NYMEX-F-CL-2112'],
}


# 查询
{
    "type": "get",                 # 必填 str 类型
    "method": "select_topics",     # 必填 str 类型
    "exchangeno": "NYMEX",         # 必填 str 类型
    "commoditytype": "F",          # 必填 str 类型
    "commodityno": "CL",           # 必填 str 类型
    "contractno": "2112",          # 必填 str 类型
    "begin": "2019-9-28 12:08:00",  # 可选 str 类型 默认为 "" 取起止时间与已有数据的交集
    "end": "2019-10-28 12:08:00",   # 可选 str 类型 默认为 "" 取起止时间与已有数据的交集
}

# 固定参数 获取60s内活跃的topic（即60s有数据推送），可订阅，有历史数据
{
    "type": "get",
    "method": "active_topics",
}


# 固定参数 获取存在数据的topic，不一定能订阅，有历史数据
{
    "type": "get",
    "method": "exists_topics",
}




