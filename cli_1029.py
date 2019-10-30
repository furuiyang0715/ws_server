import json
from tap_tools import _process_value, Reports


my_request_params4 = {
    "type": "sub",
    "topics": ['NYMEX-F-CL-2112', 'CBOT-F-C-2009', 'HKEX-F-LUA-1911'],  # 批量
    # "topics": ['NYMEX-F-CL-2112'],                                    # 单个
}


def my_on_msg4(ws, message):
    msg = json.loads(message)
    for key, value in msg.items():
        msg[key] = _process_value(key, value)
    print(msg)


Reports(params=my_request_params4, on_message=my_on_msg4)

