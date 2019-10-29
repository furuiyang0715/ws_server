import json

import websocket
try:
    import thread
except ImportError:
    import _thread as thread


def _process_value(field, value):
    """数据处理
    """
    str_fields = [
        "exchangeno",
        "commoditytype",
        "commodityno",
        "contractno",
    ]

    repeated_fields = [
        "qbidprice",
        "qbidqty",
        "qaskprice",
        "qaskqty",
    ]

    float_fields = [
        "qpreclosingprice",
        "qlastprice",
        "qopeningprice",
        "qhighprice",
        "qlowprice",
        "qhishighprice",
        "qhislowprice",
        "qlimitupprice",
        "qlimitdownprice",
        "qtotalqty",
        "qtotalturnover",
        "qpositionqty",
        "qaverageprice",
        "qclosingprice",
        "qsettleprice",
        "qlastqty",
        "qimpliedbidprice",
        "qimpliedbidqty",
        "qimpliedaskprice",
        "qimpliedaskqty",
        "qpredelta",
        "qcurrdelta",
        "qinsideqty",
        "qoutsideqty",
        "qturnoverrate",
        "q5davgqty",
        "qperatio",
        "qtotalvalue",
        "qnegotiablevalue",
        "qpositiontrend",
        "qchangespeed",
        "qchangerate",
        "qchangevalue",
        "qswing",
        "qtotalbidqty",
        "qtotalaskqty",
        ""
    ]
    if field == "ts":
        pass
    elif field in str_fields:
        value = str(value)
    elif field in float_fields:
        value = float(value)
    elif field in repeated_fields:
        lst = value.strip("/").split("/")
        value = [float(v) for v in lst]
    else:
        pass
    return value


def on_message(ws, message):
    try:
        msg = json.loads(message)
    except Exception as e:
        print(f"提示信息: {message}")
    else:
        for key, value in msg.items():
            msg[key] = _process_value(key, value)
        print(msg)
        print()
        print()


def on_error(ws, error):
    print(f'连接异常:{error}')


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    def run(*args):
        dd = {"type": "sub", "topic": 'CME-F-JY-1911'}
        msg = json.dumps(dd)
        ws.send(msg)

    print("建立连接成功 ")
    thread.start_new_thread(run, ())


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://tap-api.jingzhuan.cn:46399/",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()


