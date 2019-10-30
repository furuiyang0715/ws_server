import json
from functools import partial

import websocket
try:
    import thread
except ImportError:
    import _thread as thread


def _on_message(ws, message):
    pass


def _on_error(ws, error):
    # print("info: ", error)
    pass


def _on_close(ws):
    # print("closed")
    pass


def _on_open(ws, **kwargs):
    def run(kwargs):
        msg = json.dumps(kwargs['kwargs'])
        ws.send(msg)

    thread.start_new_thread(run, (kwargs,))


def _process_value(field, value):
    """数据类型处理
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


class Reports(object):
    def __init__(self, params: dict, on_message):
        self.params = params
        self.client_run(params=params, on_message=on_message)

    def client_run(self, url='ws://tap-api.jingzhuan.cn:46399/',
    # def client_run(self, url='ws://127.0.0.1:46399/',
                   on_message=_on_message,
                   on_error=_on_error,
                   on_close=_on_close,
                   on_open=_on_open,
                   debug=False,
                   params={}):
        websocket.enableTrace(debug)
        ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
        on_open = partial(on_open, kwargs=params)
        ws.on_open = on_open
        ws.run_forever()
