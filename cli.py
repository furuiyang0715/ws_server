import json

import websocket
try:
    import thread
except ImportError:
    import _thread as thread
import time


def on_message(ws, message):
    print(f"收到: {message}")
    if message == "error-1":
        print("输入了无法解析的数据")


def on_error(ws, error):
    print(f'连接出现了异常:{error}')


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    def run(*args):
        while True:
            msg = input("请输入get or sub or quit: ")
            dd = {"type": "get"}
            dd.update({"type": msg})
            msg = json.dumps(dd)

            ws.send(msg)
            time.sleep(1)

    print("建立连接成功 ")
    thread.start_new_thread(run, ())


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://127.0.0.1:8888/",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()


