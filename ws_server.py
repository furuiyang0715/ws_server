import logging.config
import os
import socket
import sys

from mywebsocket import WebSocket

from configs import WEBSOCKETSERVERHOST, WEBSOCKETSERVERPORT, WEBSOCKETSERVERCONNECTIONS


class WebSocketServer(object):
    def __init__(self):
        self.connections = self._get_connections()
        self.connection_index = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.g_code_length = 0
        self.g_header_length = 0

    def _get_connections(self):
        return {}

    def _listen_socket_init(self):
        """
        初始化 server 的 listen socket
        """
        ip = WEBSOCKETSERVERHOST
        port = WEBSOCKETSERVERPORT
        max_connections = WEBSOCKETSERVERCONNECTIONS

        logger.info(f"WebServer is listening {ip},{port}")
        # Avoid bind() exception: OSError: [Errno 48] Address already in use
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((ip, port))
        self.socket.listen(max_connections)

    def register(self, connection):
        """
        注册 server 端的连接数加 + 1
        """
        self.connections['connection' + str(self.connection_index)] = connection
        self.connection_index += 1
        logger.info(f"now connections: {self.connections}")
        logger.info(f"now max connection index: {self.connection_index}")

    def unregister(self, connection_index: str):
        """
        取消注册 传入的均是单个数值 与 register 保持一致
        """
        c_idx = 'connection' + str(connection_index)
        del self.connections[c_idx]

    def begin(self):
        logger.info('WebSocketServer Start!')
        self._listen_socket_init()
        try:
            while True:
                handle_socket, address = self.socket.accept()
                logger.debug(f"连接上客户端 {address}, 生成 handle socket {handle_socket} 去处理")
                handle_wssocket = WebSocket(self, handle_socket, self.connection_index, address[0],
                                            address, path=None)
                # try:
                #     handle_wssocket.start()
                # except Exception as e:
                #     logger.warning(f" handle socket 线程异常退出, 原因: {e} ")
                #     handle_socket.close()
                handle_wssocket.start()
                self.register(handle_socket)
        except KeyboardInterrupt:   # ctrl+c 终止了服务端程序等
            logger.warning("caught keyboard interrupt, exiting")
        finally:
            # 退出服务端进程
            sys.exit(0)


logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "[%(levelname)1.1s %(asctime)s|%(module)s|%(funcName)s|%(lineno)d] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": 'simple',
            "stream": "ext://sys.stdout"
        },
        "main_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "logs/wsserver.log"),
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },
    },
    "loggers": {
        "main_log": {
            "level": "DEBUG",
            "handlers": ["console", "main_file_log"]
        },
    }
})


logger = logging.getLogger("main_log")