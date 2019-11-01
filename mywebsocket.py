import base64
import hashlib
import json
import logging
import struct
import threading

from datas_src.apis import API
from datas_src.consumer import subscribe

logger = logging.getLogger("main_log")


class WebSocket(threading.Thread):
    def __init__(self, server, handle_socket, index, real_ip, remote, path=""):
        threading.Thread.__init__(self)
        self.server = server
        self.conn = handle_socket
        self.index = index
        self.name = real_ip
        self.remote = remote
        self.path = path
        self.GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        self.buffer = ""
        self.buffer_utf8 = b""
        self.length_buffer = 0

        # TODO 因为现在是多线程处理  Python的多线程是伪多线程
        # TODO server 是在单个线程中用于接收数据的记录:  g_code_length 和 g_header_length 是进程共享的
        # TODO 阻塞式地每处理完一个 socket 发送来的数据 经以上两个字段清空
        # TODO 有待改进为协程异步的模式
        # self.g_code_length = 0
        # self.g_header_length = 0

        self.handshaken = False

    def close(self):
        """关闭连接
        (1) 现将其从服务端的注册列表中删除
        (2) 将其本质上的 socket 关闭
        (3) 清空接收缓存区中的全局变量
        """
        logger.info(f"closing connection to {self.remote}")
        try:
            self.server.unregister(self.index)
        except Exception as e:
            logger.warning(f"error: server.unregister() exception for {self.remote}: {repr(e)}")

        try:
            self.conn.close()
        except OSError as e:
            logger.warning(f"error: socket.close() exception for {self.remote}: {repr(e)}")
        finally:
            # Delete reference to socket object for garbage collection
            self.conn = None
        self._reset_recv_info()

    def _generate_token(self, ws_key):
        """
        拿到客户端的 ws_key 生成 ws_token
        """
        ws_key += self.GUID
        ser_websocketkey = hashlib.sha1(ws_key.encode(encoding='utf-8')).digest()
        websocket_token = base64.b64encode(ser_websocketkey)
        return websocket_token.decode('utf-8')

    def _split_recv_buffer(self):
        """
        提取 ws 请求头信息在一个字典里
        """
        headers = {}
        header, data = self.buffer.split('\r\n\r\n', 1)
        for line in header.split("\r\n")[1:]:
            key, value = line.split(": ", 1)
            headers[key] = value
        return headers

    def _start_handshaken(self):
        """
        握手，升级 websocket 协议
        """
        logger.debug(f'Socket {self.index} Start Handshaken with {self.remote}!')
        self.buffer = self.conn.recv(1024).decode('utf-8')
        logger.debug(f"Socket {self.index} self.buffer is {self.buffer}")
        if self.buffer.find('\r\n\r\n') == -1:
            raise RuntimeError("socket error!")
        headers = self._split_recv_buffer()
        websocketkey = headers.get("Sec-WebSocket-Key", None)
        if websocketkey is None:
            raise RuntimeError("socket error: 未找到 Sec-WebSocket-Key")
        websocket_token = self._generate_token(websocketkey)
        headers["Location"] = ("ws://%s%s" % (headers["Host"], self.path))
        handshake = "HTTP/1.1 101 Switching Protocols\r\n" \
                    "Connection: Upgrade\r\n" \
                    "Sec-WebSocket-Accept: " + websocket_token + "\r\n" \
                                                                 "Upgrade: websocket\r\n\r\n"
        self.conn.send(handshake.encode(encoding='utf-8'))

    def _convert_str_message(self, msg):
        """
        打包将要发送给客户端的数据
        server 使用 socket 将转换后的消息发送给 cli
        :param msg: bytes 或 str 类型的
        :return:
        """
        if isinstance(msg, bytes):
            bytes_msg = msg
        elif isinstance(msg, str):
            bytes_msg = msg.encode("utf-8")
        else:
            raise ValueError

        send_msg = b""
        send_msg += b"\x81"
        back_str = []
        back_str.append('\x81')

        data_length = len(bytes_msg)

        if data_length <= 125:
            send_msg += str.encode(chr(data_length))
        elif data_length <= 65535:
            send_msg += struct.pack('b', 126)
            send_msg += struct.pack('>h', data_length)
        elif data_length <= (2 ^ 64 - 1):
            send_msg += struct.pack('b', 127)
            send_msg += struct.pack('>q', data_length)
        else:
            raise RuntimeError("发送数据过长")

        send_message = send_msg + bytes_msg
        return send_message

    def _broadcast_message(self, send_message):
        if send_message is not None and len(send_message) > 0:
            to_del_idx = None
            for c_idx, connection in self.server.connections.items():
                try:
                    connection.send(send_message)
                except BrokenPipeError:
                    logger.warning(f'广播过程中 {c_idx} 已经断开')
                    to_del_idx = c_idx
                except Exception as e:
                    logger.warning(f"广播过程中 {c_idx} 出现了异常:{e}")
                    to_del_idx = c_idx
            if to_del_idx is not None:
                self.server.delete_connection(c_idx)

        self.server.g_code_length = 0

    def push(self, msg):
        """向单个 cli 推送消息
        """
        str_msg = self._convert_str_message(msg)
        if str_msg is not None and len(str_msg) > 0:
            self.server.connections["connection" + str(self.index)].send(str_msg)
        self.server.g_code_length = 0

    def broadcast(self, msg):
        """server 向每一个 cli 广播信息
        """
        str_msg = self._convert_str_message(msg)
        self._broadcast_message(str_msg)

    def _cal_msg_length(self, msg):
        """
        根据协议计算 server 待接收数据的长度信息
        """
        code_length = msg[1] & 127
        if code_length == 126:
            code_length = struct.unpack('>H', msg[2:4])[0]
            header_length = 8
        elif code_length == 127:
            code_length = struct.unpack('>Q', msg[2:10])[0]
            header_length = 14
        else:
            header_length = 6
        code_length = int(code_length)

        return header_length, code_length

    def _parse_cli_data(self, msg):
        """
        解析从客户端收到的数据
        """
        code_length = msg[1] & 127

        if code_length == 126:
            self.g_code_length = struct.unpack('>H', msg[2:4])[0]
            masks = msg[4:8]
            data = msg[8:]

        elif code_length == 127:
            self.g_code_length = struct.unpack('>Q', msg[2:10])[0]
            masks = msg[10:14]
            data = msg[14:]
        else:
            masks = msg[2:6]
            data = msg[6:]

        en_bytes = b""
        cn_bytes = []

        # nv 属于解除掩码之后的数据 通过 nv.encode() 的方式编码之后可计算编码后的 bytes 的长度
        # 如果长度不为1，则作为非英文字符处理，考虑到中英文混杂的情况，需要添加占位符
        for i, d in enumerate(data):
            nv = chr(d ^ masks[i % 4])
            nv_bytes = nv.encode()
            nv_len = len(nv_bytes)
            if nv_len == 1:
                en_bytes += nv_bytes
            else:
                en_bytes += b'%s'
                cn_bytes.append(ord(nv_bytes.decode()))

        if len(cn_bytes) > 2:
            cn_str = ""
            clen = len(cn_bytes)
            count = int(clen / 3)
            for x in range(count):
                i = x * 3
                b = bytes([cn_bytes[i], cn_bytes[i + 1], cn_bytes[i + 2]])
                cn_str += b.decode()
            new = en_bytes.replace(b'%s%s%s', b'%s')
            new = new.decode()
            res = (new % tuple(list(cn_str)))
        else:
            res = en_bytes.decode()
        return res

    def _reset_recv_info(self):
        """清空全局变量
        """
        self.server.g_code_length = 0
        self.server.g_header_length = 0
        self.length_buffer = 0
        self.buffer_utf8 = b""

    def run(self):
        """
        一个 hand_socket 处理的过程要经历：
        （1） 完成握手 升级 websocket 协议
        （2） 在一个事件循环中处理请求和订阅任务 会有以下几种情况：
                (1) 中间过程异常 线程退出 在外层的 run 中整体捕获
                (2) 正确处理请求，线程正常退出
                (3) 多线程持续处理订阅任务 断连时在客户端维护重连机制
        :return:
        """
        try:
            self._run()
        except Exception as e:
            logger.warning(f"线程异常:{e} 请及时检查")
            self.close()

    def _run(self):
        logger.info(f'Handle Socket {self.index} Start!')
        while True:
            if self.handshaken is False:
                self._start_handshaken()
                self.handshaken = True
            else:
                part_msg = self.conn.recv(128)
                # 计算待接收数据的总长度，判断是否接收完，如未接受完需要继续接收 并将计算出的长度预设给 server 的全局变量
                if self.server.g_code_length == 0:   # 说明是第一次接收
                    self.server.g_header_length, self.server.g_code_length = self._cal_msg_length(part_msg)
                # 已经接收到的长度
                self.length_buffer += len(part_msg)
                # 已经接收到的bytes内容
                self.buffer_utf8 += part_msg
                # 已经接受的长度减去头部信息的长度小于从头部信息里面计算处出来的实际长度 就是没接收完成
                if self.length_buffer - self.server.g_header_length < self.server.g_code_length:
                    logger.info("数据未接收完,接续接收中")
                    continue
                else:
                    logger.info(f"g_code_length:{self.server.g_code_length}")
                    logger.info(f"Recv信息 {self.buffer_utf8},长度为 {len(self.buffer_utf8)}")
                    if not self.buffer_utf8:
                        raise RuntimeError(f"未接收到到有效信息，请检查参数!")
                    try:
                        recv_message = self._parse_cli_data((self.buffer_utf8))
                    except Exception as e:
                        raise RuntimeError(f"消息无法解析{e}，请检查参数")

                    try:
                        self._process_request(recv_message)
                    except Exception as e:
                        raise RuntimeError(f'{self.index}请求处理异常{e} 请联系管理员')

                    # 非订阅任务正确处理之后会进入到此流程
                    self._reset_recv_info()
                    self.close()

    def _process_request(self, recv_message):
        """
        分流处理请求
        """
        py_type_msg = json.loads(recv_message)
        logger.info(f"接收到的请求参数是: {py_type_msg}")
        if py_type_msg.get("type") == "get":
            g_ret = None   # 一次性返回的查询请求
            s_ret = None   # 订阅请求以及多次返回的查询请求
            instance = API()
            if py_type_msg.get("method", None) == "active_topics":
                g_ret = instance.get_active_topic
            elif py_type_msg.get("method", None) == "exists_topics":
                g_ret = instance.get_exists_topic
            elif py_type_msg.get("method", None) == "select_topics":
                exchangeno = py_type_msg.get("exchangeno", None)
                commoditytype = py_type_msg.get("commoditytype", None)
                commodityno = py_type_msg.get("commodityno", None)
                contractno = py_type_msg.get("contractno", None)
                begin = py_type_msg.get('begin', None)
                end = py_type_msg.get("end", None)
                s_ret = instance.select(exchangeno, commoditytype, commodityno, contractno, begin, end)
            else:
                raise RuntimeError("请求参数异常")
            if g_ret:
                self.push(json.dumps(g_ret))
            if s_ret:
                for data in s_ret:
                    # TODO 这样推送过去 客户端还需要进行处理
                    self.push(data)

        elif py_type_msg.get("type") == "sub":
            topics = py_type_msg.get("topics")
            if not isinstance(topics, list):
                topics = [topics]
            sub_generator = subscribe(topics)
            for data in sub_generator:
                # TODO 同上
                self.push(json.dumps(data))
        else:
            raise RuntimeError("请求参数异常")
