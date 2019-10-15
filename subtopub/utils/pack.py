import struct

class pack:
    """
    消息头：版本（1字节） + 消息类型（1字节）+ 消息体长度（4字节）
    消息体：待发送消息体
    """
    def pack(self, version, msg_type, msg_len, msg):
        binary_msg = struct.pack("<BBI" + len(msg) + "s", version, msg_type, msg_len, msg)
        return binary_msg
    
    def unpack(self, b_type, stream):
        target = struct.unpack(b_type, stream)
        return target[0]

