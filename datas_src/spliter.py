import datetime
from box import Box


def spliter(msg: bytes):
    message = msg.decode().replace('\x00','').split(',')
    diction = {}
    for one in message:
        content = one.split(':')
        # print(content)
        if content[0] == 'ts':
            diction[content[0]] = ":".join(content[1:])
        else:
            diction[content[0]] = content[1]
        # print(diction)
    return Box(diction)
    
    # return message[1], message[2], message[3], message[4], message[5], message[6]


def datetime_replace(datetime_str: str):
    datetime_datetime = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
    target = datetime_datetime.replace(second=0).strftime('%Y-%m-%d %H:%M:%S')
    return target
