#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/27 16:42
# @Author  : linjinting
# @Site    :
# @Software: redis_sync
# @File    : redis_sync.py
# @Function:
from redis_operator_base import RedisOperatorBase
import threading
import time
# from redis_connection_base import RedisConnectionBase

def get_time_stamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%H:%M:%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s.%03d" % (data_head, data_secs)
    return time_stamp

class HashSetData(object):
    @staticmethod
    def set_value(op, key, value, s_type):

        if s_type == 'server':
            op.hmset(key, value)

        elif s_type == 'rdb':
            for field in value.keys():
                op.hset(key, field, value.get(field))

class ZsetSetData(object):
    @staticmethod
    def set_value(op, key, value, s_type):
        if s_type == 'server':
            op.zadd(key, value)
        elif s_type == 'rdb':
            op.zadd(key, value)

class SetSetData(object):
    @staticmethod
    def set_value(op, key, value, s_type):
        if s_type == 'server':
            op.sadd(key, *value)
        elif s_type == 'rdb':
            op.sadd(key, [value, ])

class ListSetData(object):
    @staticmethod
    def set_value(op, key, value, s_type):
        if s_type == 'server':
                op.rpush(key, *value)
        elif s_type == 'rdb':
            op.rpush(key, [value, ])

class StrSetData(object):
    @staticmethod
    def set_value(op,key, value,s_type):
        if s_type == 'server':
            op.set(key, value)
        elif s_type == 'rdb':
            op.set(key, value)

ops = {
    b'string': StrSetData,
    b'list': ListSetData,
    b'set': SetSetData,
    b'zset': ZsetSetData,
    b'hash': HashSetData,
}

class RedisSync(object):
    def __init__(self,callback_from,s_type="server"):
        self.callback_from = callback_from

        # self.rcb = RedisConnectionBase()
        self.s_type = s_type

    def set_allvalues(self, drconn, key_value):
        if drconn is None:
            return
        # self.conn = drconn
        self.ropb = RedisOperatorBase(drconn)
        th = threading.Thread(target=self.set_allvalues_concurrency, args=(drconn, key_value))
        th.start()
    def set_allvalues_concurrency(self, drconn, _key_value, pipe=True):
        msg = ("[%s]set redis data start\n") % (get_time_stamp())
        self.callback_from.show_msg(msg)
        self.callback_from.show_msg("[%s]set key value\n"%(get_time_stamp()))
        if pipe:
            with drconn.pipeline(transaction=False) as pipe:
                for key, type, value in _key_value:
                    rops = ops.get(type)
                    rops.set_value(pipe, key, value, self.s_type)
                pipe.execute()

        self.ropb.save()
        self.callback_from.show_msg("[%s]save data end~~"%(get_time_stamp()))
        self.callback_from.sync_end()


if __name__ == '__main__':
    srarg_d = {
        "host": "127.0.0.1",
        "port": 6379
    }
    drarg_d = {
        "host": "127.0.0.1",
        "port": 16379
    }
    # RS = RedisSync()
    # RS.set_source_rarg(srarg_d)
    # RS.set_destination_rarg(drarg_d)
    # RS.get_conn()
    # RS.get_keys()
    # RS.get_allvalues()
    # RS.set_allvalues()
    s_t =get_time_stamp()
    print(s_t)
    time.sleep(5)
    print(get_time_stamp())