#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/27 16:42
# @Author  : linjinting
# @Site    :
# @Software: redis_sync
# @File    : UI_redis_sync.py
# @Function:
from redis_operator_base import RedisOperatorBase
import threading, time
from queue import Queue
from time_ import get_time_stamp as stime
from operator_ship import ops


class RedisSync(object):
    def __init__(self, callback_msg=None, db_type="server",isPipe=True):
        if callback_msg is None:
            self.callback_msg = self
        else:
            self.callback_msg = callback_msg
        self.db_type = db_type
        self.opera_redis_src = None
        self.opera_redis_dst = None
        self.isPipe = isPipe
        self.pipe_src = None
        self.pipe_dst = None

        self.value_dict = dict()
        self.KeyVals_of_type = dict()
        self.total_count = 0
        self.residue_count = 0
        self.__isEnd = False

    def init_operators(self, conn_src, conn_dst=None):
        if conn_src is None:
            if self.db_type == 'rdb':
                self.callback_msg.show_msg("rdb is empty\n")
            return
        if self.db_type == 'server':
            self.opera_redis_src = RedisOperatorBase(conn_src)
            self.pipe_src = conn_src.pipeline(transaction=False)

        if conn_dst is None:
            return
        self.opera_redis_dst = RedisOperatorBase(conn_dst)
        self.pipe_dst = conn_dst.pipeline(transaction=False)

    def run(self):
        self.queue = Queue()
        get_thread = threading.Thread(target=self.get_keys)
        get_thread.start()

        if not self.isPipe :
            set_thread = threading.Thread(target=self.__set_value)
            set_thread.start()
            count_thread = threading.Thread(target=self.__count)
            count_thread.start()

    def get_keys(self):

        self.callback_msg.show_msg("[%s] start get keys\n" % (stime()))
        self._keys = self.opera_redis_src.scan()
        self.total_count = self.residue_count = len(self._keys)
        if not self._keys:
            self.callback_msg.show_msg("[%s]keys is empty\n" % (stime()))
            return
        if self.isPipe :
            self.__pipe_get()
        else:
            self.__get_value()


    def __pipe_get(self):
        self.callback_msg.show_msg("[%s] start get key type\n" % (stime()))
        with self.pipe_src as pipe:
            for key in self._keys:
                pipe.type(key)
            # print("pipe end")
            _types = pipe.execute()
        keys_types = zip(self._keys, _types)

        self.callback_msg.show_msg("[%s] start get value\n" % (stime()))

        ### pipe
        with self.pipe_src as pipe:
            for key, type_ in keys_types:
                rop = ops.get(type_)
                if rop is None:
                    print(key, type_)
                    continue
                res = rop.get_value(pipe, key)
            _values = pipe.execute()
        _keys_types_values = zip(self._keys, _types, _values)
        self.callback_msg.show_msg("[%s]get data end\n" % (stime()))
        self.pipe_set(_keys_types_values)


    def pipe_set(self, _key_value, ispipe=True):
        self.callback_msg.show_msg(("[%s]set redis data start\n") % (stime()))
        if ispipe:
            with self.pipe_dst as pipe:
                for key, type_, value in _key_value:
                    rops = ops.get(type_)
                    # print(key, value)
                    rops.set_value(pipe, self.db_type, key, value)
                res = pipe.execute()
                # print(res)
        self.callback_msg.show_msg("[%s]set data end~" % (stime()))
        self.opera_redis_dst.save()
        self.callback_msg.show_msg("[%s]save end~" % (stime()))
        self.callback_msg.sync_end()

    def __get_value(self):
        for key in self._keys:
            type_ = self.opera_redis_src.type(key)

            rop_ship = ops.get(type_)
            if rop_ship is None:
                print(key, type_)
                continue
            res = rop_ship.get_value(self.opera_redis_src, key)
            if res is None:
                print(key, self.opera_redis_src)
                raise
            self.value_dict[key] = (res,type_)

            self.queue.put(key)
        self.queue.put('end')

    def __set_value(self):
        while True:
            key = self.queue.get()
            self.queue.task_done()
            data_ = self.value_dict.get(key, None)
            if data_ is None:
                if key == 'end':
                    break
                continue
            value, type_ = data_
            rop = ops.get(type_)
            try:
                rop.set_value(self.opera_redis_dst, self.db_type, key, value)
            except:
                print(key, value)
            self.residue_count -= 1
        self.__isEnd = True
        self.callback_msg.show_msg("[%s]sync end~~" % (stime()))
        self.callback_msg.sync_end()

    def __count(self):
        while True:
            if self.__isEnd:
                break
            self.callback_msg.show_msg("total:%s,residue:%s\n" % (self.total_count, self.residue_count))
            time.sleep(1)

    def show_msg(self, msg):
        print(msg)
    def sync_end(self):
        self.__isEnd = True

if __name__ == '__main__':
    import redis_connection_base

    rc = redis_connection_base.RedisConnectionBase()
    srarg_d = {
        "host": "127.0.0.1",
        "port": 6379
    }
    drarg_d = {
        "host": "10.7.3.53",
        "port": 16379
    }
    RS = RedisSync(isPipe=False)
    conn = rc.get_conn(srarg_d)
    conn_dst = rc.get_conn(drarg_d)
    RS.init_operators(conn,conn_dst)
    RS.run()
