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

class RedisSync(object):
    def __init__(self,callback_from,s_type="server"):
        self.callback_from = callback_from

        # self.rcb = RedisConnectionBase()
        self.s_type = s_type

    def set_allvalues(self, drconn, hash_keyvalue, zset_keyvalue, set_keyvalue, list_keyvalue, str_keyvalue):
        if drconn is None:
            return
        # self.conn = drconn
        self.ropb = RedisOperatorBase(drconn)
        th = threading.Thread(target=self.set_allvalues_concurrency, args=(drconn, hash_keyvalue, zset_keyvalue, set_keyvalue, list_keyvalue, str_keyvalue))
        th.start()


    def set_allvalues_concurrency(self, drconn, hash_keyvalue, zset_keyvalue, set_keyvalue, list_keyvalue, str_keyvalue, pipe=True):
        msg = ("[%s]set redis data start\n") % (get_time_stamp())
        self.callback_from.show_msg(msg)
        if hash_keyvalue:
            msg = ("[%s]set hash key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if pipe:
                with drconn.pipeline(transaction=False) as pipe:
                    self.__set_hash_value(pipe, hash_keyvalue)
                    pipe.execute()
            else:
                self.__set_hash_value(self.ropb, hash_keyvalue)
                # if self.s_type == 'server':
                #     for key in hash_keyvalue.keys():
                #         self.ropb.hmset(key, hash_keyvalue.get(key))
                # elif self.s_type == 'rdb':
                #     for hashkey in hash_keyvalue:
                #         self.ropb.hset(hashkey[0],hashkey[1],hashkey[2])

        if zset_keyvalue:
            msg = ("[%s]set zset key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if pipe:
                with drconn.pipeline(transaction=False) as pipe:
                    self.__set_zset_value(pipe, zset_keyvalue)
                    pipe.execute()
            else:
                self.__set_zset_value(self.ropb, zset_keyvalue)

        if set_keyvalue:
            msg = ("[%s]set set key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if pipe:
                with drconn.pipeline(transaction=False) as pipe:
                    self.__set_set_value(pipe, set_keyvalue)
                    pipe.execute()
            else:
                self.__set_set_value(self.ropb, set_keyvalue)

        if list_keyvalue:
            msg = ("[%s]set list key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if pipe:
                with drconn.pipeline(transaction=False) as pipe:
                    self.__set_list_value(pipe, list_keyvalue)
                    pipe.execute()
            else:
                self.__set_list_value(self.ropb, list_keyvalue)

        if str_keyvalue:
            msg = ("[%s]set string key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if pipe:
                with drconn.pipeline(transaction=False) as pipe:
                    self.__set_str_value(pipe, str_keyvalue)
                    pipe.execute()
            else:
                self.__set_str_value(self.ropb, str_keyvalue)

        self.ropb.save()
        self.callback_from.show_msg("[%s]save data end~~"%(get_time_stamp()))
        self.callback_from.sync_end()

    def __set_hash_value(self, op, hash_keyvalue):

        if self.s_type == 'server':
            for key in hash_keyvalue.keys():
                op.hmset(key, hash_keyvalue.get(key))
        elif self.s_type == 'rdb':
            for hashkey in hash_keyvalue:
                op.hset(hashkey[0], hashkey[1], hashkey[2])

    def __set_zset_value(self, op, zset_keyvalue):
        if self.s_type == 'server':
            for key in zset_keyvalue.keys():
                op.zadd(key, zset_keyvalue.get(key))
        elif self.s_type == 'rdb':
            for zsetkey in zset_keyvalue:
                mapping = {zsetkey[1]: zsetkey[2]}
                op.zadd(zsetkey[0], mapping)

    def __set_set_value(self, op, zset_keyvalue):
        if self.s_type == 'server':
            for key in zset_keyvalue.keys():
                op.sadd(key, zset_keyvalue.get(key))
        elif self.s_type == 'rdb':
            for setkey in zset_keyvalue:
                op.sadd(setkey[0], [setkey[1], ])

    def __set_list_value(self, op, list_keyvalue):
        if self.s_type == 'server':
            for key in list_keyvalue.keys():
                op.rpush(key, list_keyvalue.get(key))
        elif self.s_type == 'rdb':
            for listkey in list_keyvalue:
                op.rpush(listkey[0], [listkey[1], ])

    def __set_str_value(self, op, str_keyvalue):
        if self.s_type == 'server':
            for key in str_keyvalue.keys():
                op.set(key, str_keyvalue.get(key))
        elif self.s_type == 'rdb':
            for strkey in str_keyvalue:
                op.set(strkey[0], strkey[1])

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