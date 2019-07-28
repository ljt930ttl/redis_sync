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
import time,sys
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
        self.ropb = RedisOperatorBase()
        # self.rcb = RedisConnectionBase()
        self.s_type = s_type
        # self.srarg_d = dict()
        # self.drarg_d = dict()

    def set_allvalues(self, drconn, hash_keyvalue, zset_keyvalue, set_keyvalue, list_keyvalue, str_keyvalue):
        if drconn is None:
            return
        th = threading.Thread(target=self.set_allvalues_concurrency, args=(drconn, hash_keyvalue, zset_keyvalue, set_keyvalue, list_keyvalue, str_keyvalue))
        th.start()


    def set_allvalues_concurrency(self,drconn, hash_keyvalue, zset_keyvalue, set_keyvalue, list_keyvalue, str_keyvalue):

        if hash_keyvalue:
            msg = ("[%s]set hash key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if self.s_type == 'server':
                for key in hash_keyvalue.keys():
                    self.ropb.hmset(drconn, key, hash_keyvalue.get(key))
            elif self.s_type == 'rdb':
                for hashkey in hash_keyvalue:
                    self.ropb.hset(drconn,hashkey[0],hashkey[1],hashkey[2])

        if zset_keyvalue:
            msg = ("[%s]set zset key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if self.s_type == 'server':
                for key in zset_keyvalue.keys():
                    self.ropb.zadd(drconn, key, zset_keyvalue.get(key))
            elif self.s_type == 'rdb':
                for zsetkey in zset_keyvalue:
                    mapping = {zsetkey[1]:zsetkey[2]}
                    self.ropb.zadd(drconn,zsetkey[0],mapping)

        if set_keyvalue:
            msg = ("[%s]set set key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if self.s_type == 'server':
                for key in set_keyvalue.keys():
                    self.ropb.sadd(drconn, key, set_keyvalue.get(key))
            elif self.s_type == 'rdb':
                for setkey in set_keyvalue:
                    self.ropb.sadd(drconn,setkey[0],[setkey[1],])

        if list_keyvalue:
            msg = ("[%s]set list key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if self.s_type == 'server':
                for key in list_keyvalue.keys():
                    self.ropb.rpush(drconn, key, list_keyvalue.get(key))

            elif self.s_type == 'rdb':
                for listkey in list_keyvalue:
                    self.ropb.rpush(drconn,listkey[0],[listkey[1],])

        if str_keyvalue:
            msg = ("[%s]set string key\n")%(get_time_stamp())
            self.callback_from.show_msg(msg)
            if self.s_type == 'server':
                for key in str_keyvalue.keys():
                    self.ropb.set(drconn, key, str_keyvalue.get(key))

            elif self.s_type == 'rdb':
                for strkey in str_keyvalue:
                    self.ropb.set(drconn,strkey[0],strkey[1])

        self.ropb.save(drconn)
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