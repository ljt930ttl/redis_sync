#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/28 13:45
# @Author  : linjinting
# @Site    :
# @Software: redis_sync
# @File    : get_rdb.py
# @Function:
from rdbtools import RdbParser, RdbCallback
from redis_operator_base import RedisOperatorBase
import threading
import time

def get_time_stamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%H:%M:%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s.%03d" % (data_head, data_secs)
    return time_stamp

class GetRdb(object):
    def __init__(self, callback_from, s_type):
        self.s_type = s_type
        self.callback_from = callback_from
        self.init_value()

        self.keys = list()

    def init_value(self):
        if self.s_type == 'server':
            self.hash_keyvalue_d = dict()
            self.zset_keyvalue_d = dict()
            self.set_keyvalue_d = dict()
            self.list_keyvalue_d = dict()
            self.str_keyvalue_d = dict()
        else:
            self.hash_keyvalue_l = list()
            self.zset_keyvalue_l = list()
            self.set_keyvalue_l = list()
            self.list_keyvalue_l = list()
            self.str_keyvalue_l = list()


    def get_keys(self,srconn):
        if srconn is None:
            return
        return self.ropb.scan(srconn)
        # print(keys[0])

    def get_allvalues(self,sours):

        if sours is None:
            if self.s_type == 'rdb':
                self.callback_from.show_msg("rdb is empty\n")
            return
        if self.s_type == 'server':
            self.ropb = RedisOperatorBase()
            m_keys = self.get_keys(sours)

            if m_keys:
                th = threading.Thread(target=self.server_allvalues_concurrency,args=(sours,m_keys))
                th.start()
        else:
            th = threading.Thread(target=self.rdb_allvalues_concurrency, args=(sours,))
            th.start()


        # print(self.keyvalue['ticketAllKey:running:00000002'])
        # print("end")

    def rdb_allvalues_concurrency(self, filename):

        callback = ParserRDBCallback()
        parser = RdbParser(callback)
        self.callback_from.show_msg("[%s]start parse the rdb\n" % (get_time_stamp()))
        parser.parse(filename)
        head_info, hash_keyvalue_l, zset_keyvalue_l, set_keyvalue_l, list_keyvalue_l, str_keyvalue_l = callback.get_values()
        self.callback_from.show_msg(str(head_info))
        self.callback_from.show_msg("[%s]end parse \n" % (get_time_stamp()))

        self.callback_from.get_end(hash_keyvalue_l, zset_keyvalue_l, set_keyvalue_l, list_keyvalue_l, str_keyvalue_l)
    def server_allvalues_concurrency(self,srconn,m_keys):

        self.callback_from.show_msg("[%s]satrt get value\n" %(get_time_stamp()))
        for key in m_keys:
            # msg = ("get key:%s")%(key)
            # self.callback_from.show_msg(msg)
            keetype = self.ropb.get_type(srconn, key)
            if keetype == b"hash":
                vlaue_d = self.ropb.hscan(srconn, key)
                self.hash_keyvalue_d[key] = vlaue_d

            elif keetype == b"zset":
                vlaue_l = self.ropb.zscan(srconn, key)
                self.zset_keyvalue_d[key] = vlaue_l

            elif keetype == b"set":
                vlaue_l = self.ropb.sscan(srconn, key)
                self.set_keyvalue_d[key] = vlaue_l

            elif keetype == b"list":
                vlaue_l = self.ropb.lrange(srconn, key)
                self.list_keyvalue_d[key] = vlaue_l

            elif keetype == b"string":
                vlaue = self.ropb.get(srconn, key)
                self.str_keyvalue_d[key] = vlaue
            else:
                print("key type is None")
        self.callback_from.show_msg("[%s]get value end\n"%(get_time_stamp()))
        self.callback_from.get_end(self.hash_keyvalue_d, self.zset_keyvalue_d, self.set_keyvalue_d, self.list_keyvalue_d, self.str_keyvalue_d)

class ParserRDBCallback(RdbCallback):
    ''' Simple example to show how callback works.
        See RdbCallback for all available callback methods.
        See JsonCallback for a concrete example
    '''

    def __init__(self):
        super(RdbCallback,self).__init__()
        self.head_info = list()
        self.hash_keyvalue_l = list()
        self.zset_keyvalue_l = list()
        self.set_keyvalue_l = list()
        self.list_keyvalue_l = list()
        self.str_keyvalue_l = list()

    def aux_field(self, key, value):
        # print('aux:[%s:%s]' % (key, value))
        self.head_info.append([key, value])
    def db_size(self, db_size, expires_size):
        # print('db_size: %s, expires_size %s' % (db_size, expires_size))
        self.head_info.append([b"db_size", db_size])
        self.head_info.append([b"expires_size", expires_size])
    def set(self, key, value, expiry, info):
        # print('%s = %s' % (str(key), str(value)))
        self.str_keyvalue_l.append([key, value])

    def hset(self, key, field, value):
        # print('%s.%s = %s' % (str(key), str(field), str(value)))
        self.hash_keyvalue_l.append([key, field, value])

    def sadd(self, key, member):
        # print('%s has {%s}' % (str(key), str(member)))
        self.set_keyvalue_l.append([key, member])

    def rpush(self, key, value):
        # print('%s has [%s]' % (str(key), str(value)))
        self.list_keyvalue_l.append([key, value])

    def zadd(self, key, score, member):
        # print('%s has {%s : %s}' % (str(key), str(member), str(score)))
        self.zset_keyvalue_l.append([key, member, score])
    def get_values(self):
        return self.head_info, self.hash_keyvalue_l, self.zset_keyvalue_l, self.set_keyvalue_l, self.list_keyvalue_l, self.str_keyvalue_l

if __name__ == '__main__':
    callback = ParserRDBCallback()
    parser = RdbParser(callback)
    parser.parse('dump.rdb')
    head_info,hash_keyvalue_l, zset_keyvalue_l, set_keyvalue_l, list_keyvalue_l, str_keyvalue_l =callback.get_values()
    print(head_info)