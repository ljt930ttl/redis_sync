#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/28 13:45
# @Author  : linjinting
# @Site    :
# @Software: redis_sync
# @File    : get_rdb.py
# @Function:
# from rdbtools import RdbParser, RdbCallback
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

class StringReader(object):
    @staticmethod
    def send_command(p, key):
        p.get(key)

    @staticmethod
    def handle_response(response, pretty, encoding):
        # if key does not exist, get will return None;
        # however, our type check requires that the key exists
        return response.decode(encoding)

class ListReader(object):
    @staticmethod
    def send_command(p, key):
        p.lrange(key, 0, -1)

    @staticmethod
    def handle_response(response, pretty, encoding):
        return [v.decode(encoding) for v in response]

class SetReader(object):
    @staticmethod
    def send_command(p, key):
        p.smembers(key)

    @staticmethod
    def handle_response(response, pretty, encoding):
        value = [v.decode(encoding) for v in response]
        if pretty:
            value.sort()
        return value

class ZsetReader(object):
    @staticmethod
    def send_command(p, key):
        p.zrange(key, 0, -1, False, True)

    @staticmethod
    def handle_response(response, pretty, encoding):
        return [(k.decode(encoding), score) for k, score in response]

class HashReader(object):
    @staticmethod
    def send_command(p, key):
        p.hgetall(key)

    @staticmethod
    def handle_response(response, pretty, encoding):
        value = {}
        for k in response:
            value[k.decode(encoding)] = response[k].decode(encoding)
        return value

ops = {
    b'string': StringReader,
    b'list': ListReader,
    b'set': SetReader,
    b'zset': ZsetReader,
    b'hash': HashReader,
}


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


    def get_keys(self):

        return self.ropb.scan()
        # print(keys[0])

    def get_allvalues(self,sours):

        if sours is None:
            if self.s_type == 'rdb':
                self.callback_from.show_msg("rdb is empty\n")
            return
        if self.s_type == 'server':
            self.ropb = RedisOperatorBase(sours)
            # self.server_allvalues_concurrency(sours)
            th = threading.Thread(target=self.server_allvalues_concurrency,args=(sours,))
            th.start()
        else:
            pass
            # th = threading.Thread(target=self.rdb_allvalues_concurrency, args=(sours,))
            # th.start()


        # print(self.keyvalue['ticketAllKey:running:00000002'])
        # print("end")

    # def rdb_allvalues_concurrency(self, filename):
    #
    #     callback = ParserRDBCallback()
    #     parser = RdbParser(callback)
    #     self.callback_from.show_msg("[%s]parse the rdb start\n" % (get_time_stamp()))
    #     parser.parse(filename)
    #     head_info_l, hash_keyvalue_l, zset_keyvalue_l, set_keyvalue_l, list_keyvalue_l, str_keyvalue_l = callback.get_values()
    #     head_info = "\n".join( str(head) for head in head_info_l)
    #
    #     self.callback_from.show_msg(head_info+"\n")
    #     self.callback_from.show_msg("[%s]end parse \n" % (get_time_stamp()))
    #
    #     self.callback_from.get_end(hash_keyvalue_l, zset_keyvalue_l, set_keyvalue_l, list_keyvalue_l, str_keyvalue_l)

    def server_allvalues_concurrency(self,sours):
        self.callback_from.show_msg("[%s]satrt get keys\n" % (get_time_stamp()))
        _keys = self.get_keys()
        if not _keys:
            return

        # if pipe:
        self.callback_from.show_msg("[%s]satrt get key type\n" % (get_time_stamp()))
        with sours.pipeline(transaction=False) as pipe:
            for key in _keys:
                pipe.type(key)
            print("pipe end")
            _types = pipe.execute()
        keys_types = zip(_keys,_types)
        # print(d_keys)
        self.callback_from.show_msg("[%s]satrt get value\n" % (get_time_stamp()))
        with sours.pipeline(transaction=False) as pipe:
            for key,type in keys_types:
                rop = ops.get(type)
                if rop is None:
                    print(key,type)
                    continue
                rop.send_command(pipe, key)
            print("pipe end")
            _values = pipe.execute()
        _keys_types_values = zip(_keys, _types, _values)

        self.callback_from.show_msg("[%s]get data end\n"%(get_time_stamp()))
        self.callback_from.get_end(_keys_types_values)

# class ParserRDBCallback(RdbCallback):
#     ''' Simple example to show how callback works.
#         See RdbCallback for all available callback methods.
#         See JsonCallback for a concrete example
#     '''
#
#     def __init__(self):
#         super(RdbCallback,self).__init__()
#         self.head_info = list()
#         self.hash_keyvalue_l = list()
#         self.zset_keyvalue_l = list()
#         self.set_keyvalue_l = list()
#         self.list_keyvalue_l = list()
#         self.str_keyvalue_l = list()
#
#     def aux_field(self, key, value):
#         # print('aux:[%s:%s]' % (key, value))
#         self.head_info.append([key, value])
#     def db_size(self, db_size, expires_size):
#         # print('db_size: %s, expires_size %s' % (db_size, expires_size))
#         self.head_info.append([b"db_size", db_size])
#         self.head_info.append([b"expires_size", expires_size])
#     def set(self, key, value, expiry, info):
#         # print('%s = %s' % (str(key), str(value)))
#         self.str_keyvalue_l.append([key, value])
#
#     def hset(self, key, field, value):
#         # print('%s.%s = %s' % (str(key), str(field), str(value)))
#         self.hash_keyvalue_l.append([key, field, value])
#
#     def sadd(self, key, member):
#         # print('%s has {%s}' % (str(key), str(member)))
#         self.set_keyvalue_l.append([key, member])
#
#     def rpush(self, key, value):
#         # print('%s has [%s]' % (str(key), str(value)))
#         self.list_keyvalue_l.append([key, value])
#
#     def zadd(self, key, score, member):
#         # print('%s has {%s : %s}' % (str(key), str(member), str(score)))
#         self.zset_keyvalue_l.append([key, member, score])
#     def get_values(self):
#         return self.head_info, self.hash_keyvalue_l, self.zset_keyvalue_l, self.set_keyvalue_l, self.list_keyvalue_l, self.str_keyvalue_l

# if __name__ == '__main__':
#     callback = ParserRDBCallback()
#     # parser = RdbParser(callback)
#     print("[%s]start"%(get_time_stamp()))
#     parser.parse('dump/dump.rdb')
#     head_info,hash_keyvalue_l, zset_keyvalue_l, set_keyvalue_l, list_keyvalue_l, str_keyvalue_l =callback.get_values()
#     print(head_info)
#     print("[%s]end" % (get_time_stamp()))