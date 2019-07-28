#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/28 12:27
# @Author  : linjinting
# @Site    : 
# @Software: redis_sync
# @File    : rdb-tools.py
# @Function:
from rdbtools import RdbParser, RdbCallback, DebugCallback , MemoryCallback


class MyCallback(RdbCallback):
    ''' Simple example to show how callback works.
        See RdbCallback for all available callback methods.
        See JsonCallback for a concrete example
    '''

    def aux_field(self, key, value):
        print('aux:[%s:%s]' % (key, value))

    def db_size(self, db_size, expires_size):
        print('db_size: %s, expires_size %s' % (db_size, expires_size))

    def set(self, key, value, expiry, info):
        print('%s = %s' % (str(key), str(value)))

    def hset(self, key, field, value):
        print('%s.%s = %s' % (str(key), str(field), str(value)))

    def sadd(self, key, member):
        print('%s has {%s}' % (str(key), str(member)))

    def rpush(self, key, value):
        print('%s has [%s]' % (str(key), str(value)))

    def zadd(self, key, score, member):
        print('%s has {%s : %s}' % (str(key), str(member), str(score)))

callback = MyCallback(None)
parser = RdbParser(callback)
parser.parse('dump.rdb')