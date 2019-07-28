#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/27 16:44
# @Author  : linjinting
# @Site    : 
# @Software: redis_sync
# @File    : redis_operator_base.py
# @Function:

class RedisOperatorBase(object):
    def __init__(self):

        pass
    def get_type(self,_rconn, key):
        return _rconn.type(key)
    def scan(self, _rconn, curs=0):
        """

        :param _rconn: redis conn object
        :return: keys (redis keys)
        """
        keys = list()
        # curs = 0
        while True:
            curs, values = _rconn.scan(cursor=curs, count=10000)
            keys = keys + values
            if curs == 0:
                break
        return keys
    def hscan(self, _rconn, key, curs=0, count=None):
        fieldvalue_d = dict()

        while True:
            curs, _fieldvalue_d = _rconn.hscan(key, cursor=curs, count=count)
            fieldvalue_d.update(_fieldvalue_d)
            if curs == 0:
                break

        return fieldvalue_d
    def zscan(self, _rconn, key, curs=0, count=None):
        fieldvalue_d = dict()

        while True:
            curs, _fieldvalue_d = _rconn.zscan(key, cursor=curs, count=count)
            fieldvalue_d.update(_fieldvalue_d)
            if curs == 0:
                break

        return fieldvalue_d

    def zrange(self, _rconn, key,limit=10, desc=False, withscores=False, score_cast_func=float):
        """
        ##暂不使用
        :param _rconn:
        :param key:
        :param limit:
        :param desc:
        :param withscores:
        :param score_cast_func:
        :return:
        """
        value_l = list()
        start = 0
        num = _rconn.zcard(key)-1 ###999

        while True:
            if num > limit:
                end = start+limit
            else:
                end = -1

            _value = _rconn.zrange(key ,start, end, desc=desc, withscores=withscores, score_cast_func=score_cast_func)
            value_l = value_l + _value

            start = end + 1
            num -= limit + 1
            if num < 0:
                break

        return value_l

    def sscan(self, _rconn, key, curs=0, count=None):
        value_l = list()

        while True:
            curs, _value = _rconn.sscan(key, cursor=curs, count=count)
            value_l = value_l + _value
            if curs == 0:
                break

        return value_l

    def lrange(self, _rconn, key,limit=100):
        value_l = list()
        start = 0

        residue = _rconn.llen(key)-1 ###剩余数

        while True:
            if residue > limit:
                end = start+limit
            else:
                end = -1

            _value = _rconn.lrange(key ,start, end)
            value_l = value_l + _value

            start = end + 1
            residue -= limit +1
            print("residue",residue)
            if residue < 0:
                break

        return value_l

    def get(self, _rconn, key):
        value =  _rconn.get(key)
        return value

    def hmset(self, _rconn, key, mapping):
        return  _rconn.hmset(key, mapping)

    def hset(self, _rconn, key, field, value):
        return  _rconn.hset(key, field, value)

    def zadd(self, _rconn, key, mapping):
        return  _rconn.zadd(key, mapping)



    def sadd(self, _rconn, key, value):
        return  _rconn.sadd(key, *value)

    def rpush(self, _rconn, key, value):
        return  _rconn.rpush(key, *value)

    def set(self, _rconn, key, value):
        return  _rconn.set(key, value)

    def save(self, _rconn):
        return _rconn.save()