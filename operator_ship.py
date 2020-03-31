#!/usr/bin/env python
# _*_coding:utf-8_*_

"""
@Time :   2020/3/30 14:43
@Author:  linjinting
@File: operator_ship.py
@Software: PyCharm
"""

class StringOperator(object):
    @staticmethod
    def set_value(op, s_type, key, value):
        if s_type == 'server':
            return op.set(key, value)
        elif s_type == 'rdb':
            return op.set(key, value)
    @staticmethod
    def get_value(p, key):
       return p.get(key)

    @staticmethod
    def handle_response(response, pretty, encoding):
        # if key does not exist, get will return None;
        # however, our type check requires that the key exists
        return response.decode(encoding)

class ListOperator(object):
    @staticmethod
    def set_value(op, s_type, key, value):
        if s_type == 'server':
            return op.rpush(key, *value)
        elif s_type == 'rdb':
            return op.rpush(key, [value, ])


    @staticmethod
    def get_value(p, key):
        # p.lrange(key, 0, -1)
        return p.lrange(key,0,-1)

    @staticmethod
    def handle_response(response, pretty, encoding):
        return [v.decode(encoding) for v in response]

class SetOperator(object):
    @staticmethod
    def set_value(op, s_type, key, value):
        if s_type == 'server':
            return op.sadd(key, *value)
        elif s_type == 'rdb':
            return op.sadd(key, [value, ])

    @staticmethod
    def get_value(p, key):
        return p.smembers(key)

    @staticmethod
    def handle_response(response, pretty, encoding):
        value = [v.decode(encoding) for v in response]
        if pretty:
            value.sort()
        return value

class ZsetOperator(object):
    @staticmethod
    def set_value(op, s_type, key, value):
        if s_type == 'server':
            value_d = dict((member, source) for member, source in value)
            return op.zadd(key, value_d)
        elif s_type == 'rdb':
            return op.zadd(key, value)

    @staticmethod
    def get_value(p, key):
        # return p.zscan(key)
        return p.zrange(key, 0, -1, False, True)

    @staticmethod
    def handle_response(response, pretty, encoding):
        return [(k.decode(encoding), score) for k, score in response]

class HashOperator(object):
    @staticmethod
    def set_value(op, s_type, key, value):
        if s_type == 'server':
            return op.hmset(key, value)
        elif s_type == 'rdb':
            for field in value.keys():
                return op.hset(key, field, value.get(field))

    @staticmethod
    def get_value(p, key):
        return p.hgetall(key)

    @staticmethod
    def handle_response(response, pretty, encoding):
        value = {}
        for k in response:
            value[k.decode(encoding)] = response[k].decode(encoding)
        return value

ops = {
    b'string': StringOperator,
    b'list': ListOperator,
    b'set': SetOperator,
    b'zset': ZsetOperator,
    b'hash': HashOperator,
}

if __name__ == '__main__':
    pass
