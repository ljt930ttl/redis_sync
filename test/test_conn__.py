#!/usr/bin/env python
# _*_coding:utf-8_*_

"""
@Time :   2020/3/31 9:22
@Author:  linjinting
@File: test_conn__.py
@Software: PyCharm
"""

import redis_connection_base

redis_arg_src = {
    "host": "127.0.0.1",
    "port": 6379
}
redis_arg_dst = {
    "host": "10.7.3.53",
    "port": 16379
}
RC = redis_connection_base.RedisConnectionBase()

def get_conn_src_test():

    conn_src = RC.get_conn(redis_arg_src)
    return conn_src

def get_conn_dst_test():
    conn_dst = RC.get_conn(redis_arg_dst)
    return conn_dst