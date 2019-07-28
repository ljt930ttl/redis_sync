#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/27 16:16
# @Author  : linjinting
# @Site    : 
# @Software: redis_sync
# @File    : redis_connection_base.py
# @Function:

from redis_connection_base import RedisConnectionBase

class RedisConnection(RedisConnectionBase):
    def __init__(self,callback):
        super(RedisConnection,self).__init__()
        self.callback = callback
        pass

    def show_msg(self,msg):
        # print("not base")
        # print(msg)
        self.callback.show_msg(msg)

if __name__ == '__main__':
    rarg_d = {
        "host":"127.0.0.1",
        "port":6379
    }
    RC = RedisConnection()
    RC.get_conn(rarg_d)
