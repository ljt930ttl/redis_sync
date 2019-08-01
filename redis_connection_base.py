#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/27 16:16
# @Author  : linjinting
# @Site    :
# @Software: redis_sync
# @File    : redis_connection_base.py
# @Function:

import redis


class RedisConnectionBase(object):
    def __init__(self):
        self.m_rconn = None
        pass

    def __check_conn(self, rarg_d, connection_pool):
        """
        check the redis connction
        :return: True or False
        """
        if connection_pool is None:
            try:
                self.m_rconn = redis.StrictRedis(**rarg_d)
                self.m_rconn.ping()
                _msg = "[info]redis client %s:%s is ok!!\n" % (
                    rarg_d['host'], rarg_d['port'])
                self.show_msg(_msg)
                return True
            except Exception as e:
                _msg = "[error]check redis %s:%s : %s\n" % (
                    rarg_d['host'], rarg_d['port'], e)
                self.show_msg(_msg)
                return False
        else:
            try:
                self.m_rconn = redis.StrictRedis(
                    connection_pool=connection_pool)
                self.m_rconn.ping()
                _msg = "[info]Connecting to redis was successful[%s:%s]\n" % (
                    rarg_d['host'], rarg_d['port'])
                self.show_msg(_msg)
                return True
            except Exception as e:
                _msg = "[error]Connecting to redis was faile[%s:%s] : %s\n" % (
                    rarg_d['host'], rarg_d['port'], e)
                self.show_msg(_msg)
                return False

    def get_conn(self, rarg_d, connection_pool=None):
        """
        # Get the redis object
        :param rarg_d: redis arg
        :param connection_pool: redis connection_pool
        :return: m_rconn or None
        """
        if self.__check_conn(rarg_d, connection_pool):
            return self.m_rconn
        else:
            return None

    def show_msg(self, msg):
        """
        # show msg
        :param msg:
        :return:
        """
        print(msg)


if __name__ == '__main__':
    rarg_d = {
        "host": "127.0.0.1",
        "port": 6379
    }
    RC = RedisConnectionBase()
    RC.get_conn(rarg_d)
