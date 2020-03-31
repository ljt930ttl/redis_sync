#!/usr/bin/env python
# _*_coding:utf-8_*_

"""
@Time :   2020/3/31 9:16
@Author:  linjinting
@File: test_pipe_sync_one.py
@Software: PyCharm
"""

import unittest
import test.test_conn__ as conf
import operator_ship
import redis_operator_base

conn_dst = conf.get_conn_dst_test()
conn_src = conf.get_conn_src_test()



ops = operator_ship.ops

class Test_Math(unittest.TestCase):

    def setUp(self):
        print("测试用例执行前的初始化操作========")
        self.rop_src = redis_operator_base.RedisOperatorBase(conn_src)
        self.rop_dst = redis_operator_base.RedisOperatorBase(conn_dst)


    def tearDown(self):
        # print("测试用例执行完之后的收尾操作=====")
        pass

    # 正确的断言
    def test_01(self):
        key = b'test:zset'
        rop = ops.get(b'zset')
        self.assertIsNotNone(rop)
        values_ = rop.get_value(self.rop_src, key)
        print(values_)
        # res = rop.set_value(rop_dst,'server', key, values_)
        # print(res)

    # def test_02(self):
    #     key = b'test:setsetset'
    #     rop = ops.get(b'set')
    #     self.assertIsNotNone(rop)
    #     values_ = rop.get_value(self.rop_src, key)
    #
    #     res = rop.set_value(self.rop_dst,'server', key, values_)
    #     print(res)
    #
    # def test_03(self):
    #     key = b'test:list:list'
    #     rop = ops.get(b'list')
    #     self.assertIsNotNone(rop)
    #     values_ = rop.get_value(self.rop_src, key)
    #
    #     res = rop.set_value(self.rop_dst,'server', key, values_)
    #     print(res)
    # 设置错误的断言
    # def test_subTwoNum_02(self):
    #     pass




if __name__ == '__main__':
    unittest.main()