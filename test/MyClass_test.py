#!/usr/bin/env python
# _*_coding:utf-8_*_

"""
@Time :   2020/3/31 8:54
@Author:  linjinting
@File: MyClass_test.py
@Software: PyCharm
"""
import unittest
import test.test_conn__ as conf
import operator_ship


conn_dst = conf.get_conn_dst_test()
conn_src = conf.get_conn_src_test()

pipe_src = conn_src.pipeline(transaction=False)
pipe_dst = conn_dst.pipeline(transaction=False)

ops = operator_ship.ops

class Test_Math(unittest.TestCase):

    def setUp(self):
        print("测试用例执行前的初始化操作========")
        self.pipe_src = pipe_src
        self.pipe_dst = pipe_dst


    def tearDown(self):
        # print("测试用例执行完之后的收尾操作=====")
        pass

    # 正确的断言
    def test_01(self):
        key = b'test:zset'
        rop = ops.get(b'zset')
        self.assertIsNotNone(rop)
        with self.pipe_src as pipe:
            result = rop.get_value(pipe, key)
            values_ = pipe.execute()

        with self.pipe_dst as pipe:
            for value in values_:
                rop.set_value(pipe,'server', key, value)
            res = pipe.execute()
        self.assertIn(0,res)

    def test_02(self):
        key = b'test:setsetset'
        rop = ops.get(b'set')
        self.assertIsNotNone(rop)
        with self.pipe_src as pipe:
            result = rop.get_value(pipe, key)
            values_ = pipe.execute()
        with self.pipe_dst as pipe:
            for value in values_:
                rop.set_value(pipe,'server', key, value)
            pipe.execute()

    def test_03(self):
        key = b'test:list:list'
        rop = ops.get(b'list')
        self.assertIsNotNone(rop)
        with self.pipe_src as pipe:
            result = rop.get_value(pipe, key)
            values_ = pipe.execute()
        with self.pipe_dst as pipe:
            for value in values_:
                rop.set_value(pipe,'server', key, value)
            pipe.execute()
    # 设置错误的断言
    # def test_subTwoNum_02(self):
    #     pass




if __name__ == '__main__':
    unittest.main()