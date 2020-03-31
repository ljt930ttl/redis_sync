#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/12/8 13:49
# @Author  : linjinting
# @Site    : 
# @Software: redis_sync
# @File    : test.py
# @Function:


ll2 = [1,2,3,4]
dict1 = {"a":"1","b":"2","c":"3"}
list1 = ["qq","ww","ee","rr"]
ll3 = ["xx","zz","cc","vv"]
tt1 = list(zip(ll2,list1))
tt2 = list(zip(ll2,list1,ll3))
aa ={"a":"v"}

def tt():

    for i ,v ,t in tt2:

        if t == "xx":
            print("xx:",v)
        if t == "zz":
            print("zz:",v)
        if t == "cc":
            print("cc:",v)
        if t == "vv":
            print("vv:",v)

if __name__ == '__main__':
    tt()