#!/usr/bin/env python
# _*_coding:utf-8_*_

"""
@Time :   2020/3/31 18:13
@Author:  linjinting
@File: time_.py
@Software: PyCharm
"""
import time
def get_time_stamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%H:%M:%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s.%03d" % (data_head, data_secs)
    return time_stamp
