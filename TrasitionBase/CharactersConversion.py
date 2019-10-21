#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/4/14 12:57
# @Author  : Aries
# @Site    :
# @File    : CharactersConversion.py
# @Software: PyCharm
import struct


class CharactersConversion():
    # def QString2PyString(self, qStr):
    #     # # QString，如果内容是中文，则直接使用会有问题，要转换成 python string
    #     # print type(qStr)
    #     return unicode(qStr.toUtf8(), 'utf-8', 'ignore')

    def QStringToInt(self, qStr):
        i_tmp = qStr.toInt()
        # example :tupe (int ,bool)
        if i_tmp[1]:
            i = i_tmp[0]
        else:
            i = 0
        return i

    def QStringToFloat(self, qStr):
        i_tmp = qStr.toInt()
        # example :tupe (int ,bool)
        if i_tmp[1]:
            i = i_tmp[0]
        else:
            i = 0

        return float(i)

    def StrToBool(self, str):  # 字符转bool
        if str.lower() == "true":
            return True
        elif str == "1":
            return True
        else:
            return False

    def BoolToStr(self, bool):
        if bool:
            return "1"
        else:
            return "0"

    @staticmethod
    def encode_to_hex(data):
        """
        功能：把unicode编码字符字符编码成“hex”编码字符串,每两位用空格分割.
        配合：encode_to_hex_yield使用，
        :param s:
        :return:
        """
        # return ' '.join([(hex(ord(c)).replace('0x', '')) for c in data])
        return " ".join(CharactersConversion.encode_to_hex_yield(data))

    @staticmethod
    def encode_to_hex_yield(data):
        """
        功能：每两位为一组，补足两位补0
        :param data:
        :return:
        """
        for c in data:
            s = hex(ord(c)).replace('0x', '')
            if len(s) < 2:
                s = "0" + s
            yield s

    @staticmethod
    def decode_to_hex(data):
        """
        功能：把“hex”的编码类型，解码成unicode编码字符串字符串，每两位为一组，不足两位用0补足
        :param data:
        :return: str
        """
        # 功能有限，暂时屏蔽
        # return ''.join([chr(i) for i in [int(b, 16) for b in s.split(' ')]])

        str = ""
        for d in data.split(" "):
            while d:
                if len(d) < 2:
                    str_t = "0" + d
                else:
                    str_t = d[0:2]
                s = int(str_t, 16)
                str += struct.pack('B', s)
                d = d[2:]
        return str

    def str_to_bin(self, s):
        """
        暂未仔细测试
        :param s:
        :return:
        """
        return ' '.join([bin(ord(c)).replace('0b', '') for c in s])

    def bin_to_str(self, s):
        """
        暂未仔细测试
        :param s:
        :return:
        """
        return ''.join([chr(i) for i in [int(b, 2) for b in s.split(' ')]])

    @staticmethod
    def strip(type, address):
        """
        强制转换socket地址为字符串。格式为
        :param type: 类型
        :param address: ip地址
        :return: "[TCP/UDP]IP:PORT"
        """
        return type + address[0] + ":" + str(address[1])


if __name__ == '__main__':
    ddd = "02020202"

