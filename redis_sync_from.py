#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2019/7/27 16:03
# @Author  : linjinting
# @Site    : 
# @Software: redis_sync
# @File    : redis_sync_from.py
# @Function:
import sys
# from TrasitionBase.CharactersConversion import CharactersConversion as charconv
from UI.redis_sync import Ui_Form_redis_sync
from PyQt5 import QtWidgets
from redis_connection import RedisConnection
from redis_sync import RedisSync
from get_rdb import GetRdb

class RedisSyncForm(QtWidgets.QDialog, Ui_Form_redis_sync):
    def __init__(self):
        super(RedisSyncForm, self).__init__()
        self.setupUi(self)
        self.bound_single()

        self.init_widgets()

        self.s_type = "server"
        self.drconn = None
        self.srconn = None
        pass
    def init_widgets(self):
        self.pushButton_sync.setEnabled(False)
        self.pushButton_getdata.setEnabled(False)

        self.lineEdit_rdb_path.setEnabled(False)
        self.pushButton_open.setEnabled(False)


    def bound_single(self):
        self.pushButton_conn.clicked.connect(self.conn_redis)
        self.pushButton_getdata.clicked.connect(self.get_data)
        self.pushButton_sync.clicked.connect(self.sync_data)
        self.checkBox_isrdb.stateChanged.connect(self.isrdb_change)
        self.pushButton_open.clicked.connect(self.openfile_dialog)
    def isrdb_change(self):
        stat = self.checkBox_isrdb.isChecked()
        print(stat)
        if stat :
            self.s_type = "rdb"
            self.lineEdit_rdb_path.setEnabled(True)
            self.pushButton_open.setEnabled(True)
        else:
            self.s_type = "server"
            self.lineEdit_rdb_path.setEnabled(False)
            self.pushButton_open.setEnabled(False)

    def conn_redis(self):
        """
        #先连接目标redis服务器，再跟进情况判断是否要连接源redis服务器
        :return:
        """
        if self.drconn:
            self.pushButton_conn.setText("connection")
            self.pushButton_getdata.setEnabled(False)
            self.drconn.connection_pool.disconnect()
            self.drconn = None
            if self.srconn:
                self.srconn.connection_pool.disconnect()
                self.srconn = None
            return

        ###set destination redis arg
        dhost = self.lineEdit_destination_host.text()
        if not dhost:
            return
        dport = self.lineEdit_destination_port.text()
        if not dport:
            return
        else:
            dport = int(dport)
        dauth = self.lineEdit_destination_auth.text()
        dragr = {
            "host":dhost,
            "port":dport,
            "password":dauth
        }

        rcb = RedisConnection(self)

        ###get destination conn
        self.drconn = rcb.get_conn(dragr)

        if self.drconn :

            # msg = "Connecting to redis was successful:%s\n"%(dragr)
            # self.show_msg(msg)
            if self.s_type == "server":
                self.conn_source_redis(rcb)
            else:
                self.pushButton_conn.setText("disconnection")
                self.pushButton_getdata.setEnabled(True)

    def conn_source_redis(self,rcb):

        if self.srconn:
            self.pushButton_conn.setText("connection")
            self.srconn.connection_pool.disconnect()
            self.srconn = None
            return
        ###set source redis arg
        shost = self.lineEdit_source_host.text()
        if not shost:
            return
        sport = self.lineEdit_source_port.text()
        if not sport:
            return
        else:
            sport = int(sport)
        sauth = self.lineEdit_source_auth.text()
        sragr = {
            "host":shost,
            "port":sport,
            "password":sauth
        }

        ###get source conn
        self.srconn = rcb.get_conn(sragr)

        if self.srconn:
            # msg = "Connecting to redis was successful:%s\n" % (sragr)
            # self.show_msg(msg)
            self.pushButton_conn.setText("disconnection")

            self.pushButton_getdata.setEnabled(True)
        else:
            #如果源redis服务连接不上，则把目标redis服务器的连接也断开
            self.drconn.connection_pool.disconnect()
            self.drconn = None


    def sync_data(self):
        rs = RedisSync(self,self.s_type)
        rs.set_allvalues(self.drconn, self.hash_keyvalue, self.zset_keyvalue, self.set_keyvalue, self.list_keyvalue,
                         self.str_keyvalue)
        self.pushButton_conn.setEnabled(False)
        self.pushButton_getdata.setEnabled(False)
        self.pushButton_sync.setEnabled(False)

    def get_data(self):

        gr = GetRdb(self,self.s_type)
        if self.s_type == "server":
            gr.get_allvalues(self.srconn)
        elif self.s_type == "rdb":
            filename = self.lineEdit_rdb_path.text()
            gr.get_allvalues(filename)
        self.pushButton_getdata.setEnabled(False)

    def get_end(self,hash_keyvalue, zset_keyvalue, set_keyvalue, list_keyvalue, str_keyvalue):
        self.hash_keyvalue = hash_keyvalue
        self.zset_keyvalue = zset_keyvalue
        self.set_keyvalue = set_keyvalue
        self.list_keyvalue = list_keyvalue
        self.str_keyvalue = str_keyvalue

        self.pushButton_sync.setEnabled(True)

    def sync_end(self):
        self.pushButton_conn.setEnabled(True)
        self.pushButton_conn.setText("connection")
        if self.drconn :
            self.drconn.connection_pool.disconnect()
        if self.srconn :
            self.srconn.connection_pool.disconnect()

    def openfile_dialog(self):
        openfile_name, file_type= QtWidgets.QFileDialog.getOpenFileName(self, caption='选择文件', filter="All Files (*);;RDB Files (*.rdb)", initialFilter="RDB Files (*.rdb)")
        print(openfile_name)
        self.lineEdit_rdb_path.setText(openfile_name)

    def show_msg(self,msg):
        self.textEdit.insertPlainText(msg)


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    form = RedisSyncForm()

    form.show()
    sys.exit(app.exec_())