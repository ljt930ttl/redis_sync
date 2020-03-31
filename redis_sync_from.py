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
from UI.UI_redis_sync import Ui_Form_redis_sync
from PyQt5 import QtWidgets
from PyQt5.QtGui import QTextCursor
from redis_connection import RedisConnection
from redis_sync import RedisSync


# from get_rdb import GetRdb


class RedisSyncForm(QtWidgets.QDialog, Ui_Form_redis_sync):
    def __init__(self):
        super(RedisSyncForm, self).__init__()
        self.setupUi(self)
        self.bound_single()

        self.init_widgets()

        self.s_type = "server"
        self.isPipe = True
        self.rconn_dst = None
        self.rconn_src = None
        self._keyvalue = None

    def init_widgets(self):
        self.pushButton_sync.setEnabled(False)
        self.lineEdit_rdb_path.setEnabled(False)
        self.pushButton_open.setEnabled(False)
        self.checkBox_isPipe.setChecked(True)
        self.checkBox_isrdb.setEnabled(False)

    def bound_single(self):
        self.pushButton_conn.clicked.connect(self.conn_redis_dst)
        self.pushButton_sync.clicked.connect(self.sync_data)
        self.checkBox_isrdb.stateChanged.connect(self.isrdb_change)
        self.checkBox_isPipe.stateChanged.connect(self.isPipe_change)
        self.pushButton_open.clicked.connect(self.openfile_dialog)

    def isrdb_change(self):
        stat = self.checkBox_isrdb.isChecked()
        print(stat)
        if stat:
            self.s_type = "rdb"
            self.lineEdit_rdb_path.setEnabled(True)
            self.pushButton_open.setEnabled(True)
        else:
            self.s_type = "server"
            self.lineEdit_rdb_path.setEnabled(False)
            self.pushButton_open.setEnabled(False)

    def isPipe_change(self):
        stat = self.checkBox_isPipe.isChecked()
        print(stat)
        if stat:
            self.isPipe = True
        else:
            self.isPipe = False

    def conn_redis_dst(self):
        """
        #先连接目标redis服务器，再跟进情况判断是否要连接源redis服务器
        :return:
        """
        if self.rconn_dst:
            self.pushButton_conn.setText("connection")
            self.rconn_dst.connection_pool.disconnect()
            self.rconn_dst = None
            if self.rconn_src:
                self.rconn_src.connection_pool.disconnect()
                self.rconn_src = None
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
        drag = {
            "host": dhost,
            "port": dport,
            "password": dauth
        }

        rcb = RedisConnection(self)

        ###get destination conn
        self.rconn_dst = rcb.get_conn(drag)

        if self.rconn_dst:

            if self.s_type == "server":
                self.conn_redis_src(rcb)
            else:
                self.pushButton_conn.setText("disconnection")
                self.pushButton_sync.setEnabled(True)

    def conn_redis_src(self, rcb):
        if self.rconn_src:
            self.pushButton_conn.setText("connection")
            self.rconn_src.connection_pool.disconnect()
            self.rconn_src = None
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
        srag = {
            "host": shost,
            "port": sport,
            "password": sauth
        }

        ###get source conn
        self.rconn_src = rcb.get_conn(srag)

        if self.rconn_src:
            # msg = "Connecting to redis was successful:%s\n" % (sragr)
            # self.show_msg(msg)
            self.pushButton_conn.setText("disconnection")

            self.pushButton_sync.setEnabled(True)
        else:
            # 如果源redis服务连接不上，则把目标redis服务器的连接也断开
            self.rconn_dst.connection_pool.disconnect()
            self.rconn_dst = None

    def sync_data(self):
        sync = RedisSync(self, self.s_type, isPipe=self.isPipe)
        self.pushButton_conn.setEnabled(False)
        self.pushButton_sync.setEnabled(False)
        # if self._keyvalue is  None:
        #     self.sync_end()
        #     return
        sync.init_operators(self.rconn_src, self.rconn_dst)
        sync.run()

    def sync_end(self):

        self.pushButton_conn.setEnabled(True)
        self.pushButton_conn.setText("connection")
        if self.rconn_dst:
            self.rconn_dst.connection_pool.disconnect()
            self.rconn_dst = None
        if self.rconn_src:
            self.rconn_src.connection_pool.disconnect()
            self.rconn_src = None

    def openfile_dialog(self):
        openfile_name, file_type = QtWidgets.QFileDialog.getOpenFileName(self, caption='选择文件',
                                                                         filter="All Files (*);;RDB Files (*.rdb)",
                                                                         initialFilter="RDB Files (*.rdb)")
        print(openfile_name)
        self.lineEdit_rdb_path.setText(openfile_name)

    def show_msg(self, msg):
        self.textEdit.insertPlainText(msg)
        self.textEdit.moveCursor(QTextCursor.End)


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    form = RedisSyncForm()

    form.show()
    sys.exit(app.exec_())
