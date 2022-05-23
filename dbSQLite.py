#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File : dbSQLite.py
# @Author : Norah C.IV
# @Time : 2022/5/23 11:43
# @Software: PyCharm
import sqlite3


sqlite_path = '/root/sqlite.db'


class DBTool(object):
    def __init__(self):
        """
        初始化函数，创建数据库连接
        """
        self.conn = sqlite3.connect(sqlite_path)
        self.c = self.conn.cursor()

    def executeUpdate(self, sql, ob):
        """
        数据库的插入、修改函数
        :param sql: 传入的SQL语句
        :param ob: 传入数据
        :return: 返回操作数据库状态
        """
        try:
            self.c.executemany(sql, ob)
            i = self.conn.total_changes
        except Exception as e:
            print('错误类型： ', e)
            return False
        finally:
            self.conn.commit()
        if i > 0:
            return True
        else:
            return False

    def executeDelete(self, sql, ob):
        """
        操作数据库数据删除的函数
        :param sql: 传入的SQL语句
        :param ob: 传入数据
        :return: 返回操作数据库状态
        """
        try:
            self.c.execute(sql, ob)
            i = self.conn.total_changes
        except Exception as e:
            return False
        finally:
            self.conn.commit()
        if i > 0:
            return True
        else:
            return False

    def executeQuery(self, sql, ob):
        """
        数据库数据查询
        :param sql: 传入的SQL语句
        :param ob: 传入数据
        :return: 返回操作数据库状态
        """
        test = self.c.execute(sql, ob)
        return test

    def close(self):
        """
        关闭数据库相关连接的函数
        :return:
        """
        self.c.close()
        self.conn.close()


if __name__ == '__main__':
    db = DBTool()

    # 查询语句
    sql1 = 'select * from alive_ip;'
    ob = []
    T = db.executeQuery(sql1, ob)
    for ip in T:
        print(ip)
    db.close()

    # 更新语句
    ip_id1 = '1'
    sql2 = "update alive_ip set ip = '2' where id = ?"
    ob = [ip_id1]
    db.executeUpdate(sql2, ob)
    db.close()

    # 添加语句
    ip = 'xxx.xxx.xxx.xxx'
    ip_id2 = '2'
    sql3 = 'insert into alive_ip values (?, ?)'
    ob = [(ip_id2, ip)]
    db.executeUpdate(sql3, ob)
    db.close()

    # 删除语句
    ip_id2 = '2'
    sql4 = 'delete from alive_ip where id = ?'
    ob = [ip_id2]
    db.executeDelete(sql4, ob)
    db.close()
