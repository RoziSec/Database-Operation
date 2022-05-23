#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File : dbMySQL.py
# @Author : Norah C.IV
# @Time : 2022/5/23 9:57
# @Software: PyCharm
import pymysql


class MysqlDB(object):

    def __init__(self):
        self.host = 'localhost'
        self.port = 3306
        self.db = 'mysql'
        self.user = 'root'
        self.passwd = '123456'
        self.charset = 'utf8'
        self.conn = None
        self.cur = None
        self.Error = None
        self.connection()

    # 创建一个连接
    def connection(self):
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                                        charset=self.charset)
            self.cur = self.conn.cursor()
        except pymysql.Error as e:
            self.Error = str(e.args[0]) + str(e.args[1])
            raise

    # 关闭连接
    def closeConn(self):
        self.cur.close()
        self.conn.close()

    # 查询一条数据
    def getOneData(self, sql):
        try:
            self.cur.execute(sql)
            return self.cur.fetchone()
        except pymysql.Error as e:
            self.Error = str(e.args[0]) + str(e.args[1])
            raise

    # 查询多条数据
    def getAllData(self, sql):
        try:
            self.cur.execute(sql)
            return self.cur.fetchall()
        except pymysql.Error as e:
            self.Error = str(e.args[0]) + str(e.args[1])
            raise

    # 添加/删除/修改
    def crud(self, sql):
        try:
            n = self.cur.execute(sql)
            self.conn.commit()
            return n
        except pymysql.Error as e:
            self.conn.rollback()
            self.Error = str(e.args[0]) + str(e.args[1])
            raise

    # 批量插入
    def execMany(self, sql, vals):
        try:
            n = self.cur.executemany(sql, vals)
            self.conn.commit()
            return n
        except pymysql.Error as e:
            self.conn.rollback()
            self.Error = str(e.args[0]) + str(e.args[1])
            raise


if __name__ == '__main__':
    mydb = MysqlDB()

    # 查询
    sql = "select * from test_user"
    res = mydb.getAllData(sql)
    print('共查询到' + str(len(res)) + '条数据')
    for item in res:
        print('name: %s, age: %s' % (item[0], item[1]))

    # 删除
    sql = "delete from test_user"
    res = mydb.crud(sql)
    print('共删除' + str(res) + '条数据')

    # 更新
    sql = "update test_user set age = '111' where name = '66666'"
    res = mydb.crud(sql)
    print('共更新' + str(res) + '条数据')

    # 添加
    try:
        sql = "insert into test_user (name, age) values ('%s', '%s')" % ('6666', '1111')
        res = mydb.crud(sql)
        print('共添加' + str(res) + '条数据')
    except pymysql.err.IntegrityError:
        print('主键重复')
    finally:
        mydb.closeConn()
