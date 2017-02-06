# coding=utf8
import uuid

from pandas import json

import cx_Oracle
from pymongo import MongoClient


class MongoHolder():
    mongodb = None
    collection = None

    def initMongoDB(self, uri, port, dbname):
        client = MongoClient(uri, port, maxPoolSize=200, connectTimeoutMS=60 * 1000, socketTimeoutMS=60 * 1000)
        self.mongodb = client[dbname]

    def readMongoTable(self, page, limit):
        return self.mongodb[self.collection].find().skip(page * limit).limit(limit)

    def countMongoDB(self):
        return self.mongodb[self.collection].count()

    def findAllCollection(self):
        return self.mongodb


class MySQLHolder():
    mysql = None
    collection = None
    mysql_db = None  # mySQL数据库名称
    text_column = []
    unique_column = []

    def initMySql(self, host, port, user, passwd, dbname):
        self.mysql = cx_Oracle.connect(user, passwd, '%s:%s/%s' % (host, port, dbname))

    # 创建指定名称的表
    def createMySqlTable(self, tableName):
        try:
            base_sql = 'CREATE TABLE %s (instertime timestamp default sysdate)'
            cursor = self.mysql.cursor()
            cursor.execute(base_sql % tableName)
            return True
        except Exception, e:
            if 'ORA-00955' in str(e[0]):
                return True
            return False

    # 创建字段
    def createMySqlFieldToTable(self, tableName, fieldName, fieldType, default='', unique=False):
        tableName = tableName.upper()
        fieldName = fieldName.upper()
        if isinstance(default, str) and len(default) > 0:
            default = 'default \'' + default + '\''

        while True:
            try:
                sql = 'alter table %s add(%s %s %s)' % (tableName, fieldName, fieldType, default)
                cursor = self.mysql.cursor()
                cursor.execute(sql)
            except Exception, e:
                if 'ORA-00942' in str(e):
                    self.createMySqlTable(tableName)
                    continue

                if 'ORA-01430' not in str(e):
                    return False
            if unique:
                uniname = str(uuid.uuid1())
                uniname = uniname[:uniname.find('-')].upper()
                sql = 'alter table %s add constraint UN_%s unique(%s)' % (tableName, uniname, fieldName)
                cursor = self.mysql.cursor()
                try:
                    cursor.execute(sql)
                except Exception, e:
                    if 'ORA-02261' not in str(e):
                        return False
            return True

    def executeSQL(self, sql):
        cursor = self.mysql.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()
        return data

    def executeSQL(self, sql, param):
        param = tuple(param)
        cursor = self.mysql.cursor()
        cursor.execute(sql, param)
        data = cursor.fetchone()
        return data

    def executeInsterSQL(self, tableName, key_arr, pla_arr, val_arr):
        val_arr = tuple(val_arr)
        # 转换为Oracle所需参数串
        pla_arr = pla_arr % tuple([':%s' % x for x in range(1, pla_arr.count('%s') + 1)])
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (tableName, key_arr, pla_arr)
        try:
            cursor = self.mysql.cursor()
            cursor.execute(sql, val_arr)
            pass
        except:
            if not self.hasMySqlTableForDB(tableName):
                self.createMySqlTable(tableName)
            tabKetArr = self.getMySqlFieldNameByTable(tableName)
            key_list = key_arr.split(',')
            for i in range(0, len(key_list)):
                key = key_list[i]
                naked = key
                if naked in self.unique_column:
                    unique = True
                else:
                    unique = False
                if (naked,) not in tabKetArr:
                    if isinstance(val_arr[i], int):
                        self.createMySqlFieldToTable(tableName, naked, 'INT(11)', unique=unique)
                    elif isinstance(val_arr[i], float) or isinstance(val_arr[i], long):
                        self.createMySqlFieldToTable(tableName, naked, 'DOUBLE', unique=unique)
                    elif naked in self.text_column:  # 检查特殊字段(TEXT)
                        self.createMySqlFieldToTable(tableName, naked, 'CLOB', unique=unique)
                    else:
                        self.createMySqlFieldToTable(tableName, naked, 'VARCHAR(512)', unique=unique)
            cursor = self.mysql.cursor()
            try:
                cursor.execute(sql, val_arr)
            except Exception, e:
                if 'ORA-00001' in str(e):
                    return
                if 'ORA-01747' in str(e):
                    print key_arr
                cursor.execute(sql, val_arr)



    def executeInsterManySQL(self, sql_list):
        for sql_arr in sql_list:
            tableName = sql_arr['tableName']
            key_arr = sql_arr['key_arr']
            pla_arr = sql_arr['pla_arr']
            val_arr = sql_arr['val_arr']
            val_arr = tuple(val_arr)
            # 转换为Oracle所需参数串
            pla_arr = pla_arr % tuple([':%s' % x for x in range(1, pla_arr.count('%s') + 1)])
            sql = 'INSERT INTO %s (%s) VALUES(%s)' % (tableName, key_arr, pla_arr)
            try:
                cursor = self.mysql.cursor()
                cursor.execute(sql, val_arr)
                pass
            except:
                if not self.hasMySqlTableForDB(tableName):
                    self.createMySqlTable(tableName)
                tabKetArr = self.getMySqlFieldNameByTable(tableName)
                key_list = key_arr.split(',')
                for i in range(0, len(key_list)):
                    key = key_list[i]
                    naked = key
                    if naked in self.unique_column:
                        unique = True
                    else:
                        unique = False
                    if (naked,) not in tabKetArr:
                        if isinstance(val_arr[i], int):
                            self.createMySqlFieldToTable(tableName, naked, 'INT(11)', unique=unique)
                        elif isinstance(val_arr[i], float) or isinstance(val_arr[i], long):
                            self.createMySqlFieldToTable(tableName, naked, 'DOUBLE', unique=unique)
                        elif naked in self.text_column:  # 检查特殊字段(TEXT)
                            self.createMySqlFieldToTable(tableName, naked, 'CLOB', unique=unique)
                        else:
                            self.createMySqlFieldToTable(tableName, naked, 'VARCHAR(512)', unique=unique)
                cursor = self.mysql.cursor()
                try:
                    cursor.execute(sql, val_arr)
                except Exception, e:
                    if 'ORA-00001' in str(e):
                        return
                    cursor.execute(sql, val_arr)

    def executeInsterSQLOfMultiterm(self, tableName, key_arr, pla_arr, val_arr_list):
        val_arr = val_arr_list[0]
        # 转换为Oracle所需参数串
        pla_arr = pla_arr % tuple([':%s' % x for x in range(1, pla_arr.count('%s') + 1)])
        # for i in range(0, len(val_arr_list)):
        #     val_arr_list[i] = tuple(val_arr_list[i])
        val_arrs = val_arr_list
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (tableName, key_arr, pla_arr)
        try:
            cursor = self.mysql.cursor()
            cursor.executemany(sql, val_arrs)
        except:
            if not self.hasMySqlTableForDB(tableName):
                self.createMySqlTable(tableName)
            tabKetArr = self.getMySqlFieldNameByTable(tableName)
            key_list = key_arr.split(',')
            for i in range(0, len(key_list)):
                key = key_list[i]
                naked = key
                if naked in self.unique_column:
                    unique = True
                else:
                    unique = False
                if naked not in tabKetArr:
                    if isinstance(val_arr[i], int):
                        self.createMySqlFieldToTable(tableName, naked, 'INT(11)', unique=unique)
                    elif isinstance(val_arr[i], float) or isinstance(val_arr[i], long):
                        self.createMySqlFieldToTable(tableName, naked, 'DOUBLE', unique=unique)
                    elif naked.lower() in self.text_column:
                        self.createMySqlFieldToTable(tableName, naked, 'TEXT', unique=unique)
                    else:
                        self.createMySqlFieldToTable(tableName, naked, 'VARCHAR(512)', unique=unique)
            cursor = self.mysql.cursor()
            cursor.prepare(sql)
            cursor.executemany(None, val_arrs)


    def executeInsterManySQLOfMultiterm(self, sql_list):
        for sql_arr in sql_list:
            tableName = sql_arr['tableName']
            key_arr = sql_arr['key_arr']
            pla_arr = sql_arr['pla_arr']
            val_arr_list = sql_arr['val_arr_list']
            val_arr = val_arr_list[0]
            # 转换为Oracle所需参数串
            pla_arr = pla_arr % tuple([':%s' % x for x in range(1, pla_arr.count('%s') + 1)])
            # for i in range(0, len(val_arr_list)):
            #     val_arr_list[i] = tuple(val_arr_list[i])
            val_arrs = val_arr_list
            sql = 'INSERT INTO %s (%s) VALUES(%s)' % (tableName, key_arr, pla_arr)
            try:
                cursor = self.mysql.cursor()
                cursor.executemany(sql, val_arrs)
            except:
                if not self.hasMySqlTableForDB(tableName):
                    self.createMySqlTable(tableName)
                tabKetArr = self.getMySqlFieldNameByTable(tableName)
                key_list = key_arr.split(',')
                for i in range(0, len(key_list)):
                    key = key_list[i]
                    naked = key
                    if naked in self.unique_column:
                        unique = True
                    else:
                        unique = False
                    if naked not in tabKetArr:
                        if isinstance(val_arr[i], int):
                            self.createMySqlFieldToTable(tableName, naked, 'INT(11)', unique=unique)
                        elif isinstance(val_arr[i], float) or isinstance(val_arr[i], long):
                            self.createMySqlFieldToTable(tableName, naked, 'DOUBLE', unique=unique)
                        elif naked in self.text_column:
                            self.createMySqlFieldToTable(tableName, naked, 'TEXT', unique=unique)
                        else:
                            self.createMySqlFieldToTable(tableName, naked, 'VARCHAR(512)', unique=unique)
                cursor = self.mysql.cursor()
                cursor.prepare(sql)
                cursor.executemany(None, val_arrs)

    # 查询表中所有字段名
    def getMySqlFieldNameByTable(self, tableName):
        base_sql = "select column_name from user_tab_cols where table_name='%s'"
        cursor = self.mysql.cursor()
        cursor.execute(base_sql % tableName.upper())  # 表名必须要大写
        data = cursor.fetchall()
        return data

    # 获得所有表名
    def getMySqlTableName(self):
        base_sql = "select table_name from user_tables"
        cursor = self.mysql.cursor()
        cursor.execute(base_sql)
        data = cursor.fetchall()
        return data

    def hasMySqlTableForDB(self, tableName):
        tableName = tableName.upper()
        base_sql = "select COUNT(table_name) from user_tables WHERE TABLE_NAME='%s'"
        cursor = self.mysql.cursor()
        cursor.execute(base_sql % tableName)
        data = cursor.fetchone()
        return data[0] > 0

    def commitSQL(self):
        self.mysql.commit()