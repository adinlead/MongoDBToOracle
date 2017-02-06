# encoding: UTF-8
import cx_Oracle
from DBUtils import *

osql = MySQLHolder()
osql.initMySql(host='192.168.1.162',port='49161',user='hengshi',passwd='111111',dbname='xe')
print osql.createMySqlFieldToTable('test','new_field_1','VARCHAR(1024)','hello',unique=True)