# coding=utf8
import sys
import ConfigParser

import math

from tqdm import tqdm

from Tools import *
from DBUtils import *

import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

sql_keywords = []

def disposeData(data, conn, doList=False):
    key_arr = 'uuid'
    pla_arr = '%s'
    val_arr = [str(data['_id']).upper()]
    sql_list = []
    sql_list_many = []

    for key in data.keys():
        son = data[key]
        column = key
        if column in sql_keywords:
            column = key + "_"
        if '_id' == key:
            continue
        if 'show' == key:
            continue
        if isinstance(son, dict):
            ret = ergodic.ergodicDict(data[key], prefix='%s_' % key)
            key_arr += ret['key']
            pla_arr += ret['pla']
            val_arr.extend(ret['val'])
            list_obj = ret['list']
            for son_key in list_obj.keys():
                # intermediate = '%s_%s' % (key, son_key)
                # intermediate_key_arr = '%s_id,%s_id' % (key, son_key)
                # intermediate_pla_arr = '%s,%s'
                # intermediate_list = []
                subList = list_obj[son_key]
                if doList:
                    for sub in subList:
                        href = sub['href']
                        uuid_str = str(uuid.uuid1()).upper().replace('-','')
                        id = href[href.rfind('/') + 1:]
                        ret = ergodic.ergodicDict(sub, son_key + "_")
                        son_key_arr = ret['key']
                        son_key_arr = 'movie_id,id' + son_key_arr
                        son_pla_arr = ret['pla']
                        son_pla_arr = '%s,%s' + son_pla_arr
                        son_val_arr = ret['val']
                        son_val_arr.insert(0, id)
                        son_val_arr.insert(0, str(data['_id']).upper())
                        # intermediate_list.append([str(data['_id']).upper(), uuid_str])
                        sql_body = {}
                        sql_body['tableName'] = '%s_%s' % (mongodb_collection,son_key)
                        sql_body['key_arr'] = son_key_arr
                        sql_body['pla_arr'] = son_pla_arr
                        sql_body['val_arr'] = son_val_arr
                        sql_list.append(sql_body)
                        # conn.executeInsterSQL(son_key, son_key_arr, son_pla_arr, son_val_arr)
                    # sql_obj = {}
                    # sql_obj['tableName'] = intermediate
                    # sql_obj['key_arr'] = intermediate_key_arr
                    # sql_obj['pla_arr'] = intermediate_pla_arr
                    # sql_obj['val_arr_list'] = intermediate_list
                    # sql_list_many.append(sql_obj)
                    # conn.executeInsterSQLOfMultiterm(intermediate, intermediate_key_arr, intermediate_pla_arr,intermediate_list)
                else:
                    for son_key in list_obj.keys():
                        subList = list_obj[son_key]
                        key_arr += (',%s_%s' % (key, son_key))
                        pla_arr += ',%s'
                        val_arr.append(json.dumps(subList, ensure_ascii=False))

        elif isinstance(son, list):
            if key in ['photos']:
                key_arr += (',%s' % column)
                pla_arr += ',%s'
                val_arr.append(json.dumps(son, ensure_ascii=False))
            elif len(son) > 0 :
                if doList:
                    intermediate = '%s_%s_imd' % (mongodb_collection, key)
                    intermediate_key_arr = '%s_id,%s_id' % (mongodb_collection, key)
                    intermediate_pla_arr = '%s,%s'
                    intermediate_list = []
                    for sub in son:
                        id = str(uuid.uuid1()).replace('-','').upper()
                        ret = ergodic.ergodicDict(sub, mongodb_collection + "_")
                        son_key_arr = ret['key']
                        son_key_arr = 'id' + son_key_arr
                        son_pla_arr = ret['pla']
                        son_pla_arr = '%s' + son_pla_arr
                        son_val_arr = ret['val']
                        son_val_arr.insert(0, id)
                        intermediate_list.append([str(data['_id']).upper(), id])
                        sql_body = {}
                        sql_body['tableName'] = '%s_%s' % (mongodb_collection, key)
                        sql_body['key_arr'] = son_key_arr
                        sql_body['pla_arr'] = son_pla_arr
                        sql_body['val_arr'] = son_val_arr
                        sql_list.append(sql_body)
                        # conn.executeInsterSQL(son_key, son_key_arr, son_pla_arr, son_val_arr)
                    sql_obj = {}
                    sql_obj['tableName'] = intermediate
                    sql_obj['key_arr'] = intermediate_key_arr
                    sql_obj['pla_arr'] = intermediate_pla_arr
                    sql_obj['val_arr_list'] = intermediate_list
                    sql_list_many.append(sql_obj)

                    pass
                else:
                    key_arr += (',%s' % column)
                    pla_arr += ',%s'
                    val_arr.append(json.dumps(son, ensure_ascii=False))
            else:
                pass
        elif isinstance(son, str):
            key_arr += (',%s' % column)
            pla_arr += ',%s'
            val_arr.append(son)
        elif isinstance(son, unicode):
            key_arr += (',%s' % column)
            pla_arr += ',%s'
            val_arr.append(unicode(son))
        else:
            key_arr += (',%s' % column)
            pla_arr += ',%s'
            val_arr.append(unicode(str(son)))
    conn.executeInsterSQL(mongodb_collection, key_arr, pla_arr, val_arr)
    conn.executeInsterManySQL(sql_list)
    conn.executeInsterManySQLOfMultiterm(sql_list_many)


if __name__ == '__main__':
    args = sys.argv
    del args[0]

    mongodb_uri = None
    mongodb_port = None
    mongodb_db = None
    mongodb_collection = None

    sql_host = None
    sql_port = None
    sql_user = None
    sql_passwd = None
    sql_db = None
    sql_table = None

    # 遇到列表则创建新的表
    list_to_new_table = True

    config_name = 'config.ini'

    if len(args) > 0 and 'config=' in args[0]:
        config_name = args[0].replace('config=', '')

    config = ConfigParser.ConfigParser()
    config.readfp(open(config_name, "rb"))
    mongodb_uri = config.get("db", "mongodb_uri")
    mongodb_port = config.get("db", "mongodb_port")
    mongodb_port = int(mongodb_port)
    mongodb_db = config.get("db", "mongodb_db")
    mongodb_collection = config.get("db", "mongodb_collection")

    sql_host = config.get("db", "sql_host")
    sql_port = config.get("db", "sql_port")
    sql_port = int(sql_port)
    sql_user = config.get("db", "sql_user")
    sql_passwd = config.get("db", "sql_passwd")
    sql_db = config.get("db", "sql_db")
    sql_table = config.get("db", "sql_table")

    list_to_new_table = config.get('ot', 'list_to_new_table')

    text_column = config.get('ot', 'text_column').split(',')
    unique_column = config.get('ot', 'unique_column').split(',')
    sql_keywords = config.get('ot', 'sql_keywords').split(',')

    page_limit = config.get('ot', 'page_limit')
    limit = 5000

    if page_limit != None:
        limit = int(page_limit)

    mongodb_collection_list = mongodb_collection.split(',')

    for mongodb_collection in mongodb_collection_list:
        print '%s.%s 开始转换' % (mongodb_db, mongodb_collection)
        ergodic = Ergodic()

        mongo_conn = MongoHolder()
        mongo_conn.collection = mongodb_collection
        mongo_conn.initMongoDB(mongodb_uri, mongodb_port, mongodb_db)

        sql_conn = MySQLHolder()  # 实例化Mysql工具
        # 设置特殊信息
        sql_conn.text_column = text_column
        sql_conn.unique_column = unique_column

        sql_conn.sql_db = sql_db
        sql_conn.collection = mongodb_collection
        sql_conn.initMySql(sql_host, sql_port, sql_user, sql_passwd, sql_db)

        mongo_count = mongo_conn.countMongoDB()
        mongo_all_page = int(math.ceil(mongo_count / limit))

        meter = tqdm(initial=0, total=mongo_count)
        for page in range(0, mongo_all_page + 1):
            mongo_data_list = mongo_conn.readMongoTable(page, limit)
            for data in mongo_data_list:
                disposeData(data, sql_conn, doList=list_to_new_table)
                # threading.Thread(target=disposeData, args=(data,sql_conn)).start()
                meter.update(1)
            sql_conn.commitSQL()
        meter.close()
        print '%s.%s 转换完成' % (mongodb_db, mongodb_collection)
