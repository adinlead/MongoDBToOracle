import MySQLdb
param_str = '%s,%s,%s,%s,%s'
param_str = param_str % tuple([':%s' % x for x in range(1 , param_str.count('%s') + 1)])
print param_str