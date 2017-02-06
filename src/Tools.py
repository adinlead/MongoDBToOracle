# encoding: UTF-8
class Ergodic:
    def ergodicDict(self, data, prefix='', doList=False):
        key_arr = ''
        pla_arr = ''
        val_arr = []
        list_obj = {}
        for key in data.keys():
            son = data[key]
            if isinstance(son, dict):
                ret = self.ergodicDict(data=son, prefix='%s_' % key)
                key_arr += ret['key']
                pla_arr += ret['pla']
                val_arr.extend(ret['val'])
                list_obj.update(ret['list'])
            elif isinstance(son, list):
                if doList:
                    self.ergodicList()
                else:
                    list_obj[key] = son
            elif isinstance(son, str):
                key_arr += (',%s%s' % (prefix, key))
                pla_arr += (',%s')
                val_arr.append(son)
            elif isinstance(son, unicode):
                key_arr += (',%s%s' % (prefix, key))
                pla_arr += (',%s')
                val_arr.append(unicode(son))
            else:
                key_arr += (',%s%s' % (prefix, key))
                pla_arr += (',%s')
                val_arr.append(unicode(str(son)))
        result = {'key': key_arr, 'pla': pla_arr, 'val': val_arr, 'list': list_obj}
        return result

    def ergodicList(self):
        pass
