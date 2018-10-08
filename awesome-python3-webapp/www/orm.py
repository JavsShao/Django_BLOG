import asyncio,logging
import aiomysql


# 创建连接池
@asyncio.coroutine
def create_pool(loop,**kwargs):
    '''
    创建连接池，每个HTTP请求都可以从连接池
    中直接获取数据库链接。
    :param loop:
    :param kwargs:
    :return:
    '''
    logging.info('创建数据库连接池...')
    global __pool   # 池链接由全局变量__pool存储，缺省情况下编码设置utf8，自动提交是事物。
    __pool = yield from aiomysql.create_pool(
        host = kwargs.get('host','127.0.0.1'),
        port = '3306',
        user = kwargs['user'],
        password = kwargs['password'],
        db = kwargs['db'],
        charset = kwargs.get('charset','utf8'),
        autocommit = kwargs.get('autocommit',True),
        maxsize = kwargs.get('maxsize',10),
        minsize = kwargs.get('minsize',1),
        loop = loop
    )

@asyncio.coroutine
def select(sql, args,size=None):
    '''
    执行SELECT语句，用我们select函数执行。
    :param sql:
    :param args:
    :param size:
    :return:
    '''
    logging.log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?','%'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows return: %s'%len(rs))
        return rs

@asyncio.coroutine
def execute(sql, args):
    '''
    执行插入，更新，删除语句，定义可以通过一个的execute()函数，
    因为这3中SQL都执行相同的参数，以及返回一个整数表示影响
    的行数。
    :param sql:
    :param args:
    :return:
    '''
    logging.log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self, name=None,primary_key=False,default=None,ddl='varchar(100'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None,primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key,default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key,default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False,default)

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model:%s'%(name, tableName))
        mappings = dict()
        fields = []
        primaryKey = None
        for k,v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mapping:%s ==> %s'%(k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if v.primaryKey:
                        raise RuntimeError('Duplicate primary key for field:%s' %k)
                    primaryKey = k
                else:
                    fields.append(k)

        if not primaryKey:
            raise RuntimeError('Primary key not found!')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '、%s、'%f,fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系。
        attrs['__table__'] = tableName
        attrs['__primary_key'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        attrs['__select__'] = 'select %s, %s from ` %s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
        tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
        tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

