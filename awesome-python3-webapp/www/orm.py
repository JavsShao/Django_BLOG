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

