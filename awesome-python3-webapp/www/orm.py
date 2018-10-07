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