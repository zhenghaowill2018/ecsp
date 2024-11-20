
import asyncio
import logging
from urllib.parse import urlsplit

import aiomysql
from pydbclib import FormatCompiler,ParameterError

logger = logging.getLogger(__name__)

def split_url(url:str) -> str:
    """
    url拆分工具，mysql默认没有url读取方法，自己实现的url拆分

    >>> split_url("mysql://fido:123456@127.0.0.1:3306/metis_formal_dev")
    SplitResult(scheme='mysql', netloc='fido:123456@127.0.0.1:3306', path='', query='', fragment='')
    """
    res = urlsplit(url)
    result = {
        "user": res.username,
        "password": res.password,
        "host": res.hostname,
        "port": res.port,
        "db": res.path[1:] or None
    }
    return result


class Mysql:
    """
    读取mysql url格式
    >>> mysql = Mysql('mysql://xxxxxx')
    mysql ping: True, 140528135535248

    >>> mysql = Mysql('mysql://xxxxxx')
    mysql ping: True, 140528135535248
    """
    def __new__(cls,**kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
            config = split_url(kwargs['url'])
            cls._instance.cnxpool = None
            try:
                loop=asyncio.get_event_loop()
                cls._instance.cnxpool = loop.run_until_complete(aiomysql.create_pool(minsize=30,maxsize=80,autocommit=True,**config))
            except:
                logger.error('connect error.', exc_info=True)
        return cls._instance

    async def get_connection(self):
        conn=await self.cnxpool.acquire()
        return conn

    async def release(self,conn):
        await self.cnxpool.release(conn)


    async def execute(self, sql, args=None, autocommit=False):
        """
        执行sql语句：
        :param sql: sql语句
        :param args: sql语句参数
        :param autocommit: 执行完sql是否自动提交
        :return: ResultProxy

        Example:
            db.execute(
                "insert into foo(a,b) values(:a,:b)",
                {"a": 1, "b": "one"}
            )

            对条写入
            db.execute(
                "insert into foo(a,b) values(:a,:b)",
                [
                    {"a": 1, "b": "one"},
                    {"a": 2, "b": "two"}
                ]
            )
        """
        mysql_conn = await self.get_connection()
        try:
            mysql_cursor = await mysql_conn.cursor(aiomysql.DictCursor)
            if args is None or isinstance(args, dict):
                sql,args=FormatCompiler(sql,args).process_one()
                res = await mysql_cursor.execute(sql,args)
            elif isinstance(args, (list, tuple)):
                res = await mysql_cursor.executemany(sql, args)
            else:
                raise ParameterError("'params'参数类型无效")
            await mysql_conn.commit()
        finally:
            await self.release(mysql_conn)
        return res

    async def format_condition(self,condition):
        param = {}
        if isinstance(condition, dict):
            expressions = []
            for i, k in enumerate(condition):
                param[f"c{i}"] = condition[k]
                expressions.append(f"{k}=:c{i}")
            condition = " and ".join(expressions)
        condition = f" where {condition}" if condition else ""
        return condition, param

    async def format_update(self,update):
        param = {}
        if isinstance(update, dict):
            expressions = []
            for i, k in enumerate(update):
                param[f"u{i}"] = update[k]
                expressions.append(f"{k}=:u{i}")
            update = ",".join(expressions)
        if not update:
            raise ParameterError("'update' 参数不能为空值")
        return update, param

    def _get_insert_sql(self, table,columns):
        return f"insert into {table} ({','.join(columns)})" \
               f" values ({','.join([':%s' % i for i in columns])})"

    async def _insert_one(self, table,record):
        """
        表中插入一条记录
        :param record: 要插入的记录数据，字典类型
        """
        if isinstance(record, dict):
            columns = record.keys()
            return await self.execute(self._get_insert_sql(table,columns), record, autocommit=True)
        else:
            raise ParameterError("无效的参数")

    async def _insert_many(self, table,records):
        """
        表中插入多条记录
        :param records: 要插入的记录数据，字典集合
        """
        if not isinstance(records, (tuple, list)):
            raise ParameterError("records param must list or tuple")
        sample = records[0]
        if isinstance(sample, dict):
            columns = sample.keys()
            return await self.execute(self._get_insert_sql(table,columns), records, autocommit=True)
        else:
            raise ParameterError("无效的参数")

    async def upsert(self,table, record):
        """
        表中插入记录,若存在便更新
        :param records: 要插入的记录数据，字典or字典列表
        """
        update, p2 = await self.format_update(record)
        if isinstance(record, dict):
            columns = record.keys()
            sql=self._get_insert_sql(table,columns)
            p2.update(record)
            return await self.execute(sql+f" ON DUPLICATE KEY UPDATE {update}", p2, autocommit=True)
        else:
            raise ParameterError("无效的参数")

    async def insert(self, table,records):
        """
        表中插入记录
        :param records: 要插入的记录数据，字典or字典列表
        """
        if isinstance(records, dict):
            return await self._insert_one(table,records)
        else:
            return await self._insert_many(table,records)

    async def find(self, table=None,condition=None, fields=None):
        """
        按条件查询所有符合条件的表记录
        :param condition: 查询条件，字典类型或者sql条件表达式
        :param fields: 指定返回的字段
        :return: 生成器类型
        """
        if fields is None:
            fields = "*"
        else:
            fields = ','.join(fields)
        condition, param = await self.format_condition(condition)
        result=await self.selectDirt(f"select {fields} from {table}{condition}",param)
        return result

    async def update(self, table,condition, update):
        """
        表更新操作
        :param condition: 更新条件，字典类型或者sql条件表达式
        :param update: 要更新的字段，字典类型
        :return: 返回影响行数
        """
        condition, p1 = await self.format_condition(condition)
        update, p2 = await self.format_update(update)
        p1.update(p2)
        res=await self.execute(f"update {table} set {update}{condition}", p1, autocommit=True)
        return res

    async def delete(self,table, condition):
        """
        删除表中记录
        :param condition: 删除条件，字典类型或者sql条件表达式
        :return: 返回影响行数
        """
        condition, param = await self.format_condition(condition)
        return await self.execute(f"delete from {table}{condition}", param)

    async def selectDirt(self,sql,args):
        mysql_conn = await self.get_connection()
        mysql_cursor = await mysql_conn.cursor(aiomysql.DictCursor)
        sql,args=FormatCompiler(sql,args).process_one()
        await mysql_cursor.execute(sql,args)
        mysql_result= await mysql_cursor.fetchall()
        mysql_conn.commit()
        await self.release(mysql_conn)
        return mysql_result


async def test(mysqlDb):
    mysql_conn = await mysqlDb.get_connection()
    mysql_cursor=await mysql_conn.cursor(aiomysql.DictCursor)
    await mysql_cursor.execute(sql)
    mysql_result= await mysql_cursor.fetchall()
    print(mysql_result)

if __name__ == "__main__":
    mysqlDb=Mysql(url="mysql://root:Root@Metis03@192.168.66.71:3306/metis_formal_dev")
    sql="select * from au_review where asin='B0002VAZSY'"
    loop=asyncio.get_event_loop()
    result=loop.run_until_complete(test(mysqlDb))
