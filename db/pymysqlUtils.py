
import pymysql
from sqlalchemy import engine
from pyetl import DatabaseConnection


class pymysqlUtils:

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls,'_instance'):
            cls._instance = super().__new__(cls)
            #cls._instance.db  = pymysql.connect(host="192.168.66.71", user="root", password="Root@Metis03", db="ecsp")
            cls._instance.db  = pymysql.connect(**kwargs)
            return cls._instance.db
        else:
            return cls._instance.db

    #因声明对象不是自身，所以暂时弃用
    @property
    def channels(self):
        if self._instance.channels is None:
            database=DatabaseConnection(self._instance.db)
            self._instance.channels=database.db.get_table('channels').find().get_all()
            return self._instance.channels
        else:
            self._instance.channels
