# -*- coding: utf-8 -*-
"""
@time: 2021/01/06 11:04 下午
@desc:
"""
from pydbclib import connect, Database
from sqlalchemy import engine

from pyetl.es import Client
from utils import LogUtils
import logging


class DatabaseConnection(object):

    def __init__(self, db):
        LogUtils.log_config(f'ecsp','0.0.0.0','0000')
        self.logger = logging.getLogger(f'DatabaseConnection')
        if isinstance(db, Database):
            self.db = db
        elif isinstance(db, engine.base.Engine) or hasattr(db, "cursor"):
            self.db = connect(driver=db)
        elif isinstance(db, dict):
            self.db = connect(**db)
        elif isinstance(db, str):
            self.db = connect(db)
        else:
            raise ValueError("db 参数类型错误")


class ElasticsearchConnection(object):
    _client = None

    def __init__(self, es_params=None):
        if es_params is None:
            es_params = {}
        self.es_params = es_params

    @property
    def client(self):
        if self._client is None:
            self._client = Client(**self.es_params)
        return self._client
