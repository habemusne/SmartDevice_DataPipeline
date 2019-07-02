# Assumption: 1. use postgres 2. table column names/types are exactly the same as input data's field names/types

import json
from os import getenv
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from traceback import format_exc

from util.resource import Resource
from util.logger import logger


class Database(Resource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        connect_string = 'postgres://{}:{}@{}:{}/{}'.format(
            kwargs.get('user', getenv('DB_USER')),
            kwargs.get('password', getenv('DB_PASS')),
            kwargs.get('host', getenv('DB_HOST')),
            kwargs.get('port', getenv('DB_PORT')),
            kwargs.get('db_name', getenv('DB_NAME'))
        )
        self.Base, self._engine = automap_base(), create_engine(connect_string)
        self.Base.prepare(self._engine, reflect=True)
        self._Model = self.get_model(self._data_name)
        self._seed_path = kwargs.get('seed_path', '')

    @Resource.log_notify
    def create(self):
        session = self.get_session()
        objects = []
        with open(self._seed_path, 'r') as f:
            for line in f:
                data = json.loads(line.strip())
                objects.append(self._Model(**{ key: data[key] for key in data }))
        try:
            session.bulk_save_objects(objects)
            session.commit()
        except:
            logger.error(format_exc())
            session.rollback()

    @Resource.log_notify
    def delete(self):
        session = self.get_session()
        try:
            session.query(self._Model).delete()
            session.commit()
        except:
            logger.error(format_exc())
            session.rollback()

    def get_session(self):
        return Session(self._engine)

    def get_model(self, name):
        return getattr(self.Base.classes, name)
