# Assumption: 1. use postgres 2. table column names/types are exactly the same as input data's field names/types

import json
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
            kwargs.get('user'), kwargs.get('password'),
            kwargs.get('host'), kwargs.get('port'), kwargs.get('db_name'))
        Base, self._engine = automap_base(), create_engine(connect_string)
        Base.prepare(self._engine, reflect=True)
        self._Model = getattr(Base.classes, self._data_name)
        self._seed_path = kwargs.get('seed_path')

    @Resource.log_notify
    def create(self):
        session = Session(self._engine)
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
        session = Session(self._engine)
        try:
            session.query(self._Model).delete()
            session.commit()
        except:
            logger.error(format_exc())
            session.rollback()

