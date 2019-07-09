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
        """
        @param data_name: table name
        @param seed_path: data in jsonlines file. Table column names/types are exactly the same as this data's field names/types
        """
        self.name = kwargs.get('data_name', '')
        connect_string = 'postgres://postgres:postgres@{}:5432/postgres'.format(getenv('DB_HOST'))
        self.Base, self._engine = automap_base(), create_engine(connect_string)
        self.Base.prepare(self._engine, reflect=True)
        self._Model = self.get_model(self.name)
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
