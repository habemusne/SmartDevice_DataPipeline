import json
import requests
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from os.path import join

STATIC_DATA_PATH = '/home/ubuntu/smart-device/docker_volumes/connect/static_data/historical.data'
DB_HOST = 'ec2-3-218-215-56.compute-1.amazonaws.com'
DB_PORT = 5432
DB_USER = 'postgres'
DB_PASS = 'postgres'
DB_NAME = 'postgres'
CONNECT_HOST = 'ec2-3-218-215-56.compute-1.amazonaws.com'
CONNECT_PORT = 8083
CONNECTOR_NAME = 'con-historical'


def database():
    db_connect_url = 'postgres://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
    Base = automap_base()
    engine = create_engine(db_connect_url)
    Base.prepare(engine, reflect=True)
    session = Session(engine)

    Model = Base.classes.historical
    objects = []
    with open(STATIC_DATA_PATH, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            objects.append(Model(
                user_id=data['user_id'],
                zipcode=data['zipcode'],
                latitude=data['latitude'],
                longitude=data['longitude'],
                city=data['city'],
                state=data['state'],
                area=data['area'],
            ))
    session.bulk_save_objects(objects)
    session.commit()


def connector():
    connect_api_url = 'http://{}:{}/connectors'.format(CONNECT_HOST, CONNECT_PORT)
    db_connect_url_jdbc = 'jdbc:postgresql://{}:{}/{}'.format(DB_HOST, DB_PORT, DB_NAME)
    payload = {
        'name': CONNECTOR_NAME,
        'config': {
            'connector.class': 'io.confluent.connect.jdbc.JdbcSourceConnector',
            'connection.url': db_connect_url_jdbc,
            'connection.user': DB_USER,
            'connection.password': DB_PASS,
            'topic.prefix': 'topic3-',
            'poll.interval.ms' : 3600000,
            'numeric.mapping': 'best_fit',
            'table.whitelist' : 'historical',
            'mode':'bulk',
            'transforms':'createKey,extractInt',
            'transforms.createKey.type':'org.apache.kafka.connect.transforms.ValueToKey',
            'transforms.createKey.fields':'id',
            'transforms.extractInt.type':'org.apache.kafka.connect.transforms.ExtractField$Key',
            'transforms.extractInt.field':'id',
        }
    }
    requests.delete(join(connect_api_url, CONNECTOR_NAME))
    requests.post(connect_api_url, json=payload)
