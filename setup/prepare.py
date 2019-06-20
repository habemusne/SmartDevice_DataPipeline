import sys
from os.path import join, abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))

from util.database import Database
from util.connector import Historical as HistoricalConnector, Realtime as RealtimeConnector
from util.ksql_object import HistoricalTable, RealtimeStream


if __name__ == '__main__':
    for obj in [Database, HistoricalConnector, RealtimeConnector, HistoricalTable, RealtimeStream]:
        obj.create()
