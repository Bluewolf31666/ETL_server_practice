import psycopg2 as pgsql
import pymongo as mongo

class DBConnector:

    def __init__(self, host, port, user, password, database, location, engine):
        #print('init')
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.location = location
        self.engine = engine
        self.conn = None
        #print('init first self.conn', self.conn)

        if self.location == 'localhost_source':
            if self.engine == 'postgre':
                #print('func _postgre_connect', self._postgre_connect)
                #print('func _mongodb_connect', self._mongodb_connect)
                self.enter_connect = self._postgre_connect
                #print('init second self.conn', self.conn)
            elif self.engine == 'mongodb':
                #print('func _postgre_connect', self._postgre_connect)
                #print('func _mongodb_connect', self._mongodb_connect)
                self.enter_connect = self._mongodb_connect
                #print('init second self.conn', self.conn)
            else:
                raise RuntimeError(f"{self.engine} is not supported")


        elif self.location == 'localhost_target':
            if self.engine == 'postgre':
                self.enter_connect = self._postgre_connect
            elif self.engine == 'mongodb':
                self.enter_connect = self._mongodb_connect
            else:
                raise RuntimeError(f"{self.engine} is not supported")


        else :
            raise RuntimeError(f"{self.location} is not supported")

    def __enter__(self):
        #print('enter')
        #print('enter first self.conn', self.conn)
        self.enter_connect()
        #print('enter second self.conn', self.conn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #print('exit')
        try :
            self.conn.close()
        except:
            pass
        # self.conn.close()
        # self.get_collection.close()

    def _postgre_connect(self):
        #print('postgre_connect')
        self.conn = pgsql.connect(host = self.host, port = self.port, user = self.user, password = self.password, dbname = self.database)

    def _mongodb_connect(self):
        #print('mongodb_connect')
        self.conn = mongo.mongo_client.MongoClient(host = self.host, port = self.port, username = self.user, password = self.password).get_database(self.database) 
    