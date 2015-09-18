class DbProxy(object):
    def __init__(self, dbapi, dbtype, dbhost, dbport, dbname, dbuser, dbpass, schema):
        self.dbapi  = dbapi
        self.dbtype = dbtype
        self.dbhost = dbhost
        self.dbport = dbport
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpass = dbpass
        self.schema = schema
        self.conn = None

    def open(self):
        self.conn = self.dbapi.connect("dbname='{0}' user='{1}' host='{2}' port={3} password='{4}'"
                                       .format(self.dbname, self.dbuser, self.dbhost, self.dbport, self.dbpass ))

    def close(self):
        self.conn.close()

    def tables(self, tables):
        raise NotImplementedError()
    def primary_key_of_table(self, table_name):
        raise NotImplementedError()
    def columns_of_table(self, table_name):
        raise NotImplementedError()

def create(dbtype, dbhost, dbport, dbname, dbuser, dbpass, schema):
    dbproxymod = __import__(dbtype.lower())
    if dbtype.lower() == 'postgresql':
        dbapi = __import__('psycopg2')
        return dbproxymod.Postgresql(dbapi, dbtype, dbhost, dbport, dbname, dbuser, dbpass, schema)
    else:
        raise NotImplementedError()        


if __name__ == "__main__":
    pass
