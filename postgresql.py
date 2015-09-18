from dbproxy import DbProxy
import psycopg2

class Postgresql(DbProxy):
    def __init__(self, dbapi, dbtype, dbhost, dbport, dbname, dbuser, dbpass, schema):
        DbProxy.__init__(self, dbapi, dbtype, dbhost, dbport, dbname, dbuser, dbpass, schema)

    def tables(self):
        tables = []
        cur = self.conn.cursor()

        cur.execute("SELECT table_name FROM information_schema.tables \
        WHERE table_schema = '%s';" % self.schema)

        rows = cur.fetchall()
        for row in rows:
            tables.append(row[0])
        return tables

    def primary_key_of_table(self, table_name):
        cur = self.conn.cursor()

        cur.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type \
        FROM   pg_index i \
        JOIN   pg_attribute a ON a.attrelid = i.indrelid \
        AND a.attnum = ANY(i.indkey) \
        WHERE  i.indrelid = '%s'::regclass \
        AND    i.indisprimary;" % (table_name) )
    
        rows = cur.fetchall()
        if rows :
            return rows[0][0]
        else:
            return None

    def columns_of_table(self, table_name):
        columns = []
        cur = self.conn.cursor()

        cur.execute("select column_name from information_schema.columns where \
        table_name='%s';" % (table_name) )
    
        rows = cur.fetchall()
        for row in rows:
            columns.append( row[0] )
        return columns
        
