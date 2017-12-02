from mysql.connector import MySQLConnection, Error

config = {
  'user': 'user',
  'password': 'pass',
  'host': '127.0.0.1',
  'database': 'clujoden'
}

class DB:
    def __init__(self):
        self.conn = MySQLConnection(**config)

    def execute(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()

    def query(self, query):
        cursor = self.conn.cursor(dictionary=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def clear(self, table):
        q = f'DELETE FROM {table}'
        self.execute(q)

    def close(self):
        self.conn.close()
