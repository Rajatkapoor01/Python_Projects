import sqlite3


class Database():
   """Database Helper class"""
   def __init__(self):
      self.conn = None

   def connect(self, filename):
      """ Connect the databse handler to a databse """
      self.conn = sqlite3.connect(filename)
      self.conn.row_factory = self.dict_factory

   def insert(self,tableName,data):
      """ Inserts data in the form of a dictionary into a table """
      columns = ', '.join(data.keys())
      placeholders = ', '.join('?' * len(data))
      sql = 'INSERT INTO {} ({}) VALUES ({})'.format(tableName, columns, placeholders)
      self.execQuery(sql, data.values())
      self.conn.commit()

   def createTable(self,tableName,columns):
      """ Creates a table from a column dictionary
          in the following form:
          columns[columnName] = "TYPE".
          An ID is automatically created
      """
      columns['id'] = "INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE"
      cur = self.conn.cursor()
      cols = ",".join(['%s %s' % (key, value) for (key, value) in columns.items()])
      cur.execute("CREATE TABLE IF NOT EXISTS " + tableName + "(" + cols + ")")
      self.conn.commit()


   def dict_factory(self,cursor, row):
      """ Used to return rows as dictionary"""
      d = {}
      for idx, col in enumerate(cursor.description):
         d[col[0]] = row[idx]
      return d

   def execQuery(self, query, data=None):
      """ Execute a query on the database """
      print query
      cur = self.conn.cursor()
      if data:
         cur.execute(query, data)
      else:
         cur.execute(query)
      return cur

   def dropTable(self, tableName):
      self.execQuery("DROP TABLE " + tableName)
      self.conn.commit()


   def close(self):
      """ Closes the connection to the database """
      self.conn.close()


def main(self):
   db = Database()
   db.connect("test.db")
   db.createTable("swer",{'file':'TEXT'})
   #Usage
   #db.execQuery("SELECT * FROM swer").fetchone()[column]
   #db.execQuery("SELECT * FROM swer").fetchall()[row][column]
   db.close()


if __name__ == '__main__':
   main()
