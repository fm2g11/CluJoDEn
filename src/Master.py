from contextlib import closing
from db import DB

class Master:

  def __init__(self):
    self.db = DB()

  def start(self):
    combination = [0] * len(self.params)
    print(combination)
    self.combinations(0, combination)

  def combinations(self, i, combination):
    if i == len(self.params):
      query = self.getTableQuery(combination)
      self.db.execute(query)
      return

    for p in self.params[i]:
      combination[i] = p
      self.combinations(i+1, combination)


  def getTableQuery(self, combination):
    query = '''INSERT INTO Input SET '''
    for i in range(len(combination)):
      query += self.inputTableParams[i][0] + ''' = "''' + str(combination[i]) + '''", '''
    query += '''status = "ready"'''
    return query

  def setInputParams(self, params):
    self.params = params

  def cleanInputTable(self):
   self.db.clear('Input')

  def createTable(self, inputTableParams):
    self.inputTableParams = inputTableParams
    self.db.execute('DROP TABLE IF EXISTS Input')
    self.db.execute(self.getTableSQL("Input", inputTableParams))
    print("Table Input created")

  def getTableSQL(self, table, params):
    query = '''
      CREATE TABLE ''' + table + '''
      (
        id INT PRIMARY KEY AUTO_INCREMENT,
      '''
    print(params)
    for param in params:
      key = param[0]
      val = param[1]
      query += str(key) + ' ' + str(val) + ','

    query += '''status varchar(256),'''
    query += '''worker varchar(256)'''
    query += '''); '''
    return query
