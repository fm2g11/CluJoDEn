from random import randint
import hashlib
import time
from contextlib import closing
import threading
import multiprocessing
import sys
from db import DB

class Worker:

  def __init__(self, wait=30, numThreads=-1):
    self.wait = wait
    if numThreads < 1:
      cores = multiprocessing.cpu_count()
      print("Setting number of threads to CPU cores: " + str(cores))
      numThreads = cores
    self.numThreads = numThreads


  def start(self, function):
    for i in range(self.numThreads):
      thread = WorkerThread('Thread_' + str(i), self.wait, function)
      thread.start()

    while True:
      time.sleep(1)

class WorkerThread (threading.Thread):

  def __init__(self, name, wait, function):
    self.NAME = str(randint(0,999999999))
    threading.Thread.__init__(self)
    self.daemon = True
    self.name = name
    self.wait = wait
    self.function = function
    self.db = DB()

  def run(self):
    while True:
      try:
        print(self.name + ": checking for data")
        # UPDATE ROW
        self.db.execute(self.getProcessingQuery())
        print("row updated")

        # SELECT ROW
        self.processing = self.db.query(self.getSelectReadyQuery())
        print(f"select row {self.processing}")

        # PROCESS JOB
        for combination in self.processing:
          print(combination)
          print(self.name + ": processing data for id: " + str(combination['id']))
          res = self.function(combination)
          # SET RESULT & COMPLETE
          self.db.execute(self.getCompleteQuery(res, combination['id']))
      except:
        print(self.name + ": an error occoured. Reconnecting...")
        print("Unexpected error:", sys.exc_info()[0])
        self.processing = []
        self.db.close()
        time.sleep(1)
        self.db = DB()

      if len(self.processing) == 0:
        print(self.name + ": no data found. Retrying in " + str(self.wait) + "s")
        time.sleep(self.wait)

  def getProcessingQuery(self):
    query = '''UPDATE Input SET '''
    query += '''status = "processing",'''
    query += '''worker = "''' + self.NAME + '''" WHERE status = "ready" LIMIT 1'''
    return query

  def getSelectReadyQuery(self):
    query = '''SELECT * FROM Input WHERE status = "processing" AND worker = "''' + self.NAME + '''"'''
    return query

  def getCompleteQuery(self, res, rowId):
    query = '''UPDATE Input SET '''
    query += '''status = "complete",'''
    for key in res:
      query += str(key) + ''' = "''' + str(res[key]) + '''",'''

    query = query[:len(query)-1]
    query += ''' WHERE id=''' + str(rowId)
    return query
