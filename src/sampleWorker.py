#!/usr/bin/env python3

from Worker import Worker
from time import sleep

def function(inputs):
  sleep(3)
  return {
    "res": inputs['key2'] * 2
  }

worker = Worker(5,1)


worker.start(function)
