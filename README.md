# CluJoDEn
Clusters Job Distribution Engine

This is a simple library to execute long timing / high computation tasks on multiple devices using a central SQL database. The library uses the __Producer / Consumer__ pattern with a **Master** object that produces the specs for each task and publishes them to the database. The **Worker** object pulls spec combinations from the database, execute the task locally and publishes the results back to the db.

The Workers use multithreading to take advantage of processors with multiple cores. You can specify the number of threads or let the system pick the number of cores on your machine.

---

## Workflow

1 master engine:
  - takes in task data
  - split it up into single jobs
  - publish jobs to common database

2 worker engines:
  - periodically check common db
  - pick jobs from the db
    - mark them as processing
  - execute jobs
  - publish resuts to common db
    - mark the as finished

3 master engine:
  - wait until all jobs are finished
  - gather results
  - merge and show

## Getting Started
1. Put your database credentials in __db.py__.
2. Create a corresponding (empty) db in your database.
3. Edit __sampleMasterMain.py__, pass your inputs and outputs to the **Master.createTable()** method. Then Set the inputs with the combination for each parameter
4. Edit __sampleWorkerMain.py__, define your task execution function, and pass it to the **Worker.start()** method
5. Good Luck and enjoy
