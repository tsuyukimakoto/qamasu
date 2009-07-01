import time

GRAB_FOR = 50

def work_safely(manager, job):
  """
  >>> from qamasu import Qamasu
  >>> from uuid import uuid4 as uuid
  >>> from random import uniform
  >>> qamasu = Qamasu(['workers.random_wait',])
  >>> qamasu.register_func('workers.random_wait')
  >>> for x in xrange(1,500):
        arg = dict(random_number=uniform(1,5))
        qamasu.enqueue('workers.random_wait', arg, uuid().hex)
  >>> qamasu.work()
  """
  print '%d wait %d seconds' % (job.id, job.arg['random_number'])
  time.sleep(job.arg['random_number'])
  job.complete()
