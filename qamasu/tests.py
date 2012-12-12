import unittest

from qamasu import Qamasu
from qamasu.models import Func

class QamasuTestCase(unittest.TestCase):

  def setUp(self):
    self.qamasu = Qamasu(['test_worker.for_test_one',])
  
  def testRegisterFunc(self):
    self.assertEquals(Func.objects.all().count(), 0)
    self.assertTrue(self.qamasu.register_func('test_worker.for_test_one'))
    self.assertEquals(Func.objects.all().count(), 1)
    f = Func.objects.all()[0]
    self.assertEquals(f.name, 'test_worker.for_test_one')
  
  def testNoAbillities(self):
    try:
      qamasu = Qamasu([])
      qamasu.work()
      self.fail()
    except SystemExit:
      pass
  
  def testFindJob(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1))
    job = self.qamasu.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    job.complete()
    job = self.qamasu.manager.find_job()
    self.failIf(job)
  
  def testFindJobRespectToPriority(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.assertEquals(len(self.qamasu.job_list()), 0)
    
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=3), priority=3)
    self.assertEquals(len(self.qamasu.job_list()), 3)
    
    job = self.qamasu.manager.find_job()
    self.assertEquals(job.arg['number'], 1)
    self.assertEquals(job.org_job.priority, 5)
    job.complete()
    self.assertEquals(len(self.qamasu.job_list()), 2)
    job = self.qamasu.manager.find_job()
    self.assertEquals(job.arg['number'], 2)
    self.assertEquals(job.org_job.priority, 1)
    job.complete()
    self.assertEquals(len(self.qamasu.job_list()), 1)
    job = self.qamasu.manager.find_job()
    self.assertEquals(job.arg['number'], 3)
    self.assertEquals(job.org_job.priority, 3)
    job.complete()
    self.assertEquals(len(self.qamasu.job_list()), 0)
    
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=3), priority=3)
    self.assertEquals(len(self.qamasu.job_list()), 3)
    
    job = self.qamasu.manager.find_job(prioritizing=True)
    self.assertEquals(job.arg['number'], 2)
    self.assertEquals(job.org_job.priority, 1)
    job.complete()
    self.assertEquals(len(self.qamasu.job_list()), 2)
    job = self.qamasu.manager.find_job(prioritizing=True)
    self.assertEquals(job.arg['number'], 3)
    self.assertEquals(job.org_job.priority, 3)
    job.complete()
    self.assertEquals(len(self.qamasu.job_list()), 1)
    job = self.qamasu.manager.find_job(prioritizing=True)
    self.assertEquals(job.arg['number'], 1)
    self.assertEquals(job.org_job.priority, 5)
    job.complete()
    self.assertEquals(len(self.qamasu.job_list()), 0)

    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    job.complete()
    job = self.qamasu.manager.find_job()
    self.failIf(job)
  
  def testWorkerForAllFunc(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.qamasu.register_func('test_worker.for_test_two')
    
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=3), priority=3)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=4), priority=3)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=5), priority=3)
    
    qamasu_w_both = Qamasu(['test_worker.for_test_one', 'test_worker.for_test_two'])
    
    job = qamasu_w_both.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 1)
    job.complete()
    job = qamasu_w_both.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 2)
    job.complete()
    job = qamasu_w_both.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 3)
    job.complete()
    job = qamasu_w_both.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 4)
    job.complete()
    job = qamasu_w_both.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 5)
    job.complete()
    
  def testWorkerForTestOne(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.qamasu.register_func('test_worker.for_test_two')

    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=3), priority=3)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=4), priority=3)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=5), priority=3)

    qamasu_w_one = Qamasu(['test_worker.for_test_one'])

    job = qamasu_w_one.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 1)
    job.complete()
    job = qamasu_w_one.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 2)
    job.complete()
    job = qamasu_w_one.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 4)
    job.complete()
    job = qamasu_w_one.manager.find_job()
    self.failIf(job)
    
    self.assertEquals(len(qamasu_w_one.job_list()), 2)
    
  def testWorkerForTestOne(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.qamasu.register_func('test_worker.for_test_two')

    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=3), priority=3)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=4), priority=3)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=5), priority=3)

    qamasu_w_two = Qamasu(['test_worker.for_test_two'])

    job = qamasu_w_two.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 3)
    job.complete()
    job = qamasu_w_two.manager.find_job()
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 5)
    job.complete()
    job = qamasu_w_two.manager.find_job()
    self.failIf(job)

    self.assertEquals(len(qamasu_w_two.job_list()), 3)

  def testWorkerForAllFuncRespectPriority(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.qamasu.register_func('test_worker.for_test_two')

    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=3), priority=3)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=4), priority=3)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=5), priority=3)

    qamasu_w_both = Qamasu(['test_worker.for_test_one', 'test_worker.for_test_two'])

    job = qamasu_w_both.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 2)
    job.complete()
    job = qamasu_w_both.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 3)
    job.complete()
    job = qamasu_w_both.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 4)
    job.complete()
    job = qamasu_w_both.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 5)
    job.complete()
    job = qamasu_w_both.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 1)
    job.complete()

  def testWorkerForTestOneRespectPriority(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.qamasu.register_func('test_worker.for_test_two')

    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=3), priority=3)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=4), priority=3)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=5), priority=3)

    qamasu_w_one = Qamasu(['test_worker.for_test_one'])

    job = qamasu_w_one.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 2)
    job.complete()
    job = qamasu_w_one.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 4)
    job.complete()
    job = qamasu_w_one.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_one')
    self.assertEquals(job.arg['number'], 1)
    job.complete()
    job = qamasu_w_one.manager.find_job()
    self.failIf(job)

    self.assertEquals(len(qamasu_w_one.job_list()), 2)

  def testWorkerForTestOneRespectPriority(self):
    self.qamasu.register_func('test_worker.for_test_one')
    self.qamasu.register_func('test_worker.for_test_two')

    self.qamasu.enqueue('test_worker.for_test_one', dict(number=1), priority=5)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=2), priority=1)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=3), priority=3)
    self.qamasu.enqueue('test_worker.for_test_one', dict(number=4), priority=3)
    self.qamasu.enqueue('test_worker.for_test_two', dict(number=5), priority=3)

    qamasu_w_two = Qamasu(['test_worker.for_test_two'])

    job = qamasu_w_two.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 3)
    job.complete()
    job = qamasu_w_two.manager.find_job(prioritizing=True)
    self.assertEquals(job.funcname, 'test_worker.for_test_two')
    self.assertEquals(job.arg['number'], 5)
    job.complete()
    job = qamasu_w_two.manager.find_job()
    self.failIf(job)

    self.assertEquals(len(qamasu_w_two.job_list()), 3)

  def testNoAbility(self):
    try:
      self.qamasu.enqueue('test_worker.for_test_one', dict(number=1))
      self.fail()
    except Func.DoesNotExist:
      pass
  
  def tearDown(self):
    Func.objects.all().delete()
    self.qamasu.purge()


