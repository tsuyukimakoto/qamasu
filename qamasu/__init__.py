try:
  import json
except ImportError:
  import simplejson as json

from datetime import datetime, timedelta
import time
import logging

from django.db import connection, transaction
from uuid import uuid4 as uuid

from qamasu.models import Func, Job, ExceptionLog

RETRY_SECONDS = 5;
FIND_JOB_LIMIT_SIZE = 4;

def load_module(name):
  mod = __import__(name)
  components = name.split('.')
  for comp in components[1:]:
      mod = getattr(mod, comp)
  return mod

class QamasuJob(object):
  def __init__(self, manager, job_data):
    self.id = job_data.id
    self.uniqkey = job_data.uniqkey
    self.func_id = job_data.func.id
    self.funcname = job_data.func.name
    self.retry_cnt = job_data.retry_cnt
    self.grabbed_until = job_data.grabbed_until
    self.arg = json.loads(job_data.arg)
    self.manager = manager
    self.org_job = job_data
    self.completed = False
  
  def complete(self):
    self.manager.dequeue(self)
    self.completed = True
  
  @property
  def is_completed(self):
    return self.completed

  def reenqueue(self, args):
    self.manager.reenqueue(self, args);


class Manager(object):
  def __init__(self, find_job_limit_size=4, retry_seconds=5, abilities=[]):
    self.find_job_limit_size = find_job_limit_size
    self.retry_seconds = retry_seconds
    self.func_map = {}
    self.abilities = abilities
    self._func_id_cache = {}
    
    self.register_abilities(self.abilities)

  def register_abilities(self, abilities):
    for ability in abilities:
      self.can_do(ability)

  def can_do(self, funcname):
    load_module(funcname)
    self.func_map[funcname] = 1

  def has_abilities(self):
    return self.func_map.keys()
  
  def enqueue(self, funcname, arg, uniqkey):
    func_id = self._func_id_cache.get(funcname, None)
    if not func_id:
      func = Func.objects.get(name=funcname)
      func_id = func.id
      self._func_id_cache[funcname] = func_id
    json_arg = json.dumps(arg, ensure_ascii=False)
    job = Job(func_id=func_id, arg=json_arg, uniqkey=uniqkey)
    job.save()
    return job
  
  def reenqueue(self, job_data, args):
    job = Job.objects.get(pk=job_data.id)
    for k,v in args.items():
      if has_attr(job.k):
        setattr(job, k, v)
    job.save()
    return Job.objects.get(pk=job_data.id)

  def dequeue(self, job):
    Job.objects.filter(pk=job.id).delete()
  
  def work_once(self):
    job = self.find_job()
    if not job:
      return None
    worker_module = load_module(job.funcname)
    res = None
    try:
      res = worker_module.work_safely(self, job)
    except Exception, e:
      self.job_failed(job.org_job, e)
    return res
  
  def lookup_job(self, job_id):
    job_list = Job.objects.filter(pk=job_id)
    return self._grab_a_job(job_list)
  
  def find_job(self):
    job_list = Job.objects.filter(func__name__in=self.func_map.keys())
    return self._grab_a_job(job_list[:self.find_job_limit_size])

  def _grab_a_job(self, job_list):
    for job_data in job_list:
      old_grabbed_until = job_data.grabbed_until;
      server_time = datetime.now() #TODO

      worker_mod = load_module(job_data.func.name);
      new_uniqkey = uuid().hex
      grabbed = Job.objects.filter(
                              uniqkey=job_data.uniqkey,
                              grabbed_until__lte=server_time
                          ).update(
                            uniqkey=new_uniqkey,
                            grabbed_until=server_time + timedelta(seconds=worker_mod.GRAB_FOR)
                          )
      if not grabbed:
        logging.debug("job(%d) is not found. Could be grabbed another worker.")
        continue
      grab_job = Job.objects.get(pk=job_data.id, uniqkey=new_uniqkey)
      logging.debug('NEW:%s' % grab_job.grabbed_until)
      job = QamasuJob(manager=self, job_data=job_data)
      return job
    return None
  
  def job_failed(self, job, message):
    error_log = ExceptionLog(func=job.func, message=message, uniqkey=job.uniqkey, arg=job.arg)
    error_log.save()
  
  def enqueue_failed_job(self, exception_log):
    return self.enqueue(exception_log.func.name, exception_log.arg, exception_log.uniqkey)

class Qamasu(object):
  """
  >>> from qamasu import Qamasu
  >>> qamasu = Qamasu(['workers.random_wait',])
  >>> qamasu.work()
  """
  def __init__(self, manager_abilities, find_job_limit_size=FIND_JOB_LIMIT_SIZE, retry_seconds=RETRY_SECONDS):
    self.find_job_limit_size = find_job_limit_size
    self.retry_seconds = retry_seconds
    self._manager = None
    self.manager_abilities = manager_abilities

  @property
  def manager(self):
    if not self._manager:
      self._manager = Manager(find_job_limit_size=self.find_job_limit_size,
                              retry_seconds=self.retry_seconds,
                              abilities=self.manager_abilities)
    return self._manager
  
  def enqueue(self, funcname, arg, uniqkey):
    self.manager.enqueue(funcname, arg, uniqkey)
  
  def work(self, work_delay=5):
    if not self.manager.has_abilities():
      logging.error('manager dose not have abilities.')
      import sys
      sys.exit(-1)
    while 1:
      if not self.manager.work_once():
        time.sleep(work_delay)
  
  def purge(self):
    Job.objects.all().delete()
  
  def job_list(self, funcs=None):
    query = Job.objects.all()
    if funcs:
      query = query.filter(func__name__in=funcs)
    return query.filter(grabbed_until__lte=datetime.now())[:self.find_job_limit_size]
  
  def job_count(self, funcs=None):
    query = Job.objects.all()
    if funcs:
      query = query.filter(func__name__in=funcs)
    return query.count()
  
  def exception_list(self, funcs=None):
    query = ExceptionLog.objects.all()
    if funcs:
      query = query.filter(func__name__in=funcs)
    return query
  
  def func_list(self):
    return Func.objects.all().order_by('id')
  
  def register_func(self, func_name):
    return Func.objects.get_or_create(name=func_name)[1]

