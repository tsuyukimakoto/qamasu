"""Copyright (c) 2006, www.everes.net
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, 
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, 
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, 
      this list of conditions and the following disclaimer in the documentation 
      and/or other materials provided with the distribution.
    * Neither the name of the everes nor the names of its contributors may be 
      used to endorse or promote products derived from this software without 
      specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE 
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF 
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."""
try:
  import json
except ImportError:
  import simplejson as json

try:
  from django.utils import timezone as datetime
except ImportError:
  from datetime import datetime
from datetime import timedelta
import time
import logging

from django.db import connection, transaction
from uuid import uuid4 as uuid

from django.conf import settings

from qamasu.models import Func, Job, ExceptionLog

RETRY_SECONDS = 5;
FIND_JOB_LIMIT_SIZE = 4;

logger = logging.getLogger('qamasu')

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
    logger.debug('Job id:%d initialize complete.' % (self.id,))
  
  def complete(self):
    self.manager.dequeue(self)
    self.completed = True
    logger.debug('Job id:%d completed.' % (self.id,))
  
  @property
  def is_completed(self):
    return self.completed

  def reenqueue(self, args):
    self.manager.reenqueue(self, args);
    logger.debug('Job id:%d retry(enqueued).' % (self.id,))


class Manager(object):
  def __init__(self, find_job_limit_size=4, retry_seconds=5, abilities=[]):
    self.find_job_limit_size = find_job_limit_size
    self.retry_seconds = retry_seconds
    self.func_map = {}
    self.abilities = abilities
    self._func_id_cache = {}
    self.register_abilities(self.abilities)
    self.set_isolation_level = False

  def register_abilities(self, abilities):
    for ability in abilities:
      self.can_do(ability)

  def can_do(self, funcname):
    load_module(funcname)
    self.func_map[funcname] = 1

  def has_abilities(self):
    return self.func_map.keys()

  def enqueue(self, funcname, arg, uniqkey, priority=None):
    func_id = self._func_id_cache.get(funcname, None)
    if not func_id:
      func = Func.objects.get(name=funcname)
      func_id = func.id
      self._func_id_cache[funcname] = func_id
    json_arg = json.dumps(arg, ensure_ascii=False)
    if priority:
      job = Job(func_id=func_id, arg=json_arg, uniqkey=uniqkey, priority=priority)
    else:
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
  
  @transaction.autocommit
  def work_once(self, prioritizing=False):
    job = self.find_job(prioritizing=prioritizing)
    if not job:
      logger.debug('No Job.')
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
  
  def find_job(self, prioritizing=False):
    job_list = Job.objects.filter(func__name__in=self.func_map.keys(),
      grabbed_until__lte=datetime.now())
    if prioritizing:
      job_list = job_list.order_by('priority', 'id')
    else:
      job_list = job_list.order_by('id')
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
        logger.debug("job(%d) is not found. Could be grabbed another worker.", job_data.id)
        continue
      grab_job = Job.objects.get(pk=job_data.id, uniqkey=new_uniqkey)
      logger.debug('NEW:%s' % grab_job.grabbed_until)
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
  True
  >>> qamasu.work()
  """
  def __init__(self, manager_abilities, find_job_limit_size=FIND_JOB_LIMIT_SIZE, retry_seconds=RETRY_SECONDS):
    self.find_job_limit_size = find_job_limit_size
    self.retry_seconds = retry_seconds
    self._manager = None
    self.manager_abilities = manager_abilities
    self.gentle_terminate = False


  @property
  def manager(self):
    if not self._manager:
      self._manager = Manager(find_job_limit_size=self.find_job_limit_size,
                              retry_seconds=self.retry_seconds,
                              abilities=self.manager_abilities)
    return self._manager
  
  def enqueue(self, funcname, arg, uniqkey=None, priority=None):
    if not uniqkey:
      uniqkey = uuid().hex
    self.manager.enqueue(funcname, arg, uniqkey, priority=priority)
  
  @transaction.autocommit
  def work(self, work_delay=5, prioritizing=False):
    if not self.manager.has_abilities():
      logger.error('manager dose not have abilities.')
      import sys
      sys.exit(-1)
    while 1:
      if self.gentle_terminate:
        logger.info('gentle terminate')
        break
      if not self.manager.work_once(prioritizing=prioritizing):
        time.sleep(work_delay)

  def handle_terminate(self, *args):
    '''
    # for python-daemon
    import signal
    from daemon import DaemonContext
    from lockfile.pidlockfile import PIDLockFile

    from qamasu import Qamasu

    qamasu = Qamasu(['workers.random_wait',])

    dc = DaemonContext(
      pidfile=PIDLockFile('/var/run/qamasu_daemon.pid'),
      stdout=open('/var/log/qamasu/out.log', 'w+'),
      stderr=open('/var/log/qamasu/err.log', 'w+')
    )
    dc.signal_map[signal.SIGTERM] = qamasu.handle_terminate

    with dc:
      import os
      qamasu.work()
    '''
    logger.info('preparing gentle terminate')
    self.gentle_terminate = True
  
  def work_prioritizing(self, work_delay=5):
    self.work(work_delay=work_delay, prioritizing=True)
  
  def purge(self):
    Job.objects.all().delete()
  
  def job_list(self, funcs=None):
    query = Job.objects.all().order_by('id')
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

