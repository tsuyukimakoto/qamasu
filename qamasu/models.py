from django.db import models

try:
  from django.utils import timezone as datetime
except ImportError:
  from datetime import datetime

from datetime import timedelta
import time

class Func(models.Model):
  name = models.CharField(max_length=50, db_index=True)

class Job(models.Model):
  retry_delay = 0 #seconds
  func = models.ForeignKey(Func)
  arg = models.TextField(blank=True)
  uniqkey = models.CharField(max_length=32, db_index=True)
  enqueue_time = models.DateTimeField(editable=False, blank=True, db_index=True)
  grabbed_until = models.DateTimeField(editable=False, blank=True, default=0)
  retry_cnt = models.PositiveSmallIntegerField(blank=True, default=0)
  priority = models.PositiveSmallIntegerField(blank=True, default=5)
  
  def save(self):
    if not self.id:
      self.enqueue_time = datetime.now()
      self.grabbed_until = datetime.now()
    else:
      self.enqueue_time = datetime.now() + timedelta(seconds=self.retry_delay)
    super(Job, self).save()

class ExceptionLog(models.Model):
  func = models.ForeignKey(Func)
  message = models.TextField(blank=True)
  arg = models.TextField(blank=True)
  exception_time = models.DateTimeField(blank=True)
  uniqkey = models.CharField(max_length=32)
  
  def save(self):
    if not self.id:
      self.exception_time = datetime.now()
