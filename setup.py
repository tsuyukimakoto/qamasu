from setuptools import setup, find_packages

LONG_DESCRIPTION = '''
====================================================
Qamasu . Job Queue Application written in Python
====================================================


Qamasu is JobQueue system that respects TheSchwartz.

Suited to load leveling.

Implemented using optimistic lock.

Requirements
--------------------------------------

* Python>=2.6
* Django>=1.0

Usage
--------------------------------------

Set Qamasu up!
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Qamasu is a Django application.

You need add qamasu to your or new django project's INSTALLED_APPS.

And //manage.py syncdb//.

Write your worker.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Define **GRAB_FOR** in seconds that is max time worker grabbed for a work.

Define **def work_safely(manager, job):** that is a work you need.

See `sample worker`_ in workers directory for detail.

.. _`sample worker`: http://bitbucket.org/tsuyukimakoto/qamasu/src/tip/workers/random_wait.py

Registration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
You need add worker to abilities.

register_func insert fuction record into database table if not exist.

::

    >>> from qamasu import Qamasu
    >>> qamasu = Qamasu([])
    >>> qamasu.register_func('workers.random_wait')

Queue!
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Once fuction is registered to qamasu, you can enqueue jobs.

Add hundreds Queues.::

    >>> from qamasu import Qamasu
    >>> from random import uniform
    >>> qamasu = Qamasu(['workers.random_wait',])
    >>> for x in xrange(1,500):
          arg = dict(random_number=uniform(1,5))
          qamasu.enqueue('workers.random_wait', arg)

Add a highest-priority queue.::

    >>> qamasu.enqueue('workers.random_wait', dict(random_number=uniform(1,5)), priority=1)

Work! Work! Work!
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Process enqueued job.

1. Instantiate Qamasu with availabilities.
2. call work method. infinite loop inside this method.

work method process queue as FIFO.

::

    >>> from qamasu import Qamasu
    >>> qamasu = Qamasu(['workers.random_wait',])
    >>> qamasu.work()

Use work_prioritizing method if you tend to process job respects to priority.
::

    >>> from qamasu import Qamasu
    >>> qamasu = Qamasu(['workers.random_wait',])
    >>> qamasu.work_prioritizing()

Caution!
--------------------------------------

For MySQL backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must set worker's transaction isolation level to read commited before working qamasu when you use InnoDB.
::

    >>> from django.db import connection
    >>> from qamasu import Qamasu
    >>> connection.cursor().execute('set session transaction isolation level read committed')
    >>> qamasu = Qamasu(['workers.random_wait',])
    >>> qamasu.work()

Or you have to set transaction isolation level read committed. It's global settings and dangerous.
::

  [mysqld]
  transaction-isolation=Read-Committed'''

setup(
    name='qamasu',
    version="0.4",
    description="Qamasu is Job Queue system that respects TheSchwartz.",
    long_description=LONG_DESCRIPTION,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Framework :: Django",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: System :: Distributed Computing",
    ],
    keywords='queue,job,django,distributed',
    author='makoto tsuyuki',
    author_email='mtsuyuki@gmail.com',
    url='http://bitbucket.org/tsuyukimakoto/qamasu',
    license='New BSD',
    packages=['qamasu'],
    install_requires=[
        "Django >= 1.0",],
    zip_safe=True,
)
