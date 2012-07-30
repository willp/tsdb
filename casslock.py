#!/usr/bin/python
import random, struct, time, uuid

import pycassa, pycassa.pool
from pycassa.cassandra.ttypes import ConsistencyLevel

class CassLock(object):
  BASE_DELAY = 1.5
  MAX_LOCKTIME = 600
  SHORT_SLEEP = 0.1

  SERVERS = None
  KSPACE = None
  POOL = None
  COL_FAMILY = None
  cfconn = None

  @classmethod
  def setup(cls, servers, keyspace='cassalock', col_family='locker', build_cf=True):
    cls.SERVERS = servers
    cls.KEYSPACE = keyspace
    cls.COL_FAMILY = col_family
    if build_cf:
      cls.build_cf()
    cls.POOL = pycassa.pool.ConnectionPool(keyspace, server_list=servers, prefill=False)
    cls.cfconn = pycassa.ColumnFamily(cls.POOL, col_family)

  @classmethod
  def build_cf(cls):
    import pycassa.system_manager as sysmgr
    csys = sysmgr.SystemManager(cls.SERVERS[0])
    keyspaces = csys.list_keyspaces()
    print 'Looking for keyspace %r in list: %r' % (cls.KEYSPACE, keyspaces)
    if cls.KEYSPACE not in keyspaces:
      ret = csys.create_keyspace(cls.KEYSPACE,
                                 replication_strategy='SimpleStrategy',
                                 strategy_options=dict(replication_factor='3'),
                                 durable_writes=False)
    cfs = csys.get_keyspace_column_families(cls.KEYSPACE, use_dict_for_col_metadata=True)
    if cls.COL_FAMILY not in cfs:
      csys.create_column_family(cls.KEYSPACE, cls.COL_FAMILY,
                                comparator_type='AsciiType',
                                default_validation_class='FloatType',
                                key_validation_class='AsciiType',
                                read_repair_chance=0.0001,
                                min_compaction_threshold=4,
                                max_compaction_threshold=8,
                                compaction_strategy='SizeTieredCompactionStrategy',
                                compression_options=dict(sstable_compression='DeflateCompressor')
                                )

  def __init__(self, lockname):
    self.lockname = lockname
    self.uid = uuid.uuid4().hex
    self.collisions = 0

  def __enter__(self):
    delay_power = self.BASE_DELAY
    cf = self.cfconn
    do_insert = True
    while True:
      if do_insert:
        cf.insert(self.lockname,
                  {self.uid: time.time()},
                  ttl=self.MAX_LOCKTIME,
                  write_consistency_level=ConsistencyLevel.QUORUM)
      try:
        contenders = cf.get(self.lockname,
                            column_count=1000,
                            include_timestamp=True,
                            read_consistency_level=ConsistencyLevel.QUORUM)
      except pycassa.NotFoundException:
        # rare: but it can happen
        do_insert = True
        continue
      if len(contenders) == 1:
        break
      # collision! decide if I am #1 or do exponential backoff
      contenders = contenders.items()
      contenders.sort(key= lambda x: (x[1][1], x[0]))
      names = [item[0] for item in contenders]
      try:
        position = names.index(self.uid) + 1
      except ValueError:
        # rare: retry the insert if we didn't see our own write
        position = max(len(names), 2)
      if position == 1:
        # short-sleep, busy-test
        do_insert = False
        time.sleep(self.SHORT_SLEEP)
      else:
        # i lose, sleep proportional to my position in the list
        self.collisions += 1
        cf.remove(self.lockname,
                  columns=[self.uid],
                  write_consistency_level=ConsistencyLevel.QUORUM)
        delay_s = delay_power * ((position + self.collisions) ** 0.5) + (random.random() * self.collisions)
        print 'Sleeping %.1f sec for coll #%d at position %d' % (delay_s, self.collisions, position)
        time.sleep(delay_s)
    return self

  def __exit__(self, *args, **kwargs):
    # release lock!
    self.cfconn.remove(self.lockname,
                       columns=[self.uid],
                       write_consistency_level=ConsistencyLevel.QUORUM)


def test_locks():
  now = time.time()
  start = (int(now / 10) + 1) * 10
  left = start -now
  print 'Starting in %.3f sec' % (left)
  time.sleep(left)
  now = time.time()
  with CassLock('MyLock3') as clk:
    elapsed = time.time() - now
    print clk.uid, ':  *** GOT LOCK! after', clk.collisions, 'collisions and',
    print ' %.3f seconds' % elapsed
    

# ------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
  import pycassa, pycassa.pool
  from pycassa.cassandra.ttypes import ConsistencyLevel

  SERVERS = ['monterra:9160',
             'monterra:9164',
             'monterra:9165',
             'monterra:9170',
             'monterra:9180',
             ]
  random.shuffle(SERVERS)
  time.sleep(random.random() * 3)
  CassLock.setup(SERVERS)
  print 'Connected!'
  test_locks()
