#!/usr/bin/python
import collections, copy, itertools, logging, random, socket, sys, threading, time
import pycassa, pycassa.connection, pycassa.pool
from pycassa.cassandra.ttypes import ConsistencyLevel

from casslock import CassLock
from nodetree import NodeTreeCount

sys.path.insert(0,  'gen-py')
from tspub import TSPublish
from tspub.ttypes import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

SERVERS = ['vm%3d' % num for num in range(101, 106)]  # TODO: load from config file
random.shuffle(SERVERS)
CassLock.setup(SERVERS)

# (X) 1. start with strings
# (X) 2. dont shard keys on time yet
# (X) 3. store nav data
# (X) 4. store series data
# (X) 5. convert to integer IDs
# 6. implement search
# 7. implement retrieval
# 8. implement bulk export

KEYSPACE = 'tsdata2'
CF_META = 'meta_2'
CF_NT = 'nodetype_defs_2'

ATTR_MAP = collections.defaultdict(dict)
RECENT_NT_ATTRS = collections.defaultdict(dict)
NAV_MAP = dict()
LAST_FLUSH = time.time()


def setup_cass():
  import pycassa.system_manager as sysmgr
  csys = sysmgr.SystemManager(SERVERS[0])
  ret = setup_tsdata(csys)
  print 'setup done'
  return ret

def setup_tsdata(csys):
  cf_opts = dict(
                 read_repair_chance=0.0001,
                 min_compaction_threshold=8,
                 max_compaction_threshold=20,
                 compaction_strategy='SizeTieredCompactionStrategy',
                 compression_options=dict(sstable_compression='DeflateCompressor')
                )
  meta_opts = dict(
                   read_repair_chance=0.01,
                   min_compaction_threshold=3,
                   max_compaction_threshold=10,
                   compaction_strategy='SizeTieredCompactionStrategy',
                   compression_options=dict(sstable_compression='')
                  )
  keyspaces = csys.list_keyspaces()
  print 'Keyspaces:', keyspaces
  if KEYSPACE not in keyspaces:
    ret = csys.create_keyspace(KEYSPACE,
                               replication_strategy='SimpleStrategy',
                               strategy_options=dict(replication_factor='3'),
                               durable_writes=False)
    print 'Got result:', ret
  cfs = csys.get_keyspace_column_families(KEYSPACE, use_dict_for_col_metadata=True)
  print 'Column families in keyspace %s:  %s' % (KEYSPACE, cfs)
  if CF_META not in cfs:
    # used for attribute auto-increment
    csys.create_column_family(KEYSPACE, CF_META,
                              comparator_type='AsciiType',
                              default_validation_class='IntegerType',
                              key_validation_class='AsciiType',
                              **meta_opts
                              )
  if CF_NT not in cfs:
    # used for nodetypes
    csys.create_column_family(KEYSPACE, CF_NT,
                              comparator_type='IntegerType',
                              default_validation_class='AsciiType',
                              key_validation_class='AsciiType',
                              **meta_opts
                              )    
  return csys

def incr_attr(cf, attrname, attrval, overfetch=0):
  created = False
  try:
    attr = cf.get(attrname,
                  column_count=overfetch + 1,
                  column_start=attrval,
                  read_consistency_level=ConsistencyLevel.ONE)
    if attrval not in attr:
      print 'NOT PRESENT!'
      raise pycassa.NotFoundException('Not there!')
    attr_id = attr[attrval]
    print 'Already "%s" exists, attr ID: %d' % (attrval, attr_id)
    if overfetch:
      return attr_id, created, attr
    return attr_id, created
  except pycassa.NotFoundException:
    print 'No attr exists for %s/ %s' % (attrname, attrval)
  with CassLock(attrname + '//lock') as clk:
    print 'Adding new %s: %s' % (attrname, attrval)
    try:
      attr = cf.get(attrname,
                    column_count=1,
                    columns=[attrval],
                    read_consistency_level=ConsistencyLevel.QUORUM)
      attr_id = attr[attrval]
      print 'Found attr ID: %d' % attr_id
      return attr_id, created
    except pycassa.NotFoundException:
      # get next attr ID
      try:
        # TODO: optimize this! O(N) on attr cardinality, use a reverse map of ID to attr
        all_attrs = cf.get(attrname,
                           column_count=1000*1000,
                           read_consistency_level=ConsistencyLevel.QUORUM)
      except pycassa.NotFoundException:
        all_attrs = []
      if len(all_attrs) == 0:
        attr_id = 1
      else:
        print 'Got %d attrs' % len(all_attrs)
        # compute max metric
        # TODO: make this more efficient, see line above
        max_attr_id = max(all_attrs.itervalues())
        print 'Max attr ID is:', max_attr_id
        attr_id = max_attr_id + 1
      cf.insert(attrname,
                {attrval: attr_id},
                write_consistency_level=ConsistencyLevel.QUORUM)
      created = True
      # TODO: add a reverse mapping from ID to metric name
      # (consider using a native secondary index)
      print 'Inserted attr ID:', attr_id, 'for attr:', attrval
  print 'Got attr id:', attr_id
  if overfetch:
    return attr_id, created, {}
  return attr_id, created

NT_ATTRS = {}  # {"nodetypestr": ("attr0", "attr1", "attr2", ...)}
NT_IDS = {}  # {"nodetypestr": <int:nodetype_id>, ...}
NT_IDS_REV = {} # { <int:nodetype_id>: "nodetypestr", ...}

def create_nodetype(csys, pool, mcf, nodetype, attrs):
  print 'Building nodetype %s with attrs: %r' % (nodetype, attrs)
  assert len(attrs) == len(set(attrs))  # no dupes
  # First, need to define the nodetype as a list of attrs
  # as:
  # 1. get or create new nodetype_id using CF_META on "nodetype" attr
  #  now we have a nodetype ID value
  # 2. add <nodetype>(str) rowkey in CF_NT
  #   column(int) are ordinals of attrs, values(str) are attr names, last one is "metric"
  # 3. add column to attr_<attr>_nodetypes rowkey in CF_META?
  #    column(str) is <nodetype>  value(int) is ordinal position?
  # 4. TODO: add attribute transformer, validator, etc as properties in
  #   another rowkey/CF which also acts as the attr "namespace" registry
  # Create the following column families:
  #   nav_<nodetype> - for tree /navigation (canonical order)
  #   ts_<nodetype> - for timeseries data
  #   TODO: SHARD column family by TIME to PARTITION data on-disk by TIME
  assert attrs[-1] == 'metric'
  nt_id, new = incr_attr(mcf, 'nodetype', nodetype)
  if new:
    print 'Added nodetype to registry of all nodetypes.'
    print 'Made new nodetype ID: %d for nodetype "%s"' % (nt_id, nodetype)
  else:
    print 'already SET! (%d)' % nt_id
    # TODO: validate nodetype really exists
    #return nt_id
  nt_cf = pycassa.ColumnFamily(pool, CF_NT)
  print 'Adding attributes %r to "%s" col-family' % (attrs, CF_NT)
  attr_dict = dict(enumerate(attrs))  # {0:attr0, 1:attr1, 2:attr2, ...}
  try:
    ret = nt_cf.get(nodetype, column_count=100, read_consistency_level=ConsistencyLevel.QUORUM)
    print 'already SET! (%r)' % ret
  except pycassa.NotFoundException:
    print 'Inserting NEW nodetype row', nodetype, attr_dict
    nt_cf.insert(nodetype,
                 attr_dict,
                 write_consistency_level=ConsistencyLevel.QUORUM)
  for a_idx, attr in attr_dict.iteritems():
    rowkey = 'attr_%s_nodetypes' % attr
    print 'Updating attr -> nodetype map: %s = %s' % (rowkey, nodetype)
    try:
      ret = mcf.get(rowkey, column_count=100, read_consistency_level=ConsistencyLevel.QUORUM)
      print '  row %s already SET! (%r)' % (rowkey, ret)
    except pycassa.NotFoundException:
      print '  add %s = "{%s: %d}"' % (rowkey, nodetype, a_idx)
      mcf.insert(rowkey,
                 {nodetype: a_idx},
                 write_consistency_level=ConsistencyLevel.QUORUM)
  CF_NAV = 'nav_' + nodetype
  CF_TS = 'ts_' + nodetype
  cfs = csys.get_keyspace_column_families(KEYSPACE, use_dict_for_col_metadata=True)
  #
  cf_opts = dict(
                 read_repair_chance=0.0001,
                 min_compaction_threshold=8,
                 max_compaction_threshold=20,
                 compaction_strategy='SizeTieredCompactionStrategy',
                 compression_options=dict(sstable_compression='DeflateCompressor')
                )
  meta_opts = dict(
                   read_repair_chance=0.01,
                   min_compaction_threshold=3,
                   max_compaction_threshold=10,
                   compaction_strategy='SizeTieredCompactionStrategy',
                   compression_options=dict(sstable_compression='')
                  )
  #
  if CF_TS not in cfs:
    print 'Creating TS col-family "%s"' % CF_TS
    csys.create_column_family(KEYSPACE, CF_TS,
                              comparator_type='IntegerType',
                              default_validation_class='FloatType',
                              key_validation_class='AsciiType',
                              **cf_opts
                              )
  else:
    print 'already SET!'
  if CF_NAV not in cfs:
    print 'Create NAV col-family "%s"' % CF_NAV
    csys.create_column_family(KEYSPACE, CF_NAV,
                              comparator_type='IntegerType',
                              default_validation_class='IntegerType',
                              key_validation_class='AsciiType',
                              **meta_opts
                              )
  else:
    print 'already SET!'
  print 'DONE!'
  NT_ATTRS[nodetype] = attrs
  NT_IDS[nodetype] = nt_id
  NT_IDS_REV[nt_id] = nodetype
  return nt_id


def process_recent(pool, nodetype):
  print 'Flushing nav for NAV_%s' % (nodetype)
  now = int(time.time())
  ntc = NAV_MAP[nodetype]
  #for attr in recent.iterkeys():
  #  ntc.add(attr)
  nav_cf = pycassa.ColumnFamily(pool, 'nav_' + nodetype)
  count = 0
  start = time.time()
  print 'INSERTING nav rows...'
  with nav_cf.batch(queue_size=2000,
                    write_consistency_level=ConsistencyLevel.ANY
                    ) as nav_cfb:
    for nav_path, attrs in ntc.walk():
      #print 'INSERTING NAV-ROW:', nav_path, list(attrs)
      attr_cols = dict((attr, now) for attr in attrs)
      count += 1
      nav_cfb.insert(nav_path,
                     attr_cols)
  elapsed = time.time() - start
  rate = count / elapsed
  print 'DONE INSERTING %6d NAV ROWS IN %5.1f SECONDS: %6.1f ROWS/SEC!' % (count, elapsed, rate)
  NAV_MAP[nodetype] = NodeTreeCount(nodetype)


class PubHandler(object):
  def __init__(self):
    self.csys = setup_cass()
    self.pool = pycassa.pool.ConnectionPool(KEYSPACE, server_list=SERVERS, timeout=15)
    self.meta_cf = pycassa.ColumnFamily(self.pool, CF_META)
    self.NTs = {}
    self._last_flush = time.time()
    self._load_nodetypes()

  def _load_nodetypes(self):
    nt_cf = pycassa.ColumnFamily(self.pool, CF_NT)
    nt_list = self.meta_cf.get('nodetype', column_count=1000, read_consistency_level=ConsistencyLevel.QUORUM)
    for nodetype, nodetype_id in nt_list.iteritems():
      NT_IDS[nodetype] = nodetype_id
      NT_IDS_REV[nodetype_id] = nodetype
    print 'NT IDs:', NT_IDS
    print 'NT LIST:', nt_list
    ret = nt_cf.get_range(column_count=100, read_consistency_level=ConsistencyLevel.QUORUM)
    for nodetype, attrdict in ret:
      if attrdict.keys() == range(len(attrdict)):
        NT_ATTRS[nodetype] = attrdict.values()
      else:
        pass  # TODO: ignore partial nodetype, log warning, schedulre refresh for later
    print 'NT ATTRS:', NT_ATTRS

  def _publish(self, cf, nodetype, attrs, ts, val, ttl=None):
    pool = self.pool
    mcf = self.meta_cf
    ts = int(ts)
    attr_names = NT_ATTRS[nodetype]
    a_ids = []
    for a_name, attr in itertools.izip(attr_names, attrs):
      attr_id = ATTR_MAP[a_name].get(attr, False)
      if not attr_id:
        (attr_id, new, overfetch) = incr_attr(mcf, 'attr_' + a_name, attr, 100)
        if overfetch:
          print 'Pre-Populating "%s" attr cache with %d extra attrs: %r' % (a_name, len(overfetch), overfetch)
          for o_attr, o_id in overfetch.iteritems():
            ATTR_MAP[a_name][o_attr] = o_id
        else:
          # no extra data in overfetch
          ATTR_MAP[a_name][attr] = attr_id
        if new:
          print 'Made new attr id "%s": %s => %d' % (a_name, attr, attr_id)
      a_ids.append(attr_id)
    key = ','.join(str(attr) for attr in a_ids)
    cf.insert(key,
              {ts: val},
              ttl=ttl,  #86400 * 365 * 4,
             )
    # update local NAV tree
    nt_navmap = NAV_MAP.get(nodetype)
    if nt_navmap is None:
      nt_navmap = NAV_MAP[nodetype] = NodeTreeCount(nodetype)
    nt_navmap.add(a_ids)
    if time.time() - self._last_flush > 90:
      # TODO: change flushing strategy to be global-timer and per-nodetype flush
      # because this will miss flushes of infrequently seen nodetypes
      print 'Flushing recent attrs out to NAV table'
      self._last_flush = time.time()
      process_recent(pool, nodetype)


  def CreateNodeType(self, nodetype, attrs):
    """Build a nodetype with named attributes in the order given
    and update any metadata needed otherwise"""
    assert len(attrs) > 0
    for item in attrs:
      assert len(item) > 0
      assert item == item.lower()
      assert ' ' not in item
    assert nodetype == nodetype.lower()
    assert len(nodetype) > 2
    assert ' ' not in nodetype
    print 'Creating nodetype "%s" with attrs: %r' % (nodetype, attrs)
    create_nodetype(self.csys, self.pool, self.meta_cf, nodetype, attrs)
    print 'Created nodetype "%s"!' % nodetype
    return True

  def CreateMetric(self, name, mtype, mclass):
    # TODO: store mclass and extra per-metric metadata
    assert ' ' not in name
    assert mtype == 'raw'
    mkey = '%s#%s' % (name, mtype)
    metric_id, new = incr_attr(self.meta_cf, 'metrics', mkey)
    return metric_id

  def _get_nt_cf(self, nodetype):
    nt_cf = self.NTs.get(nodetype, None)
    if nt_cf is not None:
      return nt_cf
    self.NTs[nodetype] = nt_cf = pycassa.ColumnFamily(self.pool, 'ts_' + nodetype)
    return(nt_cf)

  def GetNodeTypes(self):
    nts = {}
    for nodetype, attrs in NT_ATTRS.iteritems():
      nt_id = NT_IDS[nodetype]
      nts[nt_id] = NodeType(name=nodetype, attrs=attrs)
    return nts

  def Store(self, nodetype, attrs, timestamp, value):
    """
    """
    cf = self._get_nt_cf(nodetype)
    self._publish(cf, nodetype, attrs, timestamp, value)

  def StoreBulk(self, nodetype, attrs, values):
    """
    """
    cf = self._get_nt_cf(nodetype)
    with cf.batch(queue_size=5000,
                  write_consistency_level=ConsistencyLevel.ANY
                 ) as cfb:
      for ts, val in values.iteritems():
        self._publish(cfb, nodetype, attrs, ts, val)
    print 'batch completed'

  def StoreRate(self, *args):
    raise NotImplementedError('Method not yet implemented')



if __name__ == '__main__':
  # Start up server
  PORT = 1974
  pfactory = TBinaryProtocol.TBinaryProtocolFactory()
  tfactory = TTransport.TFramedTransportFactory()
  handler = PubHandler()
  processor = TSPublish.Processor(handler)
  transport = TSocket.TServerSocket('localhost', PORT)
  server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
  print 'Starting up server on port %d' % PORT
  server.serve()
