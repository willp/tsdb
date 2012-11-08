#!/usr/bin/python
import random, socket, sys, threading, time

sys.path.insert(0,  'gen-py')

from tspub import TSPublish
from tspub.ttypes import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

if __name__ == '__main__':
  
  # Start up client
  PORT = 1974
  pfactory = TBinaryProtocol.TBinaryProtocolFactory()
  sock = TSocket.TSocket('localhost', PORT)
  sock.open()
  transport = TTransport.TFramedTransport(sock)
  protocol = pfactory.getProtocol(transport)
  client = TSPublish.Client(protocol)

  lst = client.GetNodeTypeNames()
  print 'Nodetypes:', lst
  for nt, nt_attrs in (('test0', ('a0', 'metric')),
                       ('test1', ('a0', 'a1', 'metric')),
                       ('test2', ('a0', 'a1', 'a2', 'metric'))
                       ):
    if nt not in lst:
      print 'Building "%s" nodetype' % nt
      ret = client.CreateNodeType(nt, nt_attrs, 'creator@example.com')
      print 'Ret returned:', ret
      time.sleep(5)

  METRIC = 'ActiveOperations'
  ret = client.CreateMetricGauge(name=METRIC,
                                 units='Operations',
                                 description='The current number of active Operations currently executing',
                                 creator='Email_Address@example.com')
  print 'Made metric:', ret

  # Rewind 1 hour of data
  test_start = now = time.time()
  test_count = 0
  for server_id in xrange(100):
    ts = now - 60 * 60 * 24 * 14
    data = {}
    while ts < now:
      data[ts] = random.random()
      ts += 55.0 + random.random() * 5.0
    start = time.time()
    for server_letter in 'ABCDEFGHI':
      print '[%3d:%s] Publishing %d datapoints' % (server_id, server_letter, len(data))
      client.StoreBulk(nodetype='test2',
                       attrs=['MyServer', 'POD:0:%s' % server_letter, '%03d' % (server_id), METRIC],
                       values=data,
                       ttl=300)
      test_count += len(data)
      elapsed = time.time() - test_start
      print 'Done!   %.1f sec  or %.1f samp/sec' % (elapsed, test_count / elapsed)
