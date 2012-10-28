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

  lst = client.GetNodeTypes()
  print 'Nodetypes:'
  for nt in lst:
    print '  ', nt

  ret = client.CreateNodeType('test0', ['a0', 'metric'])
  print 'Ret returned:', ret
  time.sleep(5)

  ret = client.CreateMetric('Calls/Sec', 'raw', MClass.GAUGE)
  print 'Made metric:', ret

  # Rewind 1 hour of data
  now = time.time()
  ts = now - 3600 * 24
  data = {}
  while ts < now:
    data[ts] = random.random()
    ts += 55.0 + random.random() * 5.0
  print 'Publishing %d datapoints' % (len(data))
  start = time.time()
  client.StoreBulk(nodetype='test0', attrs=['MyServer130', 'Calls/Sec#raw'], values=data)
  elapsed = time.time() - start
  print 'Done!   %.1f sec  or %.1f samp/sec' % (elapsed, len(data) / elapsed)
