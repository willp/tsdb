#!/usr/bin/python
import random, sys, time
import socket, threading

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
  
  ret = client.CreateNodeType('thr_stats4', ['srvr', 'method', 'metric'])
  print 'Ret returned:', ret

  ret = client.CreateMetric('Calls/Sec', 'raw')
  print 'Made metric:', ret

  samp = Sample(nodetype='thr_stats4',
                attrs=['MyServer123', 'Operation', 'Calls/Sec#raw'],
                timestamp=time.time(),
                value=random.random()
               )
  
  print 'Publishing datapoint:', samp
  
  client.Store(samp)
  print 'Done!'
