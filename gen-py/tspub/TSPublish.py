#
# Autogenerated by Thrift Compiler (1.0.0-dev)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:new_style,dynamic
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.protocol.TBase import TBase, TExceptionBase


class Iface(object):
  def CreateNodeType(self, attrs):
    """
    Parameters:
     - attrs
    """
    pass

  def Store(self, data):
    """
    Parameters:
     - data
    """
    pass

  def StoreBulk(self, data):
    """
    Parameters:
     - data
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def CreateNodeType(self, attrs):
    """
    Parameters:
     - attrs
    """
    self.send_CreateNodeType(attrs)
    return self.recv_CreateNodeType()

  def send_CreateNodeType(self, attrs):
    self._oprot.writeMessageBegin('CreateNodeType', TMessageType.CALL, self._seqid)
    args = CreateNodeType_args()
    args.attrs = attrs
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_CreateNodeType(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = CreateNodeType_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    raise TApplicationException(TApplicationException.MISSING_RESULT, "CreateNodeType failed: unknown result");

  def Store(self, data):
    """
    Parameters:
     - data
    """
    self.send_Store(data)
    self.recv_Store()

  def send_Store(self, data):
    self._oprot.writeMessageBegin('Store', TMessageType.CALL, self._seqid)
    args = Store_args()
    args.data = data
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_Store(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = Store_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.exc is not None:
      raise result.exc
    return

  def StoreBulk(self, data):
    """
    Parameters:
     - data
    """
    self.send_StoreBulk(data)
    self.recv_StoreBulk()

  def send_StoreBulk(self, data):
    self._oprot.writeMessageBegin('StoreBulk', TMessageType.CALL, self._seqid)
    args = StoreBulk_args()
    args.data = data
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_StoreBulk(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = StoreBulk_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.exc is not None:
      raise result.exc
    return


class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["CreateNodeType"] = Processor.process_CreateNodeType
    self._processMap["Store"] = Processor.process_Store
    self._processMap["StoreBulk"] = Processor.process_StoreBulk

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_CreateNodeType(self, seqid, iprot, oprot):
    args = CreateNodeType_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = CreateNodeType_result()
    result.success = self._handler.CreateNodeType(args.attrs)
    oprot.writeMessageBegin("CreateNodeType", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_Store(self, seqid, iprot, oprot):
    args = Store_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = Store_result()
    try:
      self._handler.Store(args.data)
    except InvalidSample as exc:
      result.exc = exc
    oprot.writeMessageBegin("Store", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_StoreBulk(self, seqid, iprot, oprot):
    args = StoreBulk_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = StoreBulk_result()
    try:
      self._handler.StoreBulk(args.data)
    except InvalidSample as exc:
      result.exc = exc
    oprot.writeMessageBegin("StoreBulk", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class CreateNodeType_args(TBase):
  """
  Attributes:
   - attrs
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'attrs', (TType.STRING,None), None, ), # 1
  )

  def __init__(self, attrs=None,):
    self.attrs = attrs

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class CreateNodeType_result(TBase):
  """
  Attributes:
   - success
  """

  thrift_spec = (
    (0, TType.BOOL, 'success', None, None, ), # 0
  )

  def __init__(self, success=None,):
    self.success = success

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Store_args(TBase):
  """
  Attributes:
   - data
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'data', (Sample, Sample.thrift_spec), None, ), # 1
  )

  def __init__(self, data=None,):
    self.data = data

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Store_result(TBase):
  """
  Attributes:
   - exc
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'exc', (InvalidSample, InvalidSample.thrift_spec), None, ), # 1
  )

  def __init__(self, exc=None,):
    self.exc = exc

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class StoreBulk_args(TBase):
  """
  Attributes:
   - data
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'data', (TType.STRUCT,(Sample, Sample.thrift_spec)), None, ), # 1
  )

  def __init__(self, data=None,):
    self.data = data

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class StoreBulk_result(TBase):
  """
  Attributes:
   - exc
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'exc', (InvalidSample, InvalidSample.thrift_spec), None, ), # 1
  )

  def __init__(self, exc=None,):
    self.exc = exc

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
