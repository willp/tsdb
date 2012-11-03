#
# Autogenerated by Thrift Compiler (0.9.0-dev)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:new_style,dynamic
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException

from thrift.protocol.TBase import TBase, TExceptionBase


class MClass(TBase):
  GAUGE = 1
  RATE = 2
  ENUM = 3
  SCORE = 4
  SCORE_LIMITED = 5
  PERCENT = 6

  _VALUES_TO_NAMES = {
    1: "GAUGE",
    2: "RATE",
    3: "ENUM",
    4: "SCORE",
    5: "SCORE_LIMITED",
    6: "PERCENT",
  }

  _NAMES_TO_VALUES = {
    "GAUGE": 1,
    "RATE": 2,
    "ENUM": 3,
    "SCORE": 4,
    "SCORE_LIMITED": 5,
    "PERCENT": 6,
  }

class MDirection(TBase):
  IN = 1
  OUT = 2

  _VALUES_TO_NAMES = {
    1: "IN",
    2: "OUT",
  }

  _NAMES_TO_VALUES = {
    "IN": 1,
    "OUT": 2,
  }


class InvalidSample(TExceptionBase):
  """
  Attributes:
   - what
   - why
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'what', None, None, ), # 1
    (2, TType.STRING, 'why', None, None, ), # 2
  )

  def __init__(self, what=None, why=None,):
    self.what = what
    self.why = why

  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class NodeType(TBase):
  """
  Attributes:
   - id
   - name
   - attrs
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'id', None, None, ), # 1
    (2, TType.STRING, 'name', None, None, ), # 2
    (3, TType.LIST, 'attrs', (TType.STRING,None), None, ), # 3
  )

  def __init__(self, id=None, name=None, attrs=None,):
    self.id = id
    self.name = name
    self.attrs = attrs

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Metric(TBase):
  """
  Attributes:
   - name
   - mtype
   - mclass
   - units
   - description
   - display_name
   - direction
   - enum_map
   - min_value
   - max_value
   - doc_url
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'name', None, None, ), # 1
    (2, TType.STRING, 'mtype', None, None, ), # 2
    (3, TType.I32, 'mclass', None, None, ), # 3
    (4, TType.STRING, 'units', None, None, ), # 4
    (5, TType.STRING, 'description', None, None, ), # 5
    None, # 6
    None, # 7
    None, # 8
    None, # 9
    (10, TType.STRING, 'display_name', None, None, ), # 10
    (11, TType.I32, 'direction', None, None, ), # 11
    (12, TType.MAP, 'enum_map', (TType.I32,None,TType.STRING,None), None, ), # 12
    (13, TType.DOUBLE, 'min_value', None, None, ), # 13
    (14, TType.DOUBLE, 'max_value', None, None, ), # 14
    (15, TType.STRING, 'doc_url', None, None, ), # 15
  )

  def __init__(self, name=None, mtype=None, mclass=None, units=None, description=None, display_name=None, direction=None, enum_map=None, min_value=None, max_value=None, doc_url=None,):
    self.name = name
    self.mtype = mtype
    self.mclass = mclass
    self.units = units
    self.description = description
    self.display_name = display_name
    self.direction = direction
    self.enum_map = enum_map
    self.min_value = min_value
    self.max_value = max_value
    self.doc_url = doc_url

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
