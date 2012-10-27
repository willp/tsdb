#!/usr/bin/python

class Node(object):
  __slots__ = ('children', 'leaves')

  def __init__(self):
    self.children = {}
    self.leaves = set()

  def add(self, attrs):
    attr = attrs[0]
    num_attrs = len(attrs)
    if num_attrs == 1:
      self.leaves.add(attr)
      return
    n = self.children.get(attr, None)
    if n is None:
      n = self.children[attr] = Node()
    if len(attrs) > 1:
      n.add(attrs[1:])

  def walk(self, parent_str='N'):
    if not self.children:
      if self.leaves:
        # TODO: set nav entries here!
        # in the form:  "N0:parent" => [ self.children.keys() ] = time.time()
        #print '%s  Leaves: %r' % (parent, self.leaves)
        ###print '%s  Leaf-Cols: %r' % (parent_str, sorted(self.leaves))
        yield (parent_str, self.leaves)
      return
    # TODO: set nav entries here!
    # in the form:  "N0:parent" => [ self.children.keys() ] = time.time()
    ###print '%s  Cols: %r' % (parent_str, sorted(self.children.keys()))
    yield (parent_str, self.children.keys())
    for child, node in self.children.iteritems():
      for row in node.walk(parent_str + '-%s' % child):
        yield row

class NodeTreeCount(object):
  """One per nodetype which accumulates paths
  """
  def __init__(self, nodetype):
    self.nodetype = nodetype
    self.root = Node()

  def add(self, attrs):
    self.root.add(attrs)

  def walk(self):
    return self.root.walk()

# TODO: add unit tests
