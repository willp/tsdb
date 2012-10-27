#
# tsdb data publishing API
#

struct Sample {
    1: string nodetype
    2: map<string,string> attrs
    3: string metric
    4: double timestamp
    5: double value
}

exception InvalidSample {
    1: string what
    2: string why
}

service TSPublish {

  bool CreateNodeType(1: list<string> attrs);

  void Store(1: Sample data) throws (1: InvalidSample exc);

  void StoreBulk(1: list<Sample> data) throws (1: InvalidSample exc);
}
