#
# tsdb data publishing API
#

struct Sample {
    1: string nodetype
    2: list<string> attrs
    3: string metric
    4: double timestamp
    5: double value
}

struct SampleBulk {
    1: string nodetype
    2: list<string> attrs
    3: string metric
    4: map<double,double> values
}

exception InvalidSample {
    1: string what
    2: string why
}

service TSPublish {

  bool CreateNodeType(1: string nodetype, 2: list<string> attrs);

  bool CreateMetric(1: string name, 2: string mtype);
  
  void Store(1: Sample data) throws (1: InvalidSample exc);

  void StoreBulks(1: list<SampleBulk> batch) throws (1: InvalidSample exc);
}
