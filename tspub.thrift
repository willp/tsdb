#
# tsdb data API
#

exception InvalidSample {
    1: string what
    2: string why
}

struct NodeType {
  1: i32 id
  2: string name
  3: list<string> attrs
}

enum MClass {
  GAUGE = 1         /* instantaneous observed value    */
  RATE = 2          /* rate = dV / dT, may be scaled   */
  ENUM = 3          /* states enumerated with integers */
  SCORE = 4         /* dimensionless values, unbounded */
  SCORE_LIMITED = 5 /* dimensionless, bounded          */
  PERCENT = 6       /* special case of SCORE_LIMITED   */
}

struct Metric {
  1: string name
  2: string mtype /* type of post-processing, default "raw" means directly sampled */
  3: MClass mclass
  4: string units
  5: string description
  
  /* Optional metadata */
  10: string display_name      /* alternate string to render to users */
  11: map<i32,string> enum_map /* map enum values to string names     */
  12: double min_value         /* minimum permitted value (inclusive) */
  13: double max_value         /* maximum permitted value (inclusive) */
}

service TSPublish {

  bool CreateNodeType(1: string nodetype, 2: list<string> attrs);

  i32 CreateMetric(1: string name, 2: string mtype, 3: MClass mclass);

  map<i32,NodeType> GetNodeTypes();

  list<Metric> GetMetrics();
  
  void Store(1: string nodetype
             2: list<string> attrs
             3: double timestamp
             4: double value
            ) throws (1: InvalidSample exc);

  void StoreBulk(1: string nodetype
                 2: list<string> attrs
                 3: map<double,double> values
                ) throws (1: InvalidSample exc);
}
