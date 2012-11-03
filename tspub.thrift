#
# tsdb data API
#

exception InvalidSample {
    1: string what
    2: string why
}

exception NodeTypeExists {
  1: string what
  2: string why
}
exception MetricExists {
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

enum MDirection {
  IN = 1
  OUT = 2
}

struct Metric {
  /* Unique Identifier: (name, mtype) */
  1: required string name
  2: required string mtype /* type of post-processing, default "raw" means directly sampled */

  /* Mandatory definitions (non-unique) */
  3: required MClass mclass
  4: required string units
  5: required string description

  /* Optional metadata, only provided if applicable */
  10: optional string display_name      /* alternate string to render to users */
  11: optional MDirection direction     /* direction, from node perspective    */
  12: optional map<i32,string> enum_map /* map enum values to string names     */
  13: optional double min_value         /* minimum permitted value (inclusive) */
  14: optional double max_value         /* maximum permitted value (inclusive) */
  15: optional string doc_url           /* URL for metric documentation        */
}

service TSPublish {

  bool CreateNodeType(1: string nodetype, 2: list<string> attrs) throws (1: NodeTypeExists exc_nte);

  i32 CreateMetric(1: string name, 2: string mtype, 3: MClass mclass) throws (1: MetricExists exc_me);

  map<i32,NodeType> GetNodeTypes();

  map<i32,Metric> GetMetrics();
  
  void Store(1: string nodetype
             2: list<string> attrs
             3: double timestamp
             4: double value
            ) throws (1: InvalidSample exc);

  /* Rates must be computed over a window of time, so we include delta-T as duration_sec field */
  /* TODO: IMPLEMENT THIS, AND THINK IT THROUGH */
  void StoreRate(1: string nodetype
                 2: list<string> attrs
                 3: double timestamp      /* recent-most timestamp used for rate  */
                 4: double duration_sec   /* this is delta-T, in units of seconds */
                 5: double value
                ) throws (1: InvalidSample exc);

  void StoreBulk(1: string nodetype
                 2: list<string> attrs
                 3: map<double,double> values
                ) throws (1: InvalidSample exc);
}
