#
# tsdb data API
#

exception InvalidSample {
  1: string what
  2: string why
}
exception InvalidMetric {
  1: string what
  2: string why
}
exception AlreadyExists {
  1: string what
  2: string why
}
exception DoesNotExist {
  1: string what
  2: string why
}

enum CannedValids {
  IP = 1
  IPV4 = 2
  IPV6 = 3
  MAC_ADDRESS = 4  /* accept any known mac address formatting */
  MAC_ADDRESS_STRICT = 5 /* only hyphen-delimited uppercase MAC addresses, 00-01-C7-01-02-03 */
  FQDN = 6
  FQDNR = 7  /* resolvable FQDN */
  FQDN_COLON_PORT = 8
  FQDNR_COLON_PORT = 9  /* resolvable FQDN, valid port range */
  IP_COLON_PORT = 10
  IPV4_COLON_PORT = 11
  IPV6_COLON_PORT = 12
  
  /* system type strings */
  ABSOLUTE_PATH = 13
  RELATIVE_PATH = 14
  WINDOWS_PATH = 15
  DOTTED_STRING = 16  /* requires at least one '.' dot in the string */

  /* some handy well-known string types */
  EMAIL_ADDRESS = 17
  URL = 18
  IP_CIDR_SLASH = 19
  CLLI_CODE = 20
  LAST_COMMA_FIRST_NAMES = 21  /*  von Public, Joan Q.   (no digits) */
}

struct Validator {
  1: optional CannedValids canned

  /* additional validators */
  2: bool canned_first = false    /* when true, do canned validator before these */
  3: optional list<string> match_regex  /* match ANY regex, tested in order */
  4: i32 min_len = 1
  5: optional i32 max_len
  6: optional string invalid_chars
}

enum CannedNorms {
  RESOLVE_IP_TO_FQDN = 1   /* turn IPs into FQDNs */
  RESOLVE_FQDN_TO_IP = 2   /* turn FQDNs into IPs */
  
  /* Handle known MAC address formats, including linux, windows, solaris and cisco formats */
  MAC_ADDRESS_UPPER_COLONS = 3   /* uppercase colon-delimited */
  MAC_ADDRESS_LOWER_COLONS = 4   /* lowercase colon-delimited */
  MAC_ADDRESS_UPPER_HYPHENS = 5  /* uppercase hyphen-delimited */
  MAC_ADDRESS_LOWER_HYPHENS = 6  /* lowercase hyphen-delimited */  

  /* Phone number types */
  US_PHONE = 7              /* (202)555-1212 */
  US_PHONE_HYPHENS = 8      /* 202-555-1212 */
  US_INTL_PHONE = 9         /* +1 2025551212 */  
}

struct Normalizer {
  1: optional CannedNorms canned
  
  /* additional normalizers run BEFORE Canned normalizer */
  2: bool canned_first = false  /* when true, do canned normalizer before these */

  /* phase 1, in order */
  3: bool uri_decode = false
  4: bool strip_whitespace_lr = true   /* trim leading and trailing whitespace */
  5: bool strip_whitespace = false
  6: bool strip_nonalphanumeric = false

  /* phase 2, in order */
  7: optional bool to_camelcase  /* requires strip_whitespace = false */
  8: optional bool whitespace_to_underscore
  9: optional bool to_lowercase
  10: optional bool to_uppercase

  /* phase 3 */
  11: optional list<string> regex_match /* regex, using match group(), first to succeed wins */
  12: optional string match_group_name  /* applies to ALL regexes in list, otherwise groups[0] is used */
}

struct Attr {
  1: required string name
  2: required string description
  3: optional Validator validator
  4: optional Normalizer normalizer
}

struct NodeType {
  1: i32 id
  2: string name
  3: list<string> attrs
  4: string creator
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

struct MOptions {
  /* still thinking about this... */
  1: optional MDirection direction
  2: optional double min_value
  3: optional double max_value
  4: optional string doc_url
}

struct Metric {
  /* Unique Identifier: e.g.  InBits/Sec, InBits/Sec.Avg, InBits/Sec.Max */
  1: required string name

  /* Mandatory definitions (non-unique) */
  2: required MClass mclass        /* one of MClass: GAUGE, RATE, ENUM, SCORE, SCORE_LIMITED, PERCENT */
  3: required string units         /* quantitative units: i.e   Bits/Second, Degrees Celsius, Percent Utilization */
  4: required string description   /* 1 sentence description of metric's meaning */
  5: required string creator       /* email address/username of creator */

  /* Optional metadata, only provided if applicable */
  10: optional string display_name      /* alternate string to render to users */
  11: optional string doc_url           /* URL for metric documentation        */
  12: optional MDirection direction     /* direction, from node perspective    */
  13: optional double min_value         /* minimum permitted value (inclusive) */
  14: optional double max_value         /* maximum permitted value (inclusive) */
  15: optional map<i32,string> enum_map /* map enum values to string names     */
}

service TSPublish {

  bool CreateAttr(1: Attr attr
                 ) throws (1: AlreadyExists exc);
  
  bool CreateNodeType(1: string nodetype,
                      2: list<string> attrs,
                      3: string creator
                     ) throws (1: AlreadyExists exc1,
                               2: DoesNotExist exc2);

  NodeType GetNodeTypeById(1: i32 nt_id) throws(1: DoesNotExist exc);
  NodeType GetNodeTypeByName(1: string name) throws(1: DoesNotExist exc);
  
  map<i32,NodeType> GetNodeTypes();

  list<string> GetNodeTypeNames();

  i32 CreateMetricGauge(1: string name
                        2: string units
                        3: string description
                        4: string creator
                        /* 5: MOptions options */
                       ) throws (1: AlreadyExists exc);
  i32 CreateMetricRate(1: string name
                       2: string units
                       3: string description
                       4: string creator
                       /* 5: MOptions options */
                      ) throws (1: AlreadyExists exc);
  i32 CreateMetricEnum(1:string name,
                       2: string units
                       3: string description
                       4: map<i32,string> enum_map
                       5: string creator
                       /* 6: MOptions options */
                      ) throws (1: AlreadyExists exc);

  map<i32,Metric> GetMetrics();
  
  void Store(1: string nodetype
             2: list<string> attrs
             3: double timestamp
             4: double value
             5: i32 ttl
            ) throws (1: InvalidSample exc);

  void StoreNumeric(1: i32 nodetype_id,
                    2: list<i32> attr_ids
                    3: double timestamp
                    4: double value
                    5: i32 ttl
                   ) throws (1: InvalidSample exc);

  /* Rates must be computed over a window of time, so we include delta-T as duration_sec field */
  /* TODO: IMPLEMENT THIS, AND THINK IT THROUGH */
  void StoreRate(1: string nodetype
                 2: list<string> attrs
                 3: double timestamp      /* recent-most timestamp used for rate  */
                 4: double duration_sec   /* this is delta-T, in units of seconds */
                 5: double value
                 6: i32 ttl
                ) throws (1: InvalidSample exc);

  void StoreBulk(1: string nodetype
                 2: list<string> attrs
                 3: map<double,double> values
                 4: i32 ttl
                ) throws (1: InvalidSample exc);

}
