/// Default codec matchers - ordered list of all built-in type codecs

import postgleam/codec.{type CodecMatcher}
import postgleam/codec/array
import postgleam/codec/bit_string
import postgleam/codec/bool
import postgleam/codec/box
import postgleam/codec/bytea
import postgleam/codec/circle
import postgleam/codec/date
import postgleam/codec/float4
import postgleam/codec/float8
import postgleam/codec/inet
import postgleam/codec/int2
import postgleam/codec/int4
import postgleam/codec/int8
import postgleam/codec/interval
import postgleam/codec/json
import postgleam/codec/jsonb
import postgleam/codec/jsonpath
import postgleam/codec/line
import postgleam/codec/lseg
import postgleam/codec/macaddr
import postgleam/codec/macaddr8
import postgleam/codec/money
import postgleam/codec/name
import postgleam/codec/numeric
import postgleam/codec/oid_codec
import postgleam/codec/path
import postgleam/codec/point
import postgleam/codec/polygon
import postgleam/codec/registry
import postgleam/codec/text
import postgleam/codec/time
import postgleam/codec/timestamp
import postgleam/codec/timestamptz
import postgleam/codec/timetz
import postgleam/codec/uuid
import postgleam/codec/void
import postgleam/codec/xml

/// Returns the default list of codec matchers
pub fn matchers() -> List(CodecMatcher) {
  [
    bool.matcher(),
    int2.matcher(),
    int4.matcher(),
    int8.matcher(),
    float4.matcher(),
    float8.matcher(),
    text.matcher(),
    bytea.matcher(),
    uuid.matcher(),
    oid_codec.matcher(),
    name.matcher(),
    void.matcher(),
    date.matcher(),
    time.matcher(),
    timetz.matcher(),
    timestamp.matcher(),
    timestamptz.matcher(),
    interval.matcher(),
    json.matcher(),
    jsonb.matcher(),
    numeric.matcher(),
    point.matcher(),
    inet.matcher(),
    macaddr.matcher(),
    macaddr8.matcher(),
    money.matcher(),
    xml.matcher(),
    jsonpath.matcher(),
    bit_string.matcher(),
    line.matcher(),
    lseg.matcher(),
    box.matcher(),
    path.matcher(),
    polygon.matcher(),
    circle.matcher(),
  ]
}

/// Build the full registry including scalar codecs and array codecs.
/// Array codecs are post-registered because they need element codecs
/// from the base registry.
pub fn build_registry() -> registry.Registry {
  let base = registry.build(matchers())
  array.register_arrays(base)
}
