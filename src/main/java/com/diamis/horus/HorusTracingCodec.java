package com.diamis.horus;


import io.jaegertracing.internal.JaegerObjectFactory;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.propagation.PrefixedKeys;
import io.jaegertracing.spi.Codec;
import io.opentracing.propagation.TextMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HorusTracingCodec implements Codec<TextMap> {
  protected static final String TRACE_ID_NAME = "XB3TraceId";
  protected static final String SPAN_ID_NAME = "XB3SpanId";
  protected static final String PARENT_SPAN_ID_NAME = "XB3ParentSpanId";
  protected static final String SAMPLED_NAME = "XB3Sampled";
  protected static final String FLAGS_NAME = "XB3Flags";
  protected static final String BAGGAGE_PREFIX = "baggage-";
  // NOTE: uber's flags aren't the same as B3/Finagle ones
  protected static final byte SAMPLED_FLAG = 1;
  protected static final byte DEBUG_FLAG = 2;

  private static final PrefixedKeys keys = new PrefixedKeys();
  private final String baggagePrefix;
  private final JaegerObjectFactory objectFactory;

  /**
   * @deprecated use {@link Builder} instead
   */
  @Deprecated
  public HorusTracingCodec() {
    this(new Builder());
  }

  private HorusTracingCodec(Builder builder) {
    this.baggagePrefix = builder.baggagePrefix;
    this.objectFactory = builder.objectFactory;
  }

  //@Override
  public void inject(JaegerSpanContext spanContext, TextMap carrier) {
    carrier.put(TRACE_ID_NAME, // Use HexCode instead of getTraceId to ensure zipkin compatibility
            HorusTracingCodec.toLowerHex(spanContext.getTraceIdHigh(), spanContext.getTraceIdLow()));
    if (spanContext.getParentId() != 0L) { // Conventionally, parent id == 0 means the root span
      carrier.put(PARENT_SPAN_ID_NAME, HorusTracingCodec.toLowerHex(spanContext.getParentId()));
    }
    carrier.put(SPAN_ID_NAME, HorusTracingCodec.toLowerHex(spanContext.getSpanId()));
    carrier.put(SAMPLED_NAME, spanContext.isSampled() ? "1" : "0");
    if (spanContext.isDebug()) {
      carrier.put(FLAGS_NAME, "1");
    }
    for (Map.Entry<String, String> entry : spanContext.baggageItems()) {
      carrier.put(keys.prefixedKey(entry.getKey(), baggagePrefix), entry.getValue());
    }
  }

  //@Override
  public JaegerSpanContext extract(TextMap carrier) {    
    Long traceIdLow = null;
    Long traceIdHigh = 0L; // It's enough to check for a null low trace id
    Long spanId = null;
    Long parentId = 0L; // Conventionally, parent id == 0 means the root span
    byte flags = 0;
    Map<String, String> baggage = null;
    for (Map.Entry<String, String> entry : carrier) {
      if (entry.getKey().equalsIgnoreCase(SAMPLED_NAME)) {
        String value = entry.getValue();
        if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
          flags |= SAMPLED_FLAG;
        }
      } else if (entry.getKey().equalsIgnoreCase(TRACE_ID_NAME)) {
        traceIdLow = HorusTracingCodec.lowerHexToUnsignedLong(entry.getValue());
        traceIdHigh = HorusTracingCodec.higherHexToUnsignedLong(entry.getValue());
      } else if (entry.getKey().equalsIgnoreCase(PARENT_SPAN_ID_NAME)) {
        parentId = HorusTracingCodec.lowerHexToUnsignedLong(entry.getValue());
      } else if (entry.getKey().equalsIgnoreCase(SPAN_ID_NAME)) {
        spanId = HorusTracingCodec.lowerHexToUnsignedLong(entry.getValue());
      } else if (entry.getKey().equalsIgnoreCase(FLAGS_NAME)) {
        if (entry.getValue().equals("1")) {
          flags |= DEBUG_FLAG;
        }
      } else if (entry.getKey().startsWith(baggagePrefix)) {
        if (baggage == null) {
          baggage = new HashMap<String, String>();
        }
        baggage.put(keys.unprefixedKey(entry.getKey(), baggagePrefix), entry.getValue());
      }
    }

    if (null != traceIdLow && null != parentId && null != spanId) {
      JaegerSpanContext spanContext = objectFactory.createSpanContext(
          traceIdHigh,
          traceIdLow,
          spanId,
          parentId,
          flags,
          Collections.<String, String>emptyMap(),
          null // debugId
          );
      if (baggage != null) {
        spanContext = spanContext.withBaggage(baggage);
      }
      return spanContext;
    }
    return null;
  }

  public static class Builder {
    private String baggagePrefix = BAGGAGE_PREFIX;
    private JaegerObjectFactory objectFactory = new JaegerObjectFactory();

    /**
     * Specify baggage prefix. The default is {@value B3TextMapCodec#BAGGAGE_PREFIX}
     */
    public Builder withBaggagePrefix(String baggagePrefix) {
      this.baggagePrefix = baggagePrefix;
      return this;
    }

    /**
     * Specify JaegerSpanContext factory. Used for creating new span contexts. The default factory
     * is an instance of {@link JaegerObjectFactory}.
     */
    public Builder withObjectFactory(JaegerObjectFactory objectFactory) {
      this.objectFactory = objectFactory;
      return this;
    }

    public HorusTracingCodec build() {
      return new HorusTracingCodec(this);
    }
  }

  static Long lowerHexToUnsignedLong(String lowerHex) {
    int length = lowerHex.length();
    if (length < 1 || length > 32) {
      return null;
    }

    // trim off any high bits
    int beginIndex = length > 16 ? length - 16 : 0;

    return hexToUnsignedLong(lowerHex, beginIndex, Math.min(beginIndex + 16, lowerHex.length()));
  }

  static Long higherHexToUnsignedLong(String higherHex) {
    int length = higherHex.length();
    if (length > 32 || length < 1) {
      return null;
    }

    return hexToUnsignedLong(higherHex, 0, Math.max(length - 16, 0));
  }

  static Long hexToUnsignedLong(String lowerHex, int index, int endIndex) {
    long result = 0;
    for (; index < endIndex; index++) {
      char c = lowerHex.charAt(index);
      result <<= 4;
      if (c >= '0' && c <= '9') {
        result |= c - '0';
      } else if (c >= 'a' && c <= 'f') {
        result |= c - 'a' + 10;
      } else {
        return null;
      }
    }
    return result;
  }

  static String toLowerHex(long v) {
    char[] data = new char[16];
    writeHexLong(data, 0, v);
    return new String(data);
  }

  static String toLowerHex(long high, long low) {
    char[] result = new char[high != 0 ? 32 : 16];
    int pos = 0;
    if (high != 0) {
      writeHexLong(result, pos, high);
      pos += 16;
    }
    writeHexLong(result, pos, low);
    return new String(result);
  }

  static void writeHexLong(char[] data, int pos, long v) {
    writeHexByte(data, pos + 0, (byte) ((v >>> 56L) & 0xff));
    writeHexByte(data, pos + 2, (byte) ((v >>> 48L) & 0xff));
    writeHexByte(data, pos + 4, (byte) ((v >>> 40L) & 0xff));
    writeHexByte(data, pos + 6, (byte) ((v >>> 32L) & 0xff));
    writeHexByte(data, pos + 8, (byte) ((v >>> 24L) & 0xff));
    writeHexByte(data, pos + 10, (byte) ((v >>> 16L) & 0xff));
    writeHexByte(data, pos + 12, (byte) ((v >>> 8L) & 0xff));
    writeHexByte(data, pos + 14, (byte) (v & 0xff));
  }

  static final char[] HEX_DIGITS =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  static void writeHexByte(char[] data, int pos, byte b) {
    data[pos + 0] = HEX_DIGITS[(b >> 4) & 0xf];
    data[pos + 1] = HEX_DIGITS[b & 0xf];
  }
}