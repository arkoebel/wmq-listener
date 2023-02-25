package com.diamis.horus.jmstohttp;

import java.util.Iterator;
import java.util.Map.Entry;


import io.opentracing.propagation.TextMap;
import okhttp3.Request.Builder;

class HorusHttpHeadersInjectAdapter implements TextMap {
  private final Builder httpRequest;

  public HorusHttpHeadersInjectAdapter(final Builder httpRequest) {
    this.httpRequest = httpRequest;
  }

  //@Override
  public void put(String key, String value){
    httpRequest.header(key, value);
  }

  public Iterator<Entry<String,String>> iterator() {
    throw new UnsupportedOperationException("This class should be used only with tracer#inject()");
  }
}