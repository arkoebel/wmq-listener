package com.diamis.horus.jmstohttp;

import java.net.HttpURLConnection;
import java.util.Iterator;
import java.util.Map.Entry;


import io.opentracing.propagation.TextMap;

class HorusHttpHeadersInjectAdapter implements TextMap {
  private final HttpURLConnection httpRequest;

  public HorusHttpHeadersInjectAdapter(final HttpURLConnection httpRequest) {
    this.httpRequest = httpRequest;
  }

  //@Override
  public void put(String key, String value){
    httpRequest.addRequestProperty(key, value);
  }

  public Iterator<Entry<String,String>> iterator() {
    throw new UnsupportedOperationException("This class should be used only with tracer#inject()");
  }
}