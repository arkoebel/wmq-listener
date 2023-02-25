package com.diamis.horus.httptojms;

import java.util.Iterator;
import java.util.Map.Entry;

import javax.jms.JMSException;
import javax.jms.Message;

import io.opentracing.propagation.TextMap;

public class HorusMQHeaderInjectAdaptor implements TextMap {
    private final Message message;
  
    public HorusMQHeaderInjectAdaptor(final Message message) {
      this.message = message;
    }
  
    //@Override
    public void put(String key, String value){

      try{
        message.setStringProperty(key, value);
      }catch(JMSException e){
          //
      }
    }
  
    public Iterator<Entry<String,String>> iterator() {
      throw new UnsupportedOperationException("This class should be used only with tracer#inject()");
    }
  }
