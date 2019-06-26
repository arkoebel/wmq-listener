package com.diamis.horus;

public class HorusException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    

    public HorusException(String message, Throwable e){
        super(message,e);

    }

}