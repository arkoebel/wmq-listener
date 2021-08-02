package com.diamis.horus.httptojms;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        String[] args = new String[]{"localhost","1234","qmgr","channel","testQ","8085"};
        //JMSProducer.main(args);

        Map<String,String> params = new HashMap<String,String>();
        params.put(JMSProducer.RFH_PREFIX,"rfh2-");
        JMSProducer.setProcessingParameters(params);

        String test1 = "B64PRF-TXlWYWx1ZSMhI1RoaXMgaXMgdGhlIHZhbHVlOiB0ZXN0Cg%3D%3D";
        String test2 = "rfh2-MyValue : This is the value: test";
        Map<String,String> result = JMSProducer.unpackHeader(test1,null);
        Map<String,String> result2 = JMSProducer.unpackHeader(test2,null);
        Map<String,String> expected = new HashMap<String,String>();
        expected.put("key","MyValue");
        expected.put("value","This is the value: test");
        String expected2 = "B64PRF-cmZoMi1NeVZhbHVlIyEjVGhpcyBpcyB0aGUgdmFsdWU6IHRlc3Q%3D";

        String reformed = JMSProducer.packHeader("rfh2-" + expected.get("key"), expected.get("value"), "rfh2-" + expected.get("key"));

        System.out.println(reformed);

        assertEquals(expected, result);
        assertEquals(expected,result2);
        assertEquals(expected2,reformed);
    }
}
