package com.diamis.horus.jmstohttp;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.diamis.horus.HorusException;
import com.diamis.horus.HorusUtils;
/**
 * Unit test for simple App.
 */
public class JmsListenerTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public JmsListenerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(JmsListenerTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testJmsListener() {
        JmsListener test = null ;
        try {
          test  = new JmsListener("localhost", 1234, "TestQMGR", "TestChannel", "TEST_Q", "proxy",
                    "proxyUrl", "destinationUrl", "application/json");
            
        } catch (HorusException e) {
            e.printStackTrace();
        } finally {
            HorusUtils.logJson("INFO", "123-456", "Test_Q", "Test Message");
        }
        assertTrue( true );
    }
}
