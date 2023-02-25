package com.diamis.horus.httptojms;

import java.util.HashMap;
import java.util.Map;

import javax.lang.model.util.ElementScanner6;

import java.util.List;

import com.diamis.horus.HorusUtils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest
        extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        String[] args = new String[] { "localhost", "1234", "qmgr", "channel", "testQ", "8085" };
        // JMSProducer.main(args);

        Map<String, String> params = new HashMap<String, String>();
        params.put(JMSProducer.RFH_PREFIX, "rfh2-");
        JMSProducer.setProcessingParameters(params);

        String test1 = "B64PRF-TXlWYWx1ZSMhI1RoaXMgaXMgdGhlIHZhbHVlOiB0ZXN0Cg%3D%3D";
        String test2 = "rfh2-MyValue : This is the value: test";
        Map<String, String> result = JMSProducer.unpackHeader(test1, null);
        Map<String, String> result2 = JMSProducer.unpackHeader(test2, null);
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("key", "rfh2-MyValue");
        expected.put("value", "This is the value: test");
        String expected2 = "B64PRF-cmZoMi1NeVZhbHVlIyEjVGhpcyBpcyB0aGUgdmFsdWU6IHRlc3Q%3D";

        String reformed = JMSProducer.packHeader("rfh2-" + expected.get("key"), expected.get("value"),
                "rfh2-" + expected.get("key"));

        System.out.println(reformed);

        // assertEquals(expected, result);
        assertEquals(expected, result2);
        // assertEquals(expected2,reformed);
    }

    public void testSplit() {
        String input0 = "<?xml version=\"1.0\"?>\n" +
                "<saa:DataPDU xmlns:saa=\"urn:swift:saa:xsd:saa.2.0\"><saa:Revision>2.0.10</saa:Revision><saa:Header><saa:Message><saa:SenderReference>Biz1028441597</saa:SenderReference><saa:MessageIdentifier>colr.xxx.ccprocessingreport</saa:MessageIdentifier><saa:Format>File</saa:Format><saa:Sender><saa:DN>cn=ecms,o=trgtxecm,o=swift</saa:DN></saa:Sender><saa:Receiver><saa:DN>ou=writ,ou=ecm,o=writcher,o=swift</saa:DN></saa:Receiver><saa:InterfaceInfo><saa:UserReference>Biz1028441597</saa:UserReference><saa:ValidationLevel>Minimum</saa:ValidationLevel><saa:MessageNature>Financial</saa:MessageNature><saa:ProductInfo><saa:Product><saa:VendorName>Diamis</saa:VendorName><saa:ProductName>cristal</saa:ProductName><saa:ProductVersion>5.0</saa:ProductVersion></saa:Product></saa:ProductInfo></saa:InterfaceInfo><saa:NetworkInfo><saa:Service>esmig.ecms.fast!pu</saa:Service><saa:SWIFTNetNetworkInfo><saa:FileInfo>SwCompression=None</saa:FileInfo></saa:SWIFTNetNetworkInfo></saa:NetworkInfo><saa:SecurityInfo><saa:SWIFTNetSecurityInfo><saa:IsNRRequested>true</saa:IsNRRequested></saa:SWIFTNetSecurityInfo></saa:SecurityInfo><saa:FileLogicalName>colr.xxx.ccprocessingreport-Biz1028441597</saa:FileLogicalName></saa:Message></saa:Header><saa:Body><AppHdr xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\"><Fr><FIId><FinInstnId><BICFI>TRGTXECMXXX</BICFI><Othr><Id>BDFEFR2TXXX</Id></Othr></FinInstnId></FIId></Fr><To><FIId><FinInstnId><BICFI>WRITCHERXXX</BICFI><Othr><Id>BDFEFR2TXXX</Id></Othr></FinInstnId></FIId></To><BizMsgIdr>Biz1028441597</BizMsgIdr><MsgDefIdr>colr.xxx.ccprocessingreport</MsgDefIdr><CreDt>2023-02-17T10:28:44Z</CreDt></AppHdr><Document xmlns=\"urn:swift:xsd:colr.xxx.ccprocessingreport\"><PrcsRpt><GrpHdr><MsgId>ECMS-FR159682023001061</MsgId><CreDtTm>2023-02-17T10:28:44</CreDtTm><ReportingDt>2023-02-17</ReportingDt><CptyRiad>FR15968</CptyRiad><NbOfCCR>219</NbOfCCR><NbOfCCU>0</NbOfCCU><NbOfCOAU>0</NbOfCOAU><NbOfRR>0</NbOfRR><NbOfRU>0</NbOfRU><NbOfMob>219</NbOfMob><NbOfDemob>0</NbOfDemob></GrpHdr><Rpt><OrgnlMsgId>FR159682023001061</OrgnlMsgId><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-01</CCRef><ECMSCCId>FRCCIDXX0000001</ECMSCCId><InstrId>0wxy4xcebxizxnk1</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasS2a-12</CCRef><ECMSCCId>FRCCIDXX0000002</ECMSCCId><InstrId>0wxy4xcebzd5fjm9</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-02</CCRef><ECMSCCId>FRCCIDXX0000003</ECMSCCId><InstrId>0wxy4xcebxknh01v</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-03</CCRef><ECMSCCId>FRCCIDXX0000004</ECMSCCId><InstrId>0wxy4xcebxktfc3p</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasS2a-13</CCRef><ECMSCCId>FRCCIDXX0000005</ECMSCCId><InstrId>0wxy4xcebzd5fjmb</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasS2a-14</CCRef><ECMSCCId>FRCCIDXX0000006</ECMSCCId><InstrId>0wxy4xcebzdbdvo5</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-04</CCRef><ECMSCCId>FRCCIDXX0000007</ECMSCCId><InstrId>0wxy4xcebxktfc3r</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-05</CCRef><ECMSCCId>FRCCIDXX0000008</ECMSCCId><InstrId>0wxy4xcebxkzdo5l</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-06</CCRef><ECMSCCId>FRCCIDXX0000009</ECMSCCId><InstrId>0wxy4xcebxkzdo5n</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-07</CCRef><ECMSCCId>FRCCIDXX0000010</ECMSCCId><InstrId>0wxy4xcebxlh8ob1</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt></Rpt></PrcsRpt></Document></saa:Body></saa:DataPDU>";
        List<String> output = HorusUtils.splitDataPDU(input0, 1000, "azerty", "queue");
        assertEquals(5,output.size());

        for (int i = 0; i < output.size(); i++)
            if (i == (output.size() - 1)){
                //assertEquals(input0.length() - output.size() * 1000 - output.get(0).length(), output.get(i).length());
            }else if (i != 0)
                assertEquals(1000, output.get(i).length());

        String input1 = "<?xml version=\"1.0\"?>\n" +
                "<DataPDU xmlns=\"urn:swift:xsd:saa.2.0\"><Revision>2.0.10</Revision><Header><Message><SenderReference>Biz1028441597</SenderReference><MessageIdentifier>colr.xxx.ccprocessingreport</MessageIdentifier><Format>File</Format><Sender><DN>cn=ecms,o=trgtxecm,o=swift</DN></Sender><Receiver><DN>ou=writ,ou=ecm,o=writcher,o=swift</DN></Receiver><InterfaceInfo><UserReference>Biz1028441597</UserReference><ValidationLevel>Minimum</ValidationLevel><MessageNature>Financial</MessageNature><ProductInfo><Product><VendorName>Diamis</VendorName><ProductName>cristal</ProductName><ProductVersion>5.0</ProductVersion></Product></ProductInfo></InterfaceInfo><NetworkInfo><Service>esmig.ecms.fast!pu</Service><SWIFTNetNetworkInfo><FileInfo>SwCompression=None</FileInfo></SWIFTNetNetworkInfo></NetworkInfo><SecurityInfo><SWIFTNetSecurityInfo><IsNRRequested>true</IsNRRequested></SWIFTNetSecurityInfo></SecurityInfo><FileLogicalName>colr.xxx.ccprocessingreport-Biz1028441597</FileLogicalName></Message></Header><Body><AppHdr xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\"><Fr><FIId><FinInstnId><BICFI>TRGTXECMXXX</BICFI><Othr><Id>BDFEFR2TXXX</Id></Othr></FinInstnId></FIId></Fr><To><FIId><FinInstnId><BICFI>WRITCHERXXX</BICFI><Othr><Id>BDFEFR2TXXX</Id></Othr></FinInstnId></FIId></To><BizMsgIdr>Biz1028441597</BizMsgIdr><MsgDefIdr>colr.xxx.ccprocessingreport</MsgDefIdr><CreDt>2023-02-17T10:28:44Z</CreDt></AppHdr><Document xmlns=\"urn:swift:xsd:colr.xxx.ccprocessingreport\"><PrcsRpt><GrpHdr><MsgId>ECMS-FR159682023001061</MsgId><CreDtTm>2023-02-17T10:28:44</CreDtTm><ReportingDt>2023-02-17</ReportingDt><CptyRiad>FR15968</CptyRiad><NbOfCCR>219</NbOfCCR><NbOfCCU>0</NbOfCCU><NbOfCOAU>0</NbOfCOAU><NbOfRR>0</NbOfRR><NbOfRU>0</NbOfRU><NbOfMob>219</NbOfMob><NbOfDemob>0</NbOfDemob></GrpHdr><Rpt><OrgnlMsgId>FR159682023001061</OrgnlMsgId><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-01</CCRef><ECMSCCId>FRCCIDXX0000001</ECMSCCId><InstrId>0wxy4xcebxizxnk1</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasS2a-12</CCRef><ECMSCCId>FRCCIDXX0000002</ECMSCCId><InstrId>0wxy4xcebzd5fjm9</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-02</CCRef><ECMSCCId>FRCCIDXX0000003</ECMSCCId><InstrId>0wxy4xcebxknh01v</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-03</CCRef><ECMSCCId>FRCCIDXX0000004</ECMSCCId><InstrId>0wxy4xcebxktfc3p</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasS2a-13</CCRef><ECMSCCId>FRCCIDXX0000005</ECMSCCId><InstrId>0wxy4xcebzd5fjmb</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasS2a-14</CCRef><ECMSCCId>FRCCIDXX0000006</ECMSCCId><InstrId>0wxy4xcebzdbdvo5</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-04</CCRef><ECMSCCId>FRCCIDXX0000007</ECMSCCId><InstrId>0wxy4xcebxktfc3r</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-05</CCRef><ECMSCCId>FRCCIDXX0000008</ECMSCCId><InstrId>0wxy4xcebxkzdo5l</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-06</CCRef><ECMSCCId>FRCCIDXX0000009</ECMSCCId><InstrId>0wxy4xcebxkzdo5n</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt><CdtrRpt><CdtrRiad>FR15968</CdtrRiad><CdtrInstrRpt><OpeTp>CCR</OpeTp><CCRef>CasC-OBLIG-D3-07</CCRef><ECMSCCId>FRCCIDXX0000010</ECMSCCId><InstrId>0wxy4xcebxlh8ob1</InstrId><InstrSts>Confirmed</InstrSts></CdtrInstrRpt></CdtrRpt></Rpt></PrcsRpt></Document></Body></DataPDU>";

        List<String> output2 = HorusUtils.splitDataPDU(input1, 1000, "azerty", "queue");
        assertEquals(5,output2.size());

        for (int i = 0; i < output2.size(); i++)
            if (i == (output2.size() - 1)){
                //assertEquals(input1.length() - output2.size() * 1000 - output2.get(0).length(), output2.get(i).length());
            }else if (i != 0)
                assertEquals(1000, output2.get(i).length());
    }
}
