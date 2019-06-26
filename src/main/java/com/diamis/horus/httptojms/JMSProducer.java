package com.diamis.horus.httptojms;

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;

import com.ibm.mq.jms.MQConnectionFactory;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.DefaultResourceConfig;

import com.diamis.horus.HorusUtils;
import com.diamis.horus.HorusException;


public class JMSProducer {
	
	//static MessageProducer producer = null;
	//static Session session = null;
	//static Connection connect = null;
	static HttpServer server = null;
	static MQConnectionFactory factory = null;
	static String jmsQueue = null;
	
	private static JMSProducer jmsProducer = null;
	
	static void sendMessage(String message,String business_id) throws HorusException {
		HorusUtils.logJson("DEBUG",business_id,jmsQueue,"Writing message to " + jmsQueue + " : "+message + "\n");
		try {
			Connection connect = factory.createConnection(null,null);
			Session session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination queue = (Destination) session.createQueue(jmsQueue);
			MessageProducer producer = session.createProducer(queue);
			connect.start();	
		    TextMessage msg = session.createTextMessage(message);
		    producer.send(msg);
		    msg= null;
			connect.stop();
			producer.close();
			session.close();
			connect.close();
		}catch(JMSException e) {
			HorusUtils.logJson("ERROR",business_id,jmsQueue,"JMS Error while sending message to queue: " + e.getMessage());
			HorusUtils.logJson("INFO",business_id,jmsQueue,"Linked Exception: " + e.getLinkedException().getMessage());
			throw new HorusException("JMS Error while sending message to queue",e);
		}finally {

		}
	}
	
	private JMSProducer(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue) throws HorusException {
		factory = new MQConnectionFactory();
		try {
		factory.setHostName(jmsHost);
		factory.setPort(jmsPort);
		factory.setQueueManager(jmsQmgr);
		factory.setChannel(jmsChannel);
		factory.setTransportType(1);
		}catch(JMSException e){
			HorusUtils.logJson("ERROR", null, jmsQueue, "Error while connecting to QM: " + e.getMessage());
			throw new HorusException("Error while connecting to QM",e);
		}
		
		HorusUtils.logJson("INFO",null,jmsQueue,"Starting Horus MQ Connect to queue //"+jmsHost+":"+jmsPort+"/"+jmsQmgr+"/"+jmsQueue);
		
		this.jmsQueue = jmsQueue;
		
	}
	
	@SuppressWarnings("unchecked")
	private void start(int httpPort) throws HorusException {
		
		HorusUtils.logJson("INFO",null, jmsQueue, "Connecting to queue");
		//connect.start();
		
		HorusUtils.logJson("INFO",null, jmsQueue, "Spawning HTTP Server");
		DefaultResourceConfig resourceConfig = new DefaultResourceConfig(HorusHttpEndpoint.class);
        // The following line is to enable GZIP when client accepts it
		resourceConfig.getContainerResponseFilters().add(new GZIPContentEncodingFilter());
		try {
        	server = GrizzlyServerFactory.createHttpServer("http://0.0.0.0:"+httpPort , resourceConfig);
		}catch(IOException e){
			HorusUtils.logJson("FATAL",null, jmsQueue, "Unable to start Web Service: " + e.getMessage());
			throw new HorusException("Unable to start Web Service",e); 
		}


        while(true) {try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			server.stop();
			//connect.stop();
		}}
        
        
		
	}


	public static void main(String[] args) throws HorusException {
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
	        @Override
	            public void run() {
	                HorusUtils.logJson("FATAL",null, jmsQueue, "Received Sigterm... Cleaning up.");
	                server.stop();
	            }   
	        }); 
		
		if (args.length != 6) {
			System.out.println("Program takes six arguments, " + args.length + " supplied : <host> <port> <qmgr> <channel> <write_queue> <http_port>");
			System.exit(1);
		}
		
		jmsProducer = new JMSProducer(args[0], new Integer(args[1]).intValue(), args[2], args[3], args[4]);
		jmsProducer.start(new Integer(args[5]).intValue());
		

	}

}
