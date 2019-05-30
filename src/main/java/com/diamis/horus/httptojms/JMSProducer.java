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


public class JMSProducer {
	
	//static MessageProducer producer = null;
	//static Session session = null;
	//static Connection connect = null;
	static HttpServer server = null;
	static MQConnectionFactory factory = null;
	static String jmsQueue = null;
	
	private static JMSProducer jmsProducer = null;
	private static Logger logger = Logger.getLogger(JMSProducer.class);
	
	static void sendMessage(String message) throws JMSException {
		logger.debug("Writing message to " + jmsQueue + " : "+message + "\n");
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
			logger.error("JMS Error " + e.getMessage());
			logger.error("Linked Exception : " + e.getLinkedException().getMessage());
			throw e;
		}finally {

		}
	}
	
	private JMSProducer(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue) throws JMSException {
		factory = new MQConnectionFactory();
		factory.setHostName(jmsHost);
		factory.setPort(jmsPort);
		factory.setQueueManager(jmsQmgr);
		factory.setChannel(jmsChannel);
		factory.setTransportType(1);
		
		logger.info("Starting Horus MQ Connect to queue //"+jmsHost+":"+jmsPort+"/"+jmsQmgr+"/"+jmsQueue);
		//connect = factory.createConnection(null,null);
		//session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
		//Destination queue = (Destination) session.createQueue(jmsQueue);
		//producer = session.createProducer(queue);
		//connect.start();		
		
		this.jmsQueue = jmsQueue;
		
	}
	
	@SuppressWarnings("unchecked")
	private void start(int httpPort) throws JMSException, IllegalArgumentException, NullPointerException, IOException {
		
		logger.info("Connecting to queue");
		//connect.start();
		
		logger.info("Spawning HTTP Server");
		DefaultResourceConfig resourceConfig = new DefaultResourceConfig(HorusHttpEndpoint.class);
        // The following line is to enable GZIP when client accepts it
        resourceConfig.getContainerResponseFilters().add(new GZIPContentEncodingFilter());
        server = GrizzlyServerFactory.createHttpServer("http://0.0.0.0:"+httpPort , resourceConfig);
		
        while(true) {try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			server.stop();
			//connect.stop();
		}}
        
        
		
	}


	public static void main(String[] args) throws JMSException, IllegalArgumentException, NullPointerException, IOException {
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
	        @Override
	            public void run() {
	                logger.fatal("Received Sigterm... Cleaning up.");
	                server.stop();
	                //try {
						//connect.stop();
					//} catch (JMSException e) {
						// TODO Auto-generated catch block
					//	logger.error(e.getMessage());
					//}
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
