package com.diamis.horus.jmstohttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Date;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ibm.mq.jms.MQConnectionFactory;

import com.diamis.horus.HorusException;
import com.diamis.horus.HorusUtils;

public class JmsListener {

	private static String PROXY_MODE = "proxy";
	private static String ROUTER_MODE = "router";

	private static JmsListener listener;

	private String destinationUrl = null;
	private String proxyUrl = null;
	private String destinationMimeType = null;
	private String proxyMode = null;
	private String jmsQueue = null;

	private Destination queue = null;
	private MessageConsumer consumer = null;
	private Session session = null;
	private MQConnectionFactory factory = null;
	private Connection connect = null;

	JmsListener(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue, String proxyMode, String proxyUrl, String destinationUrl, String destinationMimeType) throws HorusException {
		this.destinationUrl = destinationUrl;
		this.proxyUrl = proxyUrl;
		this.destinationMimeType = destinationMimeType;
		this.proxyMode = proxyMode;
		this.jmsQueue = jmsQueue;

		if (!proxyMode.equals(JmsListener.PROXY_MODE)&&!proxyMode.equals(JmsListener.ROUTER_MODE)){
			HorusUtils.logJson("FATAL",null,jmsQueue,"Error: proxyMode MUST be either proxy or router.");
			throw new IllegalArgumentException("Error: proxyMode MUST be either proxy or router.");
		}
		HorusUtils.logJson("INFO",null, jmsQueue,"Sending to "+destinationUrl+" via "+proxyUrl+" ("+proxyMode+" mode) as "+destinationMimeType);

		HorusUtils.logJson("INFO",null,jmsQueue,"Defining listener for Queue jms://"+jmsHost+":"+jmsPort+"/"+jmsQmgr+"/"+jmsQueue);
		HorusUtils.logJson("INFO",null,jmsQueue,"Sending messages to "+ proxyUrl);
		HorusUtils.logJson("INFO",null,jmsQueue,"Responses to be forwarded to "+destinationUrl);

		try{
			factory = new MQConnectionFactory();
			factory.setHostName(jmsHost);
			factory.setPort(jmsPort);
			factory.setQueueManager(jmsQmgr);
			factory.setChannel(jmsChannel);
			factory.setTransportType(1);

			connect = factory.createConnection("","");
			connect.start();
			session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
			queue = (Destination) session.createQueue(this.jmsQueue);
		}catch(JMSException e){
			HorusUtils.logJson("FATAL",null,jmsQueue,"JMS Error: " + e.getMessage());
			throw new HorusException("Unable to start JMS Connection",e);
		}


	}

	private boolean testJson(String json) {
		Gson gson = new Gson();
        try {
            gson.fromJson(json, Object.class);
            return true;
        } catch (com.google.gson.JsonSyntaxException ex) {
            return false;
        }
		//return (json.startsWith("{")&&json.endsWith("}"));
	}


	private void start() throws HorusException {
		HorusUtils.logJson("INFO",null,this.jmsQueue,"Creating Consumer");
		
		try {
			consumer = this.session.createConsumer(queue);
		} catch (JMSException e3) {
			HorusUtils.logJson("FATAL",null,this.jmsQueue,"Unable to create consumer: " + e3.getMessage());
			throw new HorusException("Unable to create consumer",e3);
		}

		TextMessage message = null;
		Message m = null;
		while (true) {
			try {
				m = consumer.receive(1);
			}catch(JMSException e) {
				HorusUtils.logJson("INFO",null,this.jmsQueue,"JMS Error : " + e.getMessage() + " retrying in 5s");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					HorusUtils.logJson("INFO",null,this.jmsQueue,"Sleep interrupted " + e1.getMessage());
					e1.printStackTrace();
				}
				
			    try {
			    	connect.start();
			    	session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
			    	queue = (Destination) session.createQueue(jmsQueue);
			    	consumer = this.session.createConsumer(queue);
			    	HorusUtils.logJson("INFO",null,this.jmsQueue,"Reconnection successful");
			    }catch(JMSException e2) {
			    	HorusUtils.logJson("INFO",null,this.jmsQueue,"Failed to reconnect. Retrying...");
			    }
			}

			if (m != null) {
				if (m instanceof TextMessage) {
					message = (TextMessage) m;
					String business_id = UUID.randomUUID().toString();
					HorusUtils.logJson("DEBUG",business_id,this.jmsQueue,"Receiving JMS Message");
					URL proxyCall;
					try {
						proxyCall = new URL(proxyUrl);
					} catch (MalformedURLException e) {
						HorusUtils.logJson("FATAL",null,this.jmsQueue,"ProxyURL is invalid: " + proxyUrl);
						throw new HorusException("ProxyUrl invalid : " + proxyUrl,e);
					}
					String msg;

					try {
						msg = message.getText();
					}
					catch(JMSException e){
						HorusUtils.logJson("ERROR",business_id,this.jmsQueue,"Unable to read message: " + e.getMessage());
						throw new HorusException("Unable to read message",e);
					}
					
					HorusUtils.logJson("DEBUG",business_id,this.jmsQueue,"Message is : "+msg);
					HorusUtils.logJson("DEBUG",business_id,this.jmsQueue,"Sending request to proxy.");
					HttpURLConnection conn;
					try {
						conn = (HttpURLConnection) proxyCall.openConnection();

						conn.setDoOutput(true);
						try {
							conn.setRequestMethod("POST");
						} catch (ProtocolException e) {
							//Nothing
						}
						conn.setRequestProperty("X-Business-Id",business_id);
						if (this.proxyMode.equals(PROXY_MODE))
							conn.setRequestProperty("x_destination_url", destinationUrl);
						else
							conn.setRequestProperty("x_sender_id", destinationUrl);

						conn.setRequestProperty("Content-Type", testJson(msg)?"application/json":"application/xml");
						conn.setRequestProperty("Accept",this.destinationMimeType);

						OutputStream os = conn.getOutputStream();
						os.write(msg.getBytes());
						os.flush();

						if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
							BufferedReader bb = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
							StringBuffer myoutput = new StringBuffer();
							String output;
							try {
								while ((output = bb.readLine()) != null) {
									myoutput.append(output).append("\n");
								}
							} catch (IOException e1) {
								// Shouldn't happen.
							}
							String contenttype = conn.getHeaderField("Content-type");
							HorusUtils.logJson("ERROR",business_id,this.jmsQueue,"Failed to get URL response : HTTP error code = " + conn.getResponseCode() + "Return Content-type: " + contenttype + "Body : " + myoutput.toString());
						}else{
							BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

							String output;
							StringBuffer sb = new StringBuffer();
							try {
								while ((output = br.readLine()) != null) {
									sb.append(output).append("\n");
								}
							} catch (IOException e1) {
								// Shouldn't happen.
							}
							String oOut = sb.toString();
							HorusUtils.logJson("DEBUG",business_id,this.jmsQueue,"Received response : " + oOut);

						}
					} catch (IOException e2) {
						HorusUtils.logJson("ERROR",business_id,this.jmsQueue,"Unable to contact destination URL" + e2.getMessage());;
					}

				} else {
					HorusUtils.logJson("ERROR",null,this.jmsQueue,"Exiting.");
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 9) {
			HorusUtils.logJson("FATAL",null,null,"Program takes nine arguments, " + args.length + " supplied : <host> <port> <qmgr> <channel> <read_queue> <proxy_mode: proxy/router> <proxy_url> <destination_url> <destination_mime_type>");
			System.exit(1);
		}

		listener = new JmsListener( args[0],(new Integer(args[1])).intValue(), args[2], args[3], args[4], args[5], args[6], args[7], args[8]);
		listener.start();

	}

}
