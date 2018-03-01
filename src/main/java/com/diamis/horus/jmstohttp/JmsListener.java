package com.diamis.horus.jmstohttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ibm.mq.jms.MQConnectionFactory;

public class JmsListener {

	private static String PROXY_MODE = "proxy";
	private static String ROUTER_MODE = "router";

	private static JmsListener listener;
	private static Log logger = LogFactory.getLog(JmsListener.class);

	private String destinationUrl = null;
	private String proxyUrl = null;
	private String destinationMimeType = null;
	private String proxyMode = null;

	private Destination queue = null;
	private MessageConsumer consumer = null;
	private Session session = null;

	private JmsListener(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue, String proxyMode, String proxyUrl, String destinationUrl, String destinationMimeType) throws JMSException {
		this.destinationUrl = destinationUrl;
		this.proxyUrl = proxyUrl;
		this.destinationMimeType = destinationMimeType;
		this.proxyMode = proxyMode;

		if (!proxyMode.equals(JmsListener.PROXY_MODE)&&!proxyMode.equals(JmsListener.ROUTER_MODE))
			throw new IllegalArgumentException("Error: proxyMode MUST be either proxy or router.");

		logger.info("Sending to "+destinationUrl+" via "+proxyUrl+" ("+proxyMode+" mode) as "+destinationMimeType);

		logger.info("Defining listener for Queue jms://"+jmsHost+":"+jmsPort+"/"+jmsQmgr+"/"+jmsQueue);
		logger.info("Sending messages to "+ proxyUrl);
		logger.info("Responses to be forwarded to "+destinationUrl);

		MQConnectionFactory factory = new MQConnectionFactory();
		factory.setHostName(jmsHost);
		factory.setPort(jmsPort);
		factory.setQueueManager(jmsQmgr);
		factory.setChannel(jmsChannel);
		factory.setTransportType(1);

		Connection connect = factory.createConnection("","");
		connect.start();
		session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
		queue = (Destination) session.createQueue(jmsQueue);


	}

	private boolean testJson(String json) {
		return (json.startsWith("{")&&json.endsWith("}"));
	}

	private void start() throws JMSException, MalformedURLException {
		logger.info("Creating Consumer");
		consumer = this.session.createConsumer(queue);

		TextMessage message = null;
		while (true) {
			Message m = consumer.receive(1);

			if (m != null) {
				if (m instanceof TextMessage) {
					message = (TextMessage) m;

					logger.debug("Receiving JMS Message");
					URL proxyCall;
					try {
						proxyCall = new URL(proxyUrl);
					} catch (MalformedURLException e) {
						throw new MalformedURLException("ProxyUrl invalid : " + proxyUrl);
					}

					logger.debug("Message is : "+message.getText());
					logger.debug("Sending request to proxy.");
					HttpURLConnection conn;
					try {
						conn = (HttpURLConnection) proxyCall.openConnection();

						conn.setDoOutput(true);
						try {
							conn.setRequestMethod("POST");
						} catch (ProtocolException e) {
							//Nothing
						}
						if (this.proxyMode.equals(PROXY_MODE))
							conn.setRequestProperty("x_destination_url", destinationUrl);
						else
							conn.setRequestProperty("x_sender_id", destinationUrl);

						conn.setRequestProperty("Content-Type", testJson(message.getText())?"application/json":"application/xml");
						conn.setRequestProperty("Accept",this.destinationMimeType);

						OutputStream os = conn.getOutputStream();
						os.write(message.getText().getBytes());
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
							logger.error("Failed to get URL response : HTTP error code = " + conn.getResponseCode() + "Return Content-type: " + contenttype + "Body : " + myoutput.toString());
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
							logger.info("Received response : " + oOut);

						}
					} catch (IOException e2) {
						logger.error("Unable to contact destination URL" + e2.getMessage());;
					}

				} else {
					System.out.println("Exiting.");
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 9) {
			System.out.println("Program takes nine arguments, " + args.length + " supplied : <host> <port> <qmgr> <channel> <read_queue> <proxy_mode: proxy/router> <proxy_url> <destination_url> <destination_mime_type>");
			System.exit(1);
		}

		listener = new JmsListener( args[0],(new Integer(args[1])).intValue(), args[2], args[3], args[4], args[5], args[6], args[7], args[8]);
		listener.start();

	}

}
