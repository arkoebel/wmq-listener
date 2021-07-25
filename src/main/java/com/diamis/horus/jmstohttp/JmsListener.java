package com.diamis.horus.jmstohttp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Map.Entry;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import javax.jms.Session;
import javax.jms.TextMessage;

import com.google.gson.Gson;
import com.ibm.mq.jms.MQConnectionFactory;

import com.diamis.horus.HorusException;
import com.diamis.horus.HorusTracingCodec;
import com.diamis.horus.HorusUtils;
import com.diamis.horus.httptojms.JMSProducer;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.CodecConfiguration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.Configuration.SenderConfiguration;
import io.jaegertracing.internal.propagation.B3TextMapCodec;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.util.GlobalTracer;
 
public class JmsListener {

	private static String PROXY_MODE = "proxy";
	private static String ROUTER_MODE = "router";

	private static JmsListener listener;

	private String destinationUrl = null;
	private String proxyUrl = null;
	private String destinationMimeType = null;
	private String proxyMode = null;
	private String jmsQueue = null;
	private static Map<String,String> processingParameters = null;

	private Destination queue = null;
	private MessageConsumer consumer = null;
	private Session session = null;
	private MQConnectionFactory factory = null;
	private Connection connect = null;

	private Tracer tracer = null;

	JmsListener(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue, String proxyMode,
			String proxyUrl, String destinationUrl, String destinationMimeType, Map<String,String> extraProps) throws HorusException {
		this.destinationUrl = destinationUrl;
		this.proxyUrl = proxyUrl;
		this.destinationMimeType = destinationMimeType;
		this.proxyMode = proxyMode;
		this.jmsQueue = jmsQueue;

		String tracerHost = extraProps.getOrDefault("tracerHost", "localhost");
		Integer tracerPort = Integer.parseInt(extraProps.getOrDefault("tracerPort", "5775"));
		HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, jmsQueue, "Init Tracer " + tracerHost + ":" + tracerPort);
		CodecConfiguration codecConfiguration = new CodecConfiguration();
        codecConfiguration.withCodec(Builtin.HTTP_HEADERS, new B3TextMapCodec.Builder().build());
        codecConfiguration.withCodec(Builtin.TEXT_MAP, new HorusTracingCodec.Builder().build());

		SamplerConfiguration samplerConfig = new SamplerConfiguration()
                .withType(ConstSampler.TYPE)
                .withParam(1);
            SenderConfiguration senderConfig = new SenderConfiguration()
                .withAgentHost(tracerHost)
                .withAgentPort(tracerPort);
            ReporterConfiguration reporterConfig = new ReporterConfiguration()
                .withLogSpans(true)
                .withFlushInterval(1000)
                .withMaxQueueSize(10000)
                .withSender(senderConfig);
			Configuration configuration = new Configuration("PINK_" + jmsQueue)
				.withSampler(samplerConfig)
				.withReporter(reporterConfig)
				.withCodec(codecConfiguration);

			GlobalTracer.registerIfAbsent(configuration.getTracer());

			tracer = GlobalTracer.get();

			Span parent = tracer.buildSpan("JMS Listener startup").start();

			parent.setTag("box", "PINK");
			parent.setTag("jmsQueue", jmsQueue);
			parent.setTag("proxyUrl", proxyUrl);
			parent.setTag("destinationUrl", destinationUrl);

		if (!proxyMode.equals(JmsListener.PROXY_MODE) && !proxyMode.equals(JmsListener.ROUTER_MODE)) {
			HorusUtils.logJson(HorusUtils.PINK_BOX,"FATAL", null, jmsQueue, "Error: proxyMode MUST be either proxy or router.");
			throw new IllegalArgumentException("Error: proxyMode MUST be either proxy or router.");
		}
		HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, jmsQueue, "Sending to " + destinationUrl + " via " + proxyUrl + " ("
				+ proxyMode + " mode) as " + destinationMimeType);

		HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, jmsQueue,
				"Defining listener for Queue jms://" + jmsHost + ":" + jmsPort + "/" + jmsQmgr + "/" + jmsQueue);
		HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, jmsQueue, "Sending messages to " + proxyUrl);
		HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, jmsQueue, "Responses to be forwarded to " + destinationUrl);
		HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, jmsQueue, "Prefixes : " + extraProps.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-") + "/" + extraProps.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"));

		parent.log("Start Connection to " + jmsQueue);
		try {
			factory = new MQConnectionFactory();
			factory.setHostName(jmsHost);
			factory.setPort(jmsPort);
			factory.setQueueManager(jmsQmgr);
			factory.setChannel(jmsChannel);
			factory.setTransportType(1);

			// Dummy comment
			connect = factory.createConnection(null, null);
			connect.start();
			session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
			queue = (Destination) session.createQueue(this.jmsQueue);
			parent.log("Connection to " + jmsQueue + " established");
			JmsListener.processingParameters = extraProps;
		} catch (JMSException e) {
			HorusUtils.logJson(HorusUtils.PINK_BOX,"FATAL", null, jmsQueue, "JMS Error: " + e.getMessage());
			parent.log("Failed to connect to " + jmsQueue);
			throw new HorusException("Unable to start JMS Connection", e);
		} finally{
			parent.finish();
		}

	}

	private boolean testJson(String json) {
		Gson gson = new Gson();
		try {
			gson.fromJson(json, Object.class);
			return true;
		} catch (com.google.gson.JsonSyntaxException ex) {
			return false;
		} catch (com.google.gson.JsonParseException ex) {
			return false;
		}
		// return (json.startsWith("{")&&json.endsWith("}"));
	}

	private void start() throws HorusException {
		HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "Creating Consumer");

		try {
			consumer = this.session.createConsumer(queue);
		} catch (JMSException e3) {
			HorusUtils.logJson(HorusUtils.PINK_BOX,"FATAL", null, this.jmsQueue, "Unable to create consumer: " + e3.getMessage());
			throw new HorusException("Unable to create consumer", e3);
		}

		TextMessage message = null;
		Message m = null;
		while (true) {
			try {
				m = consumer.receive(1);
			} catch (JMSException e) {
				HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "JMS Error : " + e.getMessage() + " retrying in 5s");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "Sleep interrupted " + e1.getMessage());
					e1.printStackTrace();
				}

				try {
					connect.start();
					session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
					queue = (Destination) session.createQueue(jmsQueue);
					consumer = this.session.createConsumer(queue);
					HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "Reconnection successful");
				} catch (JMSException e2) {
					HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "Failed to reconnect. Retrying...");
				}
			}

			if (m != null) {
				if (m instanceof TextMessage) {
					message = (TextMessage) m;
					String traceid = null;
					String spanid = null;
					String parentspanid = null;
					String sampled = null;
					String business_id = UUID.randomUUID().toString();
					String msgId = null;
					String correlId = null;
					try {
						msgId = message.getJMSMessageID();
					} catch (JMSException e) {
						// Nothing
					}
					try {
						correlId = message.getJMSCorrelationID();
					} catch (JMSException e) {
						// nothing
					}
					try {
						traceid = message.getStringProperty("XB3TraceId");
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "TraceId Error : " + e.getMessage());
					}
					try {
						spanid = message.getStringProperty("XB3SpanId");
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "SpanId Error : " + e.getMessage());
					}
					try {
						parentspanid = message.getStringProperty("XB3ParentSpanId");
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "ParentSpanId Error : " + e.getMessage());
					}
					try {
						sampled = message.getStringProperty("XB3Sampled");
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "Sampled Error : " + e.getMessage());
					}

					Map<String,String> extraHeaders = new HashMap<String,String>();
					try{
						
						Enumeration<String> it = message.getPropertyNames();
						while(it.hasMoreElements()){
							String prop = it.nextElement();
							if(!(prop.startsWith("XB3"))){
								String key = JmsListener.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-") + prop;
								extraHeaders.put(key,key + ": " + message.getStringProperty(prop));
								HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "Found RFH Property " + prop + ", value= " + message.getStringProperty(prop));
							}
						}
					}catch(JMSException e){
						//Nothing
					}
					if(null!=msgId){
						String key = JmsListener.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-") + JMSProducer.MQMD_MSGID;
						extraHeaders.put(key,key + ": " + msgId);
						HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "Found MQMD MsgId : " + msgId);
					}

					if(null!=correlId){
						String key = JmsListener.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-") + JMSProducer.MQMD_CORELID;
						extraHeaders.put(key,key + ": " + correlId);
						HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", null, this.jmsQueue, "Found MQMD CorrelId : " + correlId);
					}

					Map<String,String> mapper = new HashMap<String,String>();
					mapper.put("XB3TraceId",(String)(traceid==null?"0":traceid));
					mapper.put("XB3SpanId",(String)(spanid==null?"0":spanid));
					mapper.put("XB3ParentSpanId",(String)(parentspanid==null?"0":parentspanid));
					mapper.put("XB3Sampled",(String)(sampled==null?"1":sampled));

					SpanContext current = tracer.extract(Builtin.TEXT_MAP, new TextMapAdapter(mapper));
					
					Span consumedMessage = tracer.buildSpan("ReceivedMessage").withTag("Queue",jmsQueue).asChildOf(current).start();

					HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "Receiving JMS Message");
					URL proxyCall;
					try {
						proxyCall = new URL(proxyUrl);
					} catch (MalformedURLException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX,"FATAL", null, this.jmsQueue, "ProxyURL is invalid: " + proxyUrl);
						throw new HorusException("ProxyUrl invalid : " + proxyUrl, e);
					}
					String msg;

					try {
						msg = message.getText();
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX,"ERROR", business_id, this.jmsQueue,
								"Unable to read message: " + e.getMessage());
						throw new HorusException("Unable to read message", e);
					}

					HorusUtils.logJson(HorusUtils.PINK_BOX,"INFO", business_id, this.jmsQueue, "Message is : " + msg);
					HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "Tracing parameters : " + traceid + "/" + spanid + "/" + parentspanid + "/" + sampled);
					HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "Sending request to proxy.");
					HttpURLConnection conn;
					Span httpSpan = tracer.buildSpan("Send to http server").withTag("Destination", destinationUrl).withTag("Proxy",proxyUrl).asChildOf(consumedMessage).start();
					try {
						conn = (HttpURLConnection) proxyCall.openConnection();

						conn.setDoOutput(true);
						try {
							conn.setRequestMethod("POST");
						} catch (ProtocolException e) {
							// Nothing
						}
						tracer.inject(httpSpan.context(), Builtin.HTTP_HEADERS, new HorusHttpHeadersInjectAdapter(conn));
						conn.setRequestProperty("X-Business-Id", business_id);
						if (this.proxyMode.equals(PROXY_MODE))
							conn.setRequestProperty("x_destination_url", destinationUrl);
						else
							conn.setRequestProperty("x_sender_id", destinationUrl);

						conn.setRequestProperty("Content-Type", testJson(msg) ? "application/json" : "application/xml");
						conn.setRequestProperty("Accept", this.destinationMimeType);

						for(Entry<String,String> entry : extraHeaders.entrySet()){
							System.out.println(entry.getKey() + "=" + entry.getValue());
							conn.setRequestProperty(entry.getKey(), entry.getValue());
						}

						OutputStream os = conn.getOutputStream();
						os.write(msg.getBytes());
						os.flush();

						Span httpResponseSpan = null;
						if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
							httpResponseSpan = tracer.buildSpan("Http Response").withTag("ResponseCode",conn.getResponseCode()).asChildOf(httpSpan).start();
							
							if(null!=conn.getErrorStream()){
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
								HorusUtils.logJson(HorusUtils.PINK_BOX,"ERROR", business_id, this.jmsQueue,
									"Failed to get URL response : HTTP error code = " + conn.getResponseCode()
											+ "Return Content-type: " + contenttype + "Body : " + myoutput.toString());
							}else{
								HorusUtils.logJson(HorusUtils.PINK_BOX,"ERROR", business_id, this.jmsQueue,
									"Failed to get URL response : HTTP error code = " + conn.getResponseCode()
											+ "Return Content-type: " + conn.getHeaderField("Content-type") + "Body : empty");
							}
						} else {
							httpResponseSpan = tracer.buildSpan("Http Response").withTag("ResponseCode",conn.getResponseCode()).asChildOf(httpSpan).start();
							
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
							HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, this.jmsQueue, "Received response : " + oOut);

						}
						httpResponseSpan.finish();
						httpSpan.setTag("ResponseCode",conn.getResponseCode());
						httpSpan.finish();
					} catch (IOException e2) {
						HorusUtils.logJson(HorusUtils.PINK_BOX,"ERROR", business_id, this.jmsQueue,
								"Unable to contact destination URL" + e2.getMessage());
						;
					}

					consumedMessage.finish();
					

				} else {
					HorusUtils.logJson(HorusUtils.PINK_BOX,"ERROR", null, this.jmsQueue, "Exiting.");
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String jmsHost, jmsQmgr, jmsChannel, jmsQueue, proxyMode,proxyUrl,destinationUrl,destinationMimeType;
		Integer jmsPort;
		Map<String,String> extraProps = new HashMap<String,String>();

		if ((args.length != 9)&&(args.length != 1)) {
			HorusUtils.logJson(HorusUtils.PINK_BOX,"FATAL", null, null, "Program takes nine arguments, " + args.length
					+ " supplied : <host> <port> <qmgr> <channel> <read_queue> <proxy_mode: proxy/router> <proxy_url> <destination_url> <destination_mime_type>");
			System.exit(1);
		}

		if(args.length == 1){
			Properties props = new Properties();
			try {
				props.load(new FileReader(args[0]));
			} catch (FileNotFoundException e) {
				System.out.println("Supplied configuration file not found.");
				System.exit(1);
			} catch (IOException e) {
				System.out.println("Unable to read supplied configuration file.");
				System.exit(1);
			}
			jmsHost = props.getProperty("jms.host");
			jmsPort = Integer.parseInt(props.getProperty("jms.port"));
			jmsQmgr = props.getProperty("jms.qmgr");
			jmsChannel = props.getProperty("jms.channel");
			jmsQueue = props.getProperty("jms.queue");
			proxyMode = props.getProperty("proxy.mode");
			proxyUrl = props.getProperty("proxy.url");
			destinationUrl = props.getProperty("destination.url");
			destinationMimeType = props.getProperty("destination.mime-type");

			extraProps.put("tracerHost",props.getProperty("tracer.host"));
			extraProps.put("tracerPort",props.getProperty("tracer.port"));
			extraProps.put("rfhHttpHeaderPrefix",props.getProperty("rfh.http.header.prefix"));
			extraProps.put("mqmdHttpHeaderPrefix",props.getProperty("mqmd.http.header.prefix"));
		}else{
			jmsHost = args[0];
			jmsPort = Integer.parseInt(args[1]);
			jmsQmgr = args[2];
			jmsChannel = args[3];
			jmsQueue = args[4];
			proxyMode = args[5];
			proxyUrl = args[6];
			destinationUrl = args[7];
			destinationMimeType = args[8];
		}

		listener = new JmsListener(jmsHost,jmsPort,jmsQmgr,jmsChannel,jmsQueue,proxyMode,proxyUrl,destinationUrl,destinationMimeType,extraProps);

		listener.start();

	}

}
