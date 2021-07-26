package com.diamis.horus.httptojms;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.glassfish.grizzly.http.server.HttpServer;

import com.ibm.mq.jms.MQConnectionFactory;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.DefaultResourceConfig;

import com.diamis.horus.HorusUtils;
import com.diamis.horus.HorusException;
import com.diamis.horus.HorusTracingCodec;

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

public class JMSProducer {

	public final static String RFH_PREFIX = "rfhHttpHeaderPrefix";
	public final static String MQMD_PREFIX = "mqmdHttpHeaderPrefix";
	public final static String MQMD_MSGID = "msgid";
	public final static String MQMD_CORELID = "correlid";

	static HttpServer server = null;
	static MQConnectionFactory factory = null;
	static String jmsQueue = null;
	private Tracer tracer = null;
	private static Map<String,String> processingParameters = null;

	private static JMSProducer jmsProducer = null;

	static Map<String,Map<String,String>> stripHeaders(Map<String,String> headers){
		Map<String,Map<String,String>> result = new HashMap<String,Map<String,String>>();
		result.put("RFH2",new HashMap<String,String>());
		result.put("MQMD",new HashMap<String,String>());
		for(Entry<String,String> entry : headers.entrySet()){
			//System.out.println("Incoming Http Header : " + entry.getKey() + ", " + entry.getValue());

			if (entry.getValue().startsWith(JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-"))){
				int pos = entry.getValue().indexOf(":");
				String key = entry.getValue().substring(0, pos).replaceAll(JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-"), "").trim();
				String value = entry.getValue().substring(pos).trim();
				//System.out.println("Out Header RFH : " + key + ", " + value);
				result.get("RFH2").put(key,value);
			}else if (entry.getValue().startsWith(JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"))){
				int pos = entry.getValue().indexOf(":");
				String key = entry.getValue().substring(0, pos).replaceAll(JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "rfh2-"), "").trim();
				String value = entry.getValue().substring(pos).trim();
				//System.out.println("Out Header MQMD : " + key + ", " + value);
				result.get("MQMD").put(key,value);
			}else if(entry.getKey().startsWith(JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-"))){
				String key = entry.getKey().replaceAll(JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-") ,"").trim();
				//System.out.println("Out Header RFH : " + key + ", " + entry.getValue());
				result.get("RFH2").put(key,entry.getValue());
			}else if(entry.getKey().startsWith(JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"))){
				String key = entry.getKey().replaceAll(JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"), "").trim();
				//System.out.println("Out Header MQMD : " + key + ", " + entry.getValue());
				result.get("MQMD").put(key,entry.getValue());
			}
			
		}

		return result;
	}

	static void sendMessage(String message, String business_id, String traceid, String spanid, String parentspanid,
			String sampled,Map<String,Map<String,String>> mqheaders) throws HorusException {
		Tracer localtracer = GlobalTracer.get();
		SpanContext current = null;
		Span root = null;
		Map<String, String> mapper = new HashMap<String, String>();
		if ((null != traceid) && (null != spanid)) {
			mapper.put("X-B3-TraceId", traceid);
			mapper.put("X-B3-SpanId", spanid);
			mapper.put("X-B3-ParentSpanId", parentspanid);
			mapper.put("X-B3-Sampled", sampled);
			current = localtracer.extract(Builtin.TEXT_MAP, new TextMapAdapter(mapper));
		} else {
			root = localtracer.buildSpan("Received message (no spans)").start();
			root.log("Initial request");
			current = root.context();
		}

		Span consumedMessage = localtracer.buildSpan("Sending Message").withTag("Queue", jmsQueue).asChildOf(current)
				.start();

		HorusUtils.logJson("DEBUG", business_id, jmsQueue, "Writing message to " + jmsQueue + " : " + message + "\n");
		try {
			consumedMessage.log("Establishing connection");
			Connection connect = factory.createConnection(null, null);
			Session session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination queue = (Destination) session.createQueue(jmsQueue);
			MessageProducer producer = session.createProducer(queue);
			connect.start();
			TextMessage msg = session.createTextMessage(message);
			localtracer.inject(consumedMessage.context(),Builtin.TEXT_MAP,new HorusMQHeaderInjectAdaptor(msg));
			
			//Treat MQMD out headers
			for(Entry<String,String> mqmdentry: mqheaders.get("MQMD").entrySet()){
				if(JMSProducer.MQMD_MSGID.equals(mqmdentry.getKey())){
					if(mqmdentry.getValue().contains(":")){
						msg.setJMSMessageID(mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":")+1).trim());
						HorusUtils.logJson("INFO", business_id, jmsQueue,"Setting msgid to " + mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":")+1).trim());
					}else{
						msg.setJMSMessageID(mqmdentry.getValue());
						HorusUtils.logJson("INFO", business_id, jmsQueue,"Setting msgid to " + mqmdentry.getValue());
					}

				}
				if(JMSProducer.MQMD_CORELID.equals(mqmdentry.getKey())){
					if(mqmdentry.getValue().contains(":")){
						msg.setJMSCorrelationID(mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":")+1).trim());
						HorusUtils.logJson("INFO", business_id, jmsQueue,"Setting correlid to " + mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":")+1).trim());	
					}else{
						msg.setJMSCorrelationID(mqmdentry.getValue());
						HorusUtils.logJson("INFO", business_id, jmsQueue,"Setting correlid to " + mqmdentry.getValue());		
					}
				}
			}

			//Treat RFH2 out headers
			for(Entry<String,String> rfhentry: mqheaders.get("RFH2").entrySet()){
				HorusUtils.logJson("INFO", business_id, jmsQueue,"Setting RFH2 header " + rfhentry.getKey() + " to " + rfhentry.getValue().substring(rfhentry.getValue().indexOf(":")+1).trim());
				if(rfhentry.getKey().startsWith("JMS"))
					HorusUtils.logJson("DEBUG",business_id, jmsQueue,"Skipping JMS Property " + rfhentry.getKey());
				else
					msg.setStringProperty(rfhentry.getKey(), rfhentry.getValue().substring(rfhentry.getValue().indexOf(":")+1).trim());
			}

			consumedMessage.log("Sending message");
			producer.send(msg);
			msg = null;
			connect.stop();
			producer.close();
			session.close();
			connect.close();
			consumedMessage.log("Message sent");
		} catch (JMSException e) {
			HorusUtils.logJson("ERROR", business_id, jmsQueue,
					"JMS Error while sending message to queue: " + e.getMessage());
			if(e.getLinkedException()!=null)
				HorusUtils.logJson("INFO", business_id, jmsQueue,
						"Linked Exception: " + e.getLinkedException().getMessage());
			consumedMessage.log("Error sending message in queue");
			throw new HorusException("JMS Error while sending message to queue", e);
		} finally {
			consumedMessage.finish();
		}
		if (null != root)
			root.finish();
	}

	private JMSProducer(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue,Map<String,String> extraProps)
			throws HorusException {
		String tracerHost = extraProps.getOrDefault("tracerHost", "localhost");
		Integer tracerPort = Integer.parseInt(extraProps.getOrDefault("tracerPort", "5775"));
		CodecConfiguration codecConfiguration = new CodecConfiguration();
		codecConfiguration.withCodec(Builtin.HTTP_HEADERS, new B3TextMapCodec.Builder().build());
		codecConfiguration.withCodec(Builtin.TEXT_MAP, new HorusTracingCodec.Builder().build());

		SamplerConfiguration samplerConfig = new SamplerConfiguration().withType(ConstSampler.TYPE).withParam(1);
		SenderConfiguration senderConfig = new SenderConfiguration().withAgentHost(tracerHost).withAgentPort(tracerPort);
		ReporterConfiguration reporterConfig = new ReporterConfiguration().withLogSpans(true).withFlushInterval(1000)
				.withMaxQueueSize(10000).withSender(senderConfig);
		Configuration configuration = new Configuration("BLUE_" + jmsQueue).withSampler(samplerConfig)
				.withReporter(reporterConfig).withCodec(codecConfiguration);

		GlobalTracer.registerIfAbsent(configuration.getTracer());

		HorusUtils.logJson("INFO", null, jmsQueue, "Init Tracer " + tracerHost + ":" + tracerPort);
		

		tracer = GlobalTracer.get();

		Span parent = tracer.buildSpan("JMS Producer startup").start();
		parent.setTag("jmsQueue", jmsQueue);
		parent.setTag("box", "BLUE");

		parent.log("Start Connection to " + jmsQueue);
		factory = new MQConnectionFactory();
		try {
			factory.setHostName(jmsHost);
			factory.setPort(jmsPort);
			factory.setQueueManager(jmsQmgr);
			factory.setChannel(jmsChannel);
			factory.setTransportType(1);
		} catch (JMSException e) {
			HorusUtils.logJson("ERROR", null, jmsQueue, "Error while connecting to QM: " + e.getMessage());
			throw new HorusException("Error while connecting to QM", e);
		}
		parent.log("Connection to " + jmsQueue + " established");

		HorusUtils.logJson("INFO", null, jmsQueue,
				"Starting Horus MQ Connect to queue //" + jmsHost + ":" + jmsPort + "/" + jmsQmgr + "/" + jmsQueue);
		HorusUtils.logJson("INFO", null, jmsQueue, "Prefixes : " + extraProps.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-") + "/" + extraProps.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"));
		JMSProducer.processingParameters = extraProps;
		JMSProducer.jmsQueue = jmsQueue;

	}

	@SuppressWarnings("unchecked")
	private void start(int httpPort) throws HorusException {

		HorusUtils.logJson("INFO", null, jmsQueue, "Connecting to queue");
		// connect.start();

		HorusUtils.logJson("INFO", null, jmsQueue, "Spawning HTTP Server");
		DefaultResourceConfig resourceConfig = new DefaultResourceConfig(HorusHttpEndpoint.class);
		java.util.Map<String, Object> conf = new java.util.HashMap<String, Object>();
		conf.put("jmsQueue", jmsQueue);

		resourceConfig.setPropertiesAndFeatures(conf);
		// The following line is to enable GZIP when client accepts it
		resourceConfig.getContainerResponseFilters().add(new GZIPContentEncodingFilter());
		try {
			server = GrizzlyServerFactory.createHttpServer("http://0.0.0.0:" + httpPort, resourceConfig);
		} catch (IOException e) {
			HorusUtils.logJson("FATAL", null, jmsQueue, "Unable to start Web Service: " + e.getMessage());
			throw new HorusException("Unable to start Web Service", e);
		}

		while (true) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				server.stop();
				// connect.stop();
			}
		}

	}

	public static void main(String[] args) throws HorusException {

		String jmsHost, jmsQmgr, jmsChannel, jmsQueue;
		Integer jmsPort, httpPort;
		Map<String,String> extraProps = new HashMap<String,String>();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				HorusUtils.logJson("FATAL", null, JMSProducer.jmsQueue, "Received Sigterm... Cleaning up.");
				server.stop();
			}
		});

		if ((args.length != 6)&&(args.length != 1)) {
			System.out.println("Program takes six arguments, " + args.length
					+ " supplied : <host> <port> <qmgr> <channel> <write_queue> <http_port>");
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
			httpPort = Integer.parseInt(props.getProperty("http.port"));
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
			httpPort = Integer.parseInt(args[5]);
		}

		jmsProducer = new JMSProducer(jmsHost,jmsPort,jmsQmgr,jmsChannel,jmsQueue, extraProps);
		jmsProducer.start(httpPort);

	}

}
