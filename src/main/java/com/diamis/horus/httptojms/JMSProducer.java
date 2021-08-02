package com.diamis.horus.httptojms;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import java.util.Base64;
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
	public final static String PACK_SEP = "#!#";
	public final static String PACK_PREFIX = "B64PRF-";

	static HttpServer server = null;
	static MQConnectionFactory factory = null;
	static String jmsQueue = null;
	private Tracer tracer = null;
	private static Map<String, String> processingParameters = null;

	private static JMSProducer jmsProducer = null;

	public static void setProcessingParameters(Map<String, String> params) {
		processingParameters = params;
	}

	public static String packHeader(String key, String value, String primeKey) {
		final String rfh2prefix = JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-");
		final String mqmdprefix = JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-");

		if (primeKey.startsWith(rfh2prefix) || primeKey.startsWith(mqmdprefix)) {
			if (value.startsWith(JMSProducer.PACK_PREFIX))
				return value;
			else {
				String tmp = key + JMSProducer.PACK_SEP + value;
				String ret = "";
				try {
					ret = URLEncoder.encode(
							JMSProducer.PACK_PREFIX + Base64.getMimeEncoder().encodeToString(tmp.getBytes()), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					// Shouldn't happen
				}
				return ret;
			}
		}else{

			return value;
		}
	}

	public static Map<String, String> unpackHeader(String header, String key) {
		Map<String, String> result = new HashMap<String, String>();
		if (header.startsWith(JMSProducer.PACK_PREFIX)) {
			// New format : PACK_PREFIX + UrlEncode(Base64(key + PACK_SEP + value))
			String tmp = "";
			try {
				tmp = URLDecoder.decode(header.substring(JMSProducer.PACK_PREFIX.length()), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// Shouldn't happen
			}
			String decoded = new String(Base64.getMimeDecoder().decode(tmp));
			if (decoded.contains(JMSProducer.PACK_SEP)) {
				result.put("key", decoded.substring(0, decoded.indexOf(JMSProducer.PACK_SEP)));
				result.put("value", decoded
						.substring(decoded.indexOf(JMSProducer.PACK_SEP) + JMSProducer.PACK_SEP.length()).trim());
			}
		} else if (header.startsWith(JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-"))) {
			// Legacy format : RFH prefix + key + [: ] + value

			result.put("key",
					header.substring(
							JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-").length(),
							header.indexOf(':')).trim());
			result.put("value", header.substring(header.indexOf(": ") + 1).trim());
		} else if (header.startsWith(JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"))) {
			// Legacy format : MQMD prefix + key + [: ] + value

			result.put("key",
					header.substring(
							JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-").length(),
							header.indexOf(':')).trim());
			result.put("value", header.substring(header.indexOf(": ") + 1).trim());
		}else{
			result.put("key",key);
			result.put("value",header);
		}

		return result;
	}

	static Map<String, Map<String, String>> stripHeaders(Map<String, String> headers) {
		System.out.println("Strip Headers = " + headers);
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		final String rfh2prefix = JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-");
		final String mqmdprefix = JMSProducer.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-");
		result.put("RFH2", new HashMap<String, String>());
		result.put("MQMD", new HashMap<String, String>());
		for (Entry<String, String> entry : headers.entrySet()) {

			if ((entry != null) && (entry.getValue() != null) && (entry.getValue().startsWith(rfh2prefix))) {
				int pos = entry.getValue().indexOf(":");
				String key = entry.getValue().substring(0, pos).replaceAll(rfh2prefix, "").trim();
				String value = entry.getValue().substring(pos).trim();

				result.get("RFH2").put(key, value);
			} else if ((entry != null) && (entry.getValue() != null) && (entry.getValue().startsWith(mqmdprefix))) {
				int pos = entry.getValue().indexOf(":");
				String key = entry.getValue().substring(0, pos).replaceAll(mqmdprefix, "").trim();
				String value = entry.getValue().substring(pos).trim();

				result.get("MQMD").put(key, value);
			} else if ((entry != null) && (entry.getValue() != null) && (entry.getKey().startsWith(rfh2prefix))) {
				String key = entry.getKey().replaceAll(rfh2prefix, "").trim();

				result.get("RFH2").put(key, entry.getValue());
			} else if ((entry != null) && (entry.getValue() != null) && (entry.getKey()).startsWith(mqmdprefix)) {
				String key = entry.getKey().replaceAll(mqmdprefix, "").trim();

				result.get("MQMD").put(key, entry.getValue());
			}

		}

		return result;
	}

	static void sendMessage(String message, String business_id, String traceid, String spanid, String parentspanid,
			String sampled, Map<String, Map<String, String>> mqheaders) throws HorusException {
		Tracer localtracer = GlobalTracer.get();
		SpanContext current = null;
		Span root = null;
		Map<String, String> mapper = new HashMap<String, String>();
		if ((null != traceid) && (null != spanid)) {
			mapper.put("X-B3-TraceId", traceid);
			mapper.put("X-B3-SpanId", spanid);
			mapper.put("X-B3-ParentSpanId", parentspanid);
			mapper.put("X-B3-Sampled", sampled);
			current = localtracer.extract(Builtin.HTTP_HEADERS, new TextMapAdapter(mapper));
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
			localtracer.inject(consumedMessage.context(), Builtin.TEXT_MAP, new HorusMQHeaderInjectAdaptor(msg));

			// Treat MQMD out headers
			for (Entry<String, String> mqmdentry : mqheaders.get("MQMD").entrySet()) {
				if (JMSProducer.MQMD_MSGID.equals(mqmdentry.getKey())) {
					if (mqmdentry.getValue().contains(":")) {
						msg.setJMSMessageID(
								mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":") + 1).trim());
						HorusUtils.logJson("INFO", business_id, jmsQueue, "Setting msgid to "
								+ mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":") + 1).trim());
					} else {
						msg.setJMSMessageID(mqmdentry.getValue());
						HorusUtils.logJson("INFO", business_id, jmsQueue, "Setting msgid to " + mqmdentry.getValue());
					}

				}
				if (JMSProducer.MQMD_CORELID.equals(mqmdentry.getKey())) {
					if (mqmdentry.getValue().contains(":")) {
						msg.setJMSCorrelationID(
								mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":") + 1).trim());
						HorusUtils.logJson("INFO", business_id, jmsQueue, "Setting correlid to "
								+ mqmdentry.getValue().substring(mqmdentry.getValue().indexOf(":") + 1).trim());
					} else {
						msg.setJMSCorrelationID(mqmdentry.getValue());
						HorusUtils.logJson("INFO", business_id, jmsQueue,
								"Setting correlid to " + mqmdentry.getValue());
					}
				}
			}

			// Treat RFH2 out headers
			for (Entry<String, String> rfhentry : mqheaders.get("RFH2").entrySet()) {
				
				if (rfhentry.getKey().startsWith("JMS"))
					HorusUtils.logJson("DEBUG", business_id, jmsQueue, "Skipping JMS Property " + rfhentry.getKey());
				else if (rfhentry.getValue()
						.startsWith(JMSProducer.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-"))) {
					HorusUtils.logJson("INFO", business_id, jmsQueue, "Setting RFH2 header " + rfhentry.getKey()
							+ " to " + rfhentry.getValue().substring(rfhentry.getValue().indexOf(":") + 1).trim());
					msg.setStringProperty(rfhentry.getKey(),
							rfhentry.getValue().substring(rfhentry.getValue().indexOf(":") + 1).trim());
				} else {
					HorusUtils.logJson("INFO", business_id, jmsQueue,
							"Setting RFH2 header " + rfhentry.getKey() + " to " + rfhentry.getValue());
					msg.setStringProperty(rfhentry.getKey(), rfhentry.getValue());
				}
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
			if (e.getLinkedException() != null)
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

	private JMSProducer(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue,
			Map<String, String> extraProps) throws HorusException {
		String tracerHost = extraProps.getOrDefault("tracerHost", "localhost");
		Integer tracerPort = Integer.parseInt(extraProps.getOrDefault("tracerPort", "5775"));
		CodecConfiguration codecConfiguration = new CodecConfiguration();
		codecConfiguration.withCodec(Builtin.HTTP_HEADERS, new B3TextMapCodec.Builder().build());
		codecConfiguration.withCodec(Builtin.TEXT_MAP, new HorusTracingCodec.Builder().build());

		SamplerConfiguration samplerConfig = new SamplerConfiguration().withType(ConstSampler.TYPE).withParam(1);
		SenderConfiguration senderConfig = new SenderConfiguration().withAgentHost(tracerHost)
				.withAgentPort(tracerPort);
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
		HorusUtils.logJson("INFO", null, jmsQueue,
				"Prefixes : " + extraProps.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-") + "/"
						+ extraProps.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"));
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
		Map<String, String> extraProps = new HashMap<String, String>();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				HorusUtils.logJson("FATAL", null, JMSProducer.jmsQueue, "Received Sigterm... Cleaning up.");
				server.stop();
			}
		});

		if ((args.length != 6) && (args.length != 1)) {
			System.out.println("Program takes six arguments, " + args.length
					+ " supplied : <host> <port> <qmgr> <channel> <write_queue> <http_port>");
			System.exit(1);
		}

		if (args.length == 1) {
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
			extraProps.put("tracerHost", props.getProperty("tracer.host"));
			extraProps.put("tracerPort", props.getProperty("tracer.port"));
			extraProps.put("rfhHttpHeaderPrefix", props.getProperty("rfh.http.header.prefix"));
			extraProps.put("mqmdHttpHeaderPrefix", props.getProperty("mqmd.http.header.prefix"));
		} else {
			jmsHost = args[0];
			jmsPort = Integer.parseInt(args[1]);
			jmsQmgr = args[2];
			jmsChannel = args[3];
			jmsQueue = args[4];
			httpPort = Integer.parseInt(args[5]);
		}

		jmsProducer = new JMSProducer(jmsHost, jmsPort, jmsQmgr, jmsChannel, jmsQueue, extraProps);
		jmsProducer.start(httpPort);

	}

}
