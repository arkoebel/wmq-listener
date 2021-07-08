package com.diamis.horus.httptojms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

	static HttpServer server = null;
	static MQConnectionFactory factory = null;
	static String jmsQueue = null;
	private Tracer tracer = null;

	private static JMSProducer jmsProducer = null;

	static void sendMessage(String message, String business_id, String traceid, String spanid, String parentspanid,
			String sampled) throws HorusException {
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

	private JMSProducer(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue)
			throws HorusException {
		CodecConfiguration codecConfiguration = new CodecConfiguration();
		codecConfiguration.withCodec(Builtin.HTTP_HEADERS, new B3TextMapCodec.Builder().build());
		codecConfiguration.withCodec(Builtin.TEXT_MAP, new HorusTracingCodec.Builder().build());

		SamplerConfiguration samplerConfig = new SamplerConfiguration().withType(ConstSampler.TYPE).withParam(1);
		SenderConfiguration senderConfig = new SenderConfiguration().withAgentHost("localhost").withAgentPort(5775);
		ReporterConfiguration reporterConfig = new ReporterConfiguration().withLogSpans(true).withFlushInterval(1000)
				.withMaxQueueSize(10000).withSender(senderConfig);
		Configuration configuration = new Configuration("BLUE_" + jmsQueue).withSampler(samplerConfig)
				.withReporter(reporterConfig).withCodec(codecConfiguration);

		GlobalTracer.registerIfAbsent(configuration.getTracer());

		tracer = GlobalTracer.get();

		Span parent = tracer.buildSpan("JMS Producer startup").start();

		parent.setBaggageItem("box", "BLUE");
		parent.setBaggageItem("jmsQueue", jmsQueue);

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

		this.jmsQueue = jmsQueue;

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

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				HorusUtils.logJson("FATAL", null, jmsQueue, "Received Sigterm... Cleaning up.");
				server.stop();
			}
		});

		if (args.length != 6) {
			System.out.println("Program takes six arguments, " + args.length
					+ " supplied : <host> <port> <qmgr> <channel> <write_queue> <http_port>");
			System.exit(1);
		}

		jmsProducer = new JMSProducer(args[0], Integer.parseInt(args[1]), args[2], args[3], args[4]);
		jmsProducer.start(Integer.parseInt(args[5]));

	}

}
