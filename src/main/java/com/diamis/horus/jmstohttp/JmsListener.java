package com.diamis.horus.jmstohttp;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.diamis.horus.HorusException;
import com.diamis.horus.HorusTracingCodec;
import com.diamis.horus.HorusUtils;
import com.diamis.horus.httptojms.JMSProducer;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.MalformedJsonException;
import com.ibm.mq.jms.MQConnectionFactory;

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
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.util.GlobalTracer;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class JmsListener {

	private static String PROXY_MODE = "proxy";
	private static String ROUTER_MODE = "router";

	private static JmsListener listener;

	private String destinationUrl = null;
	private String proxyUrl = null;
	private String destinationMimeType = null;
	private String proxyMode = null;
	private static String jmsQueue = null;
	private static Map<String, String> processingParameters = null;

	private Destination queue = null;
	private MessageConsumer consumer = null;
	private Session session = null;
	private MQConnectionFactory factory = null;
	private Connection connect = null;

	private Tracer tracer = null;

	JmsListener(String jmsHost, int jmsPort, String jmsQmgr, String jmsChannel, String jmsQueue, String proxyMode,
			String proxyUrl, String destinationUrl, String destinationMimeType, Map<String, String> extraProps)
			throws HorusException {
		this.destinationUrl = destinationUrl;
		this.proxyUrl = proxyUrl;
		this.destinationMimeType = destinationMimeType;
		this.proxyMode = proxyMode;
		JmsListener.jmsQueue = jmsQueue;

		JMSProducer.setProcessingParameters(extraProps);
		String tracerHost = extraProps.getOrDefault("tracerHost", "localhost");
		Integer tracerPort = Integer.parseInt(extraProps.getOrDefault("tracerPort", "5775"));
		HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, jmsQueue, "Init Tracer " + tracerHost + ":" + tracerPort);
		CodecConfiguration codecConfiguration = new CodecConfiguration();
		codecConfiguration.withCodec(Builtin.HTTP_HEADERS, new B3TextMapCodec.Builder().build());
		codecConfiguration.withCodec(Builtin.TEXT_MAP, new HorusTracingCodec.Builder().build());

		SamplerConfiguration samplerConfig = new SamplerConfiguration().withType(ConstSampler.TYPE).withParam(1);
		SenderConfiguration senderConfig = new SenderConfiguration().withAgentHost(tracerHost)
				.withAgentPort(tracerPort);
		ReporterConfiguration reporterConfig = new ReporterConfiguration().withLogSpans(true).withFlushInterval(1000)
				.withMaxQueueSize(10000).withSender(senderConfig);
		Configuration configuration = new Configuration("PINK_" + jmsQueue).withSampler(samplerConfig)
				.withReporter(reporterConfig).withCodec(codecConfiguration);

		GlobalTracer.registerIfAbsent(configuration.getTracer());

		tracer = GlobalTracer.get();

		Span parent = tracer.buildSpan("JMS Listener startup").start();

		parent.setTag("box", "PINK");
		parent.setTag("jmsQueue", jmsQueue);
		parent.setTag("proxyUrl", proxyUrl);
		parent.setTag("destinationUrl", destinationUrl);

		if (!proxyMode.equals(JmsListener.PROXY_MODE) && !proxyMode.equals(JmsListener.ROUTER_MODE)) {
			HorusUtils.logJson(HorusUtils.PINK_BOX, "FATAL", null, jmsQueue,
					"Error: proxyMode MUST be either proxy or router.");
			throw new IllegalArgumentException("Error: proxyMode MUST be either proxy or router.");
		}
		HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, jmsQueue, "Sending to " + destinationUrl + " via "
				+ proxyUrl + " (" + proxyMode + " mode) as " + destinationMimeType);

		HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, jmsQueue,
				"Defining listener for Queue jms://" + jmsHost + ":" + jmsPort + "/" + jmsQmgr + "/" + jmsQueue);
		HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, jmsQueue, "Sending messages to " + proxyUrl);
		HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, jmsQueue,
				"Responses to be forwarded to " + destinationUrl);
		HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, jmsQueue,
				"Prefixes : " + extraProps.getOrDefault(JMSProducer.RFH_PREFIX, "rfh2-") + "/"
						+ extraProps.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-"));

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
			queue = (Destination) session.createQueue(JmsListener.jmsQueue);
			parent.log("Connection to " + jmsQueue + " established");
			JmsListener.processingParameters = extraProps;
		} catch (JMSException e) {
			HorusUtils.logJson(HorusUtils.PINK_BOX, "FATAL", null, jmsQueue, "JMS Error: " + e.getMessage());
			parent.log("Failed to connect to " + jmsQueue);
			throw new HorusException("Unable to start JMS Connection", e);
		} finally {
			parent.finish();
		}

	}

	private boolean testJson(String json) {
		JsonReader jsonReader = new JsonReader(new StringReader(json));
		try {
			JsonToken token;
			loop: while ((token = jsonReader.peek()) != JsonToken.END_DOCUMENT && token != null) {
				switch (token) {
					case BEGIN_ARRAY:
						jsonReader.beginArray();
						break;
					case END_ARRAY:
						jsonReader.endArray();
						break;
					case BEGIN_OBJECT:
						jsonReader.beginObject();
						break;
					case END_OBJECT:
						jsonReader.endObject();
						break;
					case NAME:
						jsonReader.nextName();
						break;
					case STRING:
					case NUMBER:
					case BOOLEAN:
					case NULL:
						jsonReader.skipValue();
						break;
					case END_DOCUMENT:
						break loop;
					default:
						throw new AssertionError(token);
				}
			}
			// System.out.println("JSON OK");
			return true;
		} catch (final MalformedJsonException ignored) {
			// System.out.println("KO " + ignored.getMessage());
			return false;
		} catch (IOException exception) {
			// System.out.println("KO " + exception.getMessage());
			return false;
		} catch (AssertionError exception) {
			// System.out.println("KO " + exception.getMessage());
			return false;
		}

	}

	private void start() throws HorusException {
		HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue, "Creating Consumer");

		try {
			consumer = this.session.createConsumer(queue);
		} catch (JMSException e3) {
			HorusUtils.logJson(HorusUtils.PINK_BOX, "FATAL", null, JmsListener.jmsQueue,
					"Unable to create consumer: " + e3.getMessage());
			throw new HorusException("Unable to create consumer", e3);
		}

		Message message = null;
		Message m = null;
		OkHttpClient client = new OkHttpClient.Builder()
			.connectTimeout(10,TimeUnit.SECONDS)
			.readTimeout(2,TimeUnit.MINUTES)
			.writeTimeout(10,TimeUnit.SECONDS)
			.build();
		while (true) {
			try {
				m = consumer.receive(1);
			} catch (JMSException e) {
				HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue,
						"JMS Error : " + e.getMessage() + " retrying in 5s");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue,
							"Sleep interrupted " + e1.getMessage());
					e1.printStackTrace();
				}

				try {
					connect.start();
					session = connect.createSession(false, Session.AUTO_ACKNOWLEDGE);
					queue = (Destination) session.createQueue(jmsQueue);
					consumer = this.session.createConsumer(queue);
					HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue, "Reconnection successful");
				} catch (JMSException e2) {
					HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue,
							"Failed to reconnect. Retrying...");
				}
			}

			if (m != null) {
				if ((m instanceof TextMessage) || (m instanceof BytesMessage)) {
					message = (Message) m;
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
						HorusUtils.logJson(HorusUtils.PINK_BOX, "DEBUG", business_id, JmsListener.jmsQueue,
								"TraceId Error : " + e.getMessage());
					}
					try {
						spanid = message.getStringProperty("XB3SpanId");
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX, "DEBUG", business_id, JmsListener.jmsQueue,
								"SpanId Error : " + e.getMessage());
					}
					try {
						parentspanid = message.getStringProperty("XB3ParentSpanId");
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX, "DEBUG", business_id, JmsListener.jmsQueue,
								"ParentSpanId Error : " + e.getMessage());
					}
					try {
						sampled = message.getStringProperty("XB3Sampled");
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX, "DEBUG", business_id, JmsListener.jmsQueue,
								"Sampled Error : " + e.getMessage());
					}

					Map<String, String> extraHeaders = new HashMap<String, String>();

					try {

						@SuppressWarnings("unchecked")
						Enumeration<String> it = message.getPropertyNames();
						while (it.hasMoreElements()) {
							String prop = it.nextElement();
							if (!(prop.startsWith("XB3")) && !(prop.startsWith("JMS"))) {
								String key = JmsListener.processingParameters.getOrDefault(JMSProducer.RFH_PREFIX,
										"rfh2-") + prop;
								extraHeaders.put(key, message.getStringProperty(prop));
								HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue,
										"Found RFH Property " + prop + ", value= " + message.getStringProperty(prop));
							}
						}
					} catch (JMSException e) {
						// Nothing
					}
					if (null != msgId) {
						String key = JmsListener.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-")
								+ JMSProducer.MQMD_MSGID;
						extraHeaders.put(key, msgId);
						HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue,
								"Found MQMD MsgId : " + msgId);
					}

					if (null != correlId) {
						String key = JmsListener.processingParameters.getOrDefault(JMSProducer.MQMD_PREFIX, "mqmd-")
								+ JMSProducer.MQMD_CORELID;
						extraHeaders.put(key, correlId);
						HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", null, JmsListener.jmsQueue,
								"Found MQMD CorrelId : " + correlId);
					}

					Map<String, String> mapper = new HashMap<String, String>();
					mapper.put("XB3TraceId", (String) (traceid == null ? "0" : traceid));
					mapper.put("XB3SpanId", (String) (spanid == null ? "0" : spanid));
					mapper.put("XB3ParentSpanId", (String) (parentspanid == null ? "0" : parentspanid));
					mapper.put("XB3Sampled", (String) (sampled == null ? "1" : sampled));

					SpanContext current = tracer.extract(Builtin.TEXT_MAP, new TextMapAdapter(mapper));

					Span consumedMessage = tracer.buildSpan("ReceivedMessage").withTag("Queue", jmsQueue)
							.asChildOf(current).start();

					HorusUtils.logJson(HorusUtils.PINK_BOX, "DEBUG", business_id, JmsListener.jmsQueue,
							"Receiving JMS Message");
					URL proxyCall;
					try {
						proxyCall = new URL(proxyUrl);
					} catch (MalformedURLException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX, "FATAL", null, JmsListener.jmsQueue,
								"ProxyURL is invalid: " + proxyUrl);
						throw new HorusException("ProxyUrl invalid : " + proxyUrl, e);
					}
					String msg;

					try {
						if (m instanceof TextMessage)
							msg = ((TextMessage) message).getText();
						else {
							byte[] bb = new byte[(int) ((BytesMessage) message).getBodyLength()];
							((BytesMessage) message).readBytes(bb);
							msg = new String(bb);
						}
					} catch (JMSException e) {
						HorusUtils.logJson(HorusUtils.PINK_BOX, "ERROR", business_id, JmsListener.jmsQueue,
								"Unable to read message: " + e.getMessage());
						throw new HorusException("Unable to read message", e);
					}

					MediaType mt = null;
					if (testJson(msg))
						mt = MediaType.get("application/json");
					else
						mt = MediaType.get("application/xml");

					RequestBody body = RequestBody.create(msg, mt);

					HorusUtils.logJson(HorusUtils.PINK_BOX, "INFO", business_id, JmsListener.jmsQueue,
							"Message is : " + msg);
					HorusUtils.logJson(HorusUtils.PINK_BOX, "DEBUG", business_id, JmsListener.jmsQueue,
							"Tracing parameters : " + traceid + "/" + spanid + "/" + parentspanid + "/" + sampled);
					HorusUtils.logJson(HorusUtils.PINK_BOX, "DEBUG", business_id, JmsListener.jmsQueue,
							"Sending request to proxy.");

					Span httpSpan = tracer.buildSpan("Send to http server").withTag("Destination", destinationUrl)
							.withTag("Proxy", proxyUrl).asChildOf(consumedMessage).start();

					Builder request = new Request.Builder().url(proxyCall.toString());

					

					request.post(body);

					tracer.inject(httpSpan.context(), Builtin.HTTP_HEADERS,
							new HorusHttpHeadersInjectAdapter(request));

					request.header("X-Business-Id", business_id);
					if (this.proxyMode.equals(PROXY_MODE))
						request.header("x_destination_url", destinationUrl);

					else
						request.header("x_sender_id", destinationUrl);

					request.header("Accept", this.destinationMimeType);

					for (Entry<String, String> entry : extraHeaders.entrySet()) {
						String res = JMSProducer.packHeader(entry.getKey(), entry.getValue(), entry.getKey());
						request.header(entry.getKey(), res);

					}

					Request rq = request.build();
					client.newCall(rq).enqueue(new Callback() {
						@Override
						public void onFailure(Call call, IOException e) {
							Request req = call.request();
							req.header(msg);

							HorusUtils.logJson(HorusUtils.PINK_BOX,"ERROR", req.header("X-Business-Id"),JmsListener.jmsQueue,
									"Failed to send request\n" + e.getMessage());
							httpSpan.setTag("Request", "Failed").finish();
						}

						@Override
						public void onResponse(Call call, Response response) throws IOException {
							try (ResponseBody responseBody = response.body()) {

								httpSpan.setTag("ResponseCode", response.code());
								if (!response.isSuccessful()) {
									HorusUtils.logJson(HorusUtils.PINK_BOX,"ERROR", business_id, JmsListener.jmsQueue,
											"Failed to get URL response : HTTP error code = " + response.code()
													+ "Return Content-type: " + response.header("Content-Type")
													+ "Body : "
													+ responseBody.string());

								} else {
									HorusUtils.logJson(HorusUtils.PINK_BOX,"DEBUG", business_id, JmsListener.jmsQueue,
											"Received response : " + responseBody.string());

								}

								httpSpan.finish();
							}
						}

					});

					consumedMessage.finish();

				} else {
					HorusUtils.logJson(HorusUtils.PINK_BOX, "ERROR", null, JmsListener.jmsQueue, "Exiting.");
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String jmsHost, jmsQmgr, jmsChannel, proxyMode, proxyUrl, destinationUrl, destinationMimeType;
		Integer jmsPort;
		Map<String, String> extraProps = new HashMap<String, String>();

		if ((args.length != 9) && (args.length != 1)) {
			HorusUtils.logJson(HorusUtils.PINK_BOX, "FATAL", null, null, "Program takes nine arguments, " + args.length
					+ " supplied : <host> <port> <qmgr> <channel> <read_queue> <proxy_mode: proxy/router> <proxy_url> <destination_url> <destination_mime_type>");
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
			JmsListener.jmsQueue = props.getProperty("jms.queue");
			proxyMode = props.getProperty("proxy.mode");
			proxyUrl = props.getProperty("proxy.url");
			destinationUrl = props.getProperty("destination.url");
			destinationMimeType = props.getProperty("destination.mime-type");

			extraProps.put("tracerHost", props.getProperty("tracer.host"));
			extraProps.put("tracerPort", props.getProperty("tracer.port"));
			extraProps.put("rfhHttpHeaderPrefix", props.getProperty("rfh.http.header.prefix"));
			extraProps.put("mqmdHttpHeaderPrefix", props.getProperty("mqmd.http.header.prefix"));
		} else {
			jmsHost = args[0];
			jmsPort = Integer.parseInt(args[1]);
			jmsQmgr = args[2];
			jmsChannel = args[3];
			JmsListener.jmsQueue = args[4];
			proxyMode = args[5];
			proxyUrl = args[6];
			destinationUrl = args[7];
			destinationMimeType = args[8];
		}

		listener = new JmsListener(jmsHost, jmsPort, jmsQmgr, jmsChannel, JmsListener.jmsQueue, proxyMode, proxyUrl, destinationUrl,
				destinationMimeType, extraProps);

		listener.start();

	}

}
