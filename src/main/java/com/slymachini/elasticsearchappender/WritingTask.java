package com.slymachini.elasticsearchappender;

import io.searchbox.client.JestClient;
import io.searchbox.core.Index;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

/***
 * Class contains logic to write logging events into the elastic search. It is
 * not used outside the library, that is why it has default visibility modifier
 * 
 * @author ashykh
 * 
 */
class WritingTask implements Callable<LoggingEvent> {
	private LoggingEvent loggingEvent;
	private MessageProperties messageProperties;
	private JestClient client;

	WritingTask(LoggingEvent loggingEvent, MessageProperties messageProperties,
			JestClient client) {
		this.loggingEvent = loggingEvent;
		this.messageProperties = messageProperties;
		this.client = client;
	}

	/**
	 * Method is called by ExecutorService. It creates message and insert it
	 * into ElasticSearch
	 * 
	 * @throws Exception
	 *             if any error occurred
	 */
	@Override
	public LoggingEvent call() throws Exception {
		if (client != null) {
			client.execute(createIndex());
		}
		return loggingEvent;
	}

	private Index createIndex() throws UnknownHostException {
		String messageId = UUID.randomUUID().toString();
		Map<String, Object> data = new HashMap<String, Object>();
		writeBasic(data, loggingEvent);
		writeThrowable(data, loggingEvent);
		return new Index.Builder(data).index(messageProperties.getIndex())
				.type(messageProperties.getEventType()).id(messageId).build();
	}

	private void writeBasic(Map<String, Object> json, LoggingEvent event)
			throws UnknownHostException {
		json.put("hostName", InetAddress.getLocalHost().getHostName());
		json.put("userName", System.getProperty("user.name"));
		json.put("timestamp", event.getTimeStamp());
		json.put("logger", event.getLoggerName());
		json.put("level", event.getLevel().toString());
		json.put("message", event.getMessage());
		LocationInfo locationInfo = event.getLocationInformation();
		json.put("fileName", locationInfo.getFileName());
		json.put("className", locationInfo.getClassName());
		json.put("methodName", locationInfo.getMethodName());
		json.put("lineNumber", locationInfo.getLineNumber());
		json.put("fullInfo", locationInfo.fullInfo);
		json.put("threadName", event.getThreadName());
	}

	private void writeThrowable(Map<String, Object> json, LoggingEvent event) {
		ThrowableInformation information = event.getThrowableInformation();
		if (information != null) {
			Throwable throwable = information.getThrowable();
			json.put("className", throwable.getClass().getCanonicalName());
			json.put("stackTrace", getStackTrace(throwable));
		}
	}

	private String getStackTrace(Throwable aThrowable) {
		final Writer result = new StringWriter();
		final PrintWriter printWriter = new PrintWriter(result);
		aThrowable.printStackTrace(printWriter);
		return result.toString();
	}

}