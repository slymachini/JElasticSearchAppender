package com.slymachini.elasticsearchappender;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Log4j appender implementation to work with Elastic Search. Provides
 * possibility to send messages to the Elastic Search server.
 * 
 * @author ashykh
 * 
 */
public class ElasticSearchAppender extends AppenderSkeleton {
	// Appender configuration properties
	/**
	 * Elastic search server host. In configuration should be set like this:
	 * <host>http://127.0.0.1</host> or
	 * log4j.appender.APPENDER_NAME.host=http://127.0.0.1
	 */
	private String host;
	/**
	 * Elastic search server port. In configuration should be set like this:
	 * <port>9200</port> or log4j.appender.APPENDER_NAME.port=9200
	 */
	private String port;
	/**
	 * Elastic search index, which will be used for message. In configuration
	 * should be set like this: <index>some-index</index> or
	 * log4j.appender.APPENDER_NAME.index=some-index
	 */
	private String index;
	/**
	 * Elastic search event type, which will be used for message. In
	 * configuration should be set like this: <eventType>some-type</eventType>
	 * or log4j.appender.APPENDER_NAME.eventType=some-type
	 */
	private String eventType;
	/**
	 * If this field is set, appender will automatically add current date to the
	 * index in specified format. In configuration should be set like this:
	 * <dateFormat>yyyy.MM.dd</dateFormat> or
	 * log4j.appender.APPENDER_NAME.dateFormat=yyyy.MM.dd
	 */
	private String dateFormat;

	// Appender non-property fields. Will not be configured using configuration
	// file
	private ExecutorService threadPool = Executors.newSingleThreadExecutor();
	private JestClient client;

	@Override
	public void close() {
		client.shutdownClient();
		threadPool.shutdown();
	}

	@Override
	public boolean requiresLayout() {
		return false;
	}

	@Override
	public void activateOptions() {
		try {
			HttpClientConfig clientConfig = new HttpClientConfig.Builder(
					appendPortToHost(host, port)).multiThreaded(true).build();
			JestClientFactory factory = new JestClientFactory();
			factory.setHttpClientConfig(clientConfig);
			client = factory.getObject();
		} catch (Exception ex) {
			errorHandler.error("Could not activate appender " + this.name);
		}
		super.activateOptions();
	}

	@Override
	protected void append(LoggingEvent loggingEvent) {
		if (isAsSevereAsThreshold(loggingEvent.getLevel())) {
			MessageProperties messageProperties = new MessageProperties(index,
					eventType, dateFormat);
			threadPool.submit(new WritingTask(loggingEvent, messageProperties,
					client));
		}

	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	private String appendPortToHost(String host, String port) {
		StringBuilder result = new StringBuilder(host);
		result.append(":");
		result.append(port);
		return result.toString();
	}

}
