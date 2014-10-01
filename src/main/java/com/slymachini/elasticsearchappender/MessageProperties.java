package com.slymachini.elasticsearchappender;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * DTO class, which contains properties of the message, which will be specified
 * in elastic search URL: e.g. http://127.0.0.1:9200/index/eventType
 * 
 * @author ashykh
 * 
 */
class MessageProperties {
	private String index;
	private String eventType;

	MessageProperties(String index, String eventType, String dateFormat) {
		this.index = index;
		this.eventType = eventType;
		if (dateFormat != null) {
			index = appendDateToIndex(index, dateFormat);
		}
	}

	String getIndex() {
		return index;
	}

	String getEventType() {
		return eventType;
	}

	private String appendDateToIndex(String index, String dateFormat) {
		StringBuilder result = new StringBuilder(index);
		result.append("-");
		result.append(new SimpleDateFormat(dateFormat).format(new Date()));
		return result.toString();
	}
}
