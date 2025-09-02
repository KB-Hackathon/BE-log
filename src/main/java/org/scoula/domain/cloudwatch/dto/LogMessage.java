package org.scoula.domain.cloudwatch.dto;

import lombok.Data;

@Data
public class LogMessage {
	private String logLevel;
	private String timestamp;
	private String txId;
	private String message;
	private Common data;
	private String errorMessage;
}
