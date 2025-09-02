package org.scoula.domain.cloudwatch.serviceImpl;

import org.scoula.domain.cloudwatch.dto.LogMessage;
import org.scoula.domain.cloudwatch.service.LogConsumerService;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;

@Slf4j
@Service
@RequiredArgsConstructor
public class LogConsumerServiceImpl implements LogConsumerService {

	private final CloudWatchLogsClient logsClient;
	private final ObjectMapper objectMapper = new ObjectMapper();

	private static final String LOG_GROUP = "Dolfin";

	@Override
	public void loggingIntegration(LogMessage logMessage) throws Exception {
		String logStream = "dolfin-log-stream";
		writeToCloudWatch(logMessage, logStream);
	}

	private void writeToCloudWatch(LogMessage logMessage, String logStream) throws Exception {
		ensureLogGroupAndStream(logStream);

		String logContent = objectMapper.writeValueAsString(logMessage);

		InputLogEvent logEvent = InputLogEvent.builder()
			.message(logContent)
			.timestamp(System.currentTimeMillis())
			.build();

		PutLogEventsRequest putRequest = PutLogEventsRequest.builder()
			.logGroupName(LOG_GROUP)
			.logStreamName(logStream)
			.logEvents(logEvent)
			.sequenceToken(getSequenceToken(logStream))
			.build();

		logsClient.putLogEvents(putRequest);
	}

	private void ensureLogGroupAndStream(String logStream) {
		try {
			logsClient.createLogGroup(CreateLogGroupRequest.builder()
				.logGroupName(LOG_GROUP).build());
		} catch (ResourceAlreadyExistsException ignored) {
		}

		try {
			logsClient.createLogStream(CreateLogStreamRequest.builder()
				.logGroupName(LOG_GROUP)
				.logStreamName(logStream)
				.build());
		} catch (ResourceAlreadyExistsException ignored) {
		}
	}

	private String getSequenceToken(String logStream) {
		DescribeLogStreamsResponse describe = logsClient.describeLogStreams(
			DescribeLogStreamsRequest.builder()
				.logGroupName(LOG_GROUP)
				.logStreamNamePrefix(logStream)
				.build()
		);

		return describe.logStreams().stream()
			.findFirst()
			.map(LogStream::uploadSequenceToken)
			.orElse(null);
	}
}