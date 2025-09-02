package org.scoula.domain.kafka.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.scoula.domain.cloudwatch.dto.LogMessage;
import org.scoula.domain.cloudwatch.service.LogConsumerService;
import org.scoula.domain.kafka.producer.KafkaProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLogListener {

	private final LogConsumerService logConsumerService;
	private final KafkaProducer kafkaProducer;
	private final ObjectMapper objectMapper = new ObjectMapper();


	@KafkaListener(topics = "log-module", groupId = "log-consumer-group")
	public void listen(String message, Acknowledgment ack) {
		try {
			LogMessage logMessage = objectMapper.readValue(message, LogMessage.class);
			logConsumerService.loggingIntegration(logMessage);
			ack.acknowledge();
		}catch (Exception e) {
			log.error(e.getMessage());
			kafkaProducer.sendToRetryTopic(message);
			ack.acknowledge();
		}
	}

	@KafkaListener(topics = "log-retry", groupId = "log-retry-group")
	public void listenRetry(String message, Acknowledgment ack) {
		try {
			LogMessage logMessage = objectMapper.readValue(message, LogMessage.class);
			//todo retry count > 10 have to go DLQ
			logConsumerService.loggingIntegration(logMessage);
			ack.acknowledge();
		}catch (Exception e) {
			log.error(e.getMessage());
			kafkaProducer.sendToRetryTopic(message);
			ack.acknowledge();
		}
	}
}

