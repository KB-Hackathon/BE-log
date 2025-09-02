package org.scoula.domain.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class KafkaProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;

	public void sendToRetryTopic(String message) {
		kafkaTemplate.send("log-retry", message);
	}
}
