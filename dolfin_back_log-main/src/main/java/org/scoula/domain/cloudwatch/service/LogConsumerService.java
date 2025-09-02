package org.scoula.domain.cloudwatch.service;

import org.scoula.domain.cloudwatch.dto.LogMessage;
import org.springframework.stereotype.Service;

@Service
public interface LogConsumerService {

	void loggingIntegration(LogMessage message) throws Exception;
}
