package org.scoula.domain.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan(basePackages = {
	"org.scoula.domain.kafka",         // KafkaProducer, KafkaListener, KafkaConfig
	"org.scoula.domain.cloudwatch"     // CloudWatchService, CloudWatchConfig
})
@PropertySource("classpath:application.properties")
public class RootConfig {
}
