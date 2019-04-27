package cn.com.pan.kafka.config;

import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;

public interface ReactiveKafkaListenerEndpoint {

	Collection<String> getTopics();

	Pattern getTopicPattern();

	Collection<TopicPartition> getTopicPartitions();

}
