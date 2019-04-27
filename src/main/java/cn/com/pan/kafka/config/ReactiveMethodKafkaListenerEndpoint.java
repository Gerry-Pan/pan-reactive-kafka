package cn.com.pan.kafka.config;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.springframework.util.Assert;

import lombok.Getter;
import lombok.Setter;

public class ReactiveMethodKafkaListenerEndpoint implements ReactiveKafkaListenerEndpoint {

	@Setter
	@Getter
	private Object bean;

	@Setter
	@Getter
	private Method method;

	private final Collection<String> topics = new ArrayList<String>();

	@Setter
	@Getter
	private Pattern topicPattern;

	private Collection<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();

	public void setTopics(String... topics) {
		Assert.notNull(topics, "'topics' must not be null");
		this.topics.clear();
		this.topics.addAll(Arrays.asList(topics));
	}

	public Collection<String> getTopics() {
		return Collections.unmodifiableCollection(this.topics);
	}

	public Collection<TopicPartition> getTopicPartitions() {
		return Collections.unmodifiableCollection(topicPartitions);
	}

	public void setTopicPartitions(TopicPartition... topicPartitions) {
		Assert.notNull(topicPartitions, "'topicPartitions' must not be null");
		this.topicPartitions.clear();
		this.topicPartitions.addAll(Arrays.asList(topicPartitions));
	}

	public void setTopicPartitions(Collection<TopicPartition> topicPartitions) {
		Assert.notNull(topicPartitions, "'topicPartitions' must not be null");
		this.topicPartitions.clear();
		this.topicPartitions.addAll(topicPartitions);
	}

}
