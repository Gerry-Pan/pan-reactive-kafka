package cn.com.pan.kafka.annotation;

public @interface ReactiveTopicPartition {

	String topic();

	String[] partitions() default {};

}
