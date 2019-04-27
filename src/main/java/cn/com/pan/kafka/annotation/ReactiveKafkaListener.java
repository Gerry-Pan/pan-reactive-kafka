package cn.com.pan.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ReactiveKafkaListener {

	String[] topics() default {};

	/**
	 * 暂不支持，由于ReceiverOptions.subscription(Pattern
	 * pattern)，只能订阅单个Pattern，若有多个ReactiveKafkaListener设置topicPattern，则不能订阅多个Pattern
	 * 
	 * @return
	 */
	String topicPattern() default "";

	ReactiveTopicPartition[] topicPartitions() default {};

}
