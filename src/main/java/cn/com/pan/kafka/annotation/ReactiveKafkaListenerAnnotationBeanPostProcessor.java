package cn.com.pan.kafka.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import cn.com.pan.kafka.config.ReactiveMethodKafkaListenerEndpoint;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.concurrent.Queues;

@SuppressWarnings("unchecked")
public class ReactiveKafkaListenerAnnotationBeanPostProcessor
		implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

	private BeanFactory beanFactory;

	private BeanExpressionContext expressionContext;

	private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

	private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

	private final List<String> allTopics = new ArrayList<String>();

	private final Collection<TopicPartition> allTopicPartitions = new ArrayList<TopicPartition>();

	private final List<ReactiveMethodKafkaListenerEndpoint> endpointList = new ArrayList<ReactiveMethodKafkaListenerEndpoint>();

	private FluxProcessor<ReceiverRecord<? extends Object, ? extends Object>, ReceiverRecord<? extends Object, ? extends Object>> kafkaProcessor = null;

	private Map<String, Object> consumerProperties = null;

	public ReactiveKafkaListenerAnnotationBeanPostProcessor(Map<String, Object> consumerProperties) {
		Assert.notNull(consumerProperties, "consumerProperties must not be null.");
		this.consumerProperties = consumerProperties;
		this.kafkaProcessor = TopicProcessor.create("kafkaProcessor", Queues.SMALL_BUFFER_SIZE);
	}

	public ReactiveKafkaListenerAnnotationBeanPostProcessor(Map<String, Object> consumerProperties,
			FluxProcessor<ReceiverRecord<? extends Object, ? extends Object>, ReceiverRecord<? extends Object, ? extends Object>> kafkaProcessor) {
		Assert.notNull(consumerProperties, "consumerProperties must not be null.");
		Assert.notNull(kafkaProcessor, "kafkaProcessor must not be null.");

		this.consumerProperties = consumerProperties;
		this.kafkaProcessor = kafkaProcessor;
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);

			Map<Method, Set<ReactiveKafkaListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					new MethodIntrospector.MetadataLookup<Set<ReactiveKafkaListener>>() {

						@Override
						public Set<ReactiveKafkaListener> inspect(Method method) {
							Set<ReactiveKafkaListener> listenerMethods = findListenerAnnotations(method);
							return (!listenerMethods.isEmpty() ? listenerMethods : null);
						}

					});

			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
			} else {
				for (Map.Entry<Method, Set<ReactiveKafkaListener>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (ReactiveKafkaListener listener : entry.getValue()) {
						Method methodToUse = checkProxy(method, bean);
						ReactiveMethodKafkaListenerEndpoint endpoint = null;

						String[] topics = resolveTopics(listener);
						Collection<TopicPartition> topicPartitions = resolveTopicPartitions(listener);

						if (topics != null && topics.length > 0) {
							allTopics.addAll(Arrays.asList(topics));

							if (endpoint == null) {
								endpoint = new ReactiveMethodKafkaListenerEndpoint();
							}

							endpoint.setTopics(topics);
						}

						if (topicPartitions != null && topicPartitions.size() > 0) {
							allTopicPartitions.addAll(topicPartitions);

							if (endpoint == null) {
								endpoint = new ReactiveMethodKafkaListenerEndpoint();
							}

							endpoint.setTopicPartitions(topicPartitions);
						}

						if (endpoint != null) {
							endpoint.setBean(bean);
							endpoint.setMethod(methodToUse);

							endpointList.add(endpoint);
						}
					}
				}
			}
		}

		return bean;
	}

	protected ReactiveMethodKafkaListenerEndpoint handleMapping(
			ReceiverRecord<? extends Object, ? extends Object> receiverRecord) {
		try {
			String topic = receiverRecord.topic();
			int partition = receiverRecord.partition();

			for (ReactiveMethodKafkaListenerEndpoint endpoint : endpointList) {
				boolean isMatch = false;
				Collection<TopicPartition> topicPartitions = endpoint.getTopicPartitions();
				if (topicPartitions != null && topicPartitions.size() > 0) {
					for (TopicPartition topicPartition : topicPartitions) {
						if (topic.equals(topicPartition.topic()) && partition == topicPartition.partition()) {
							isMatch = true;
							break;
						}
					}
				}

				if (isMatch) {
					return endpoint;
				}

				Collection<String> topics = endpoint.getTopics();
				if (topics.contains(topic)) {
					isMatch = true;
				}

				if (isMatch) {
					return endpoint;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory,
					new ListenerScope());
		}
	}

	@Override
	public void afterSingletonsInstantiated() {
		ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProperties);

		if (allTopics != null && allTopics.size() > 0) {
			receiverOptions.subscription(allTopics);
		}

		if (allTopicPartitions != null && allTopicPartitions.size() > 0) {
			receiverOptions.assignment(allTopicPartitions);
		}

		KafkaReceiver.create(receiverOptions).receive().subscribe(kafkaProcessor::onNext, e -> {
			e.printStackTrace();
		});

		Flux.from(kafkaProcessor.replay(0).autoConnect()).flatMap(receiverRecord -> {
			ReactiveMethodKafkaListenerEndpoint endpoint = handleMapping(receiverRecord);

			if (endpoint == null || endpoint.getMethod() == null || endpoint.getBean() == null) {
				return Mono.empty();
			}

			Object invoke = null;

			try {
				Method method = endpoint.getMethod();

				List<Object> args = new LinkedList<Object>();
				Class<?>[] clazzList = method.getParameterTypes();

				for (Class<?> clazz : clazzList) {
					if (clazz.equals(ReceiverRecord.class)) {
						args.add(receiverRecord);
					} else {
						args.add(null);
					}
				}

				invoke = ReflectionUtils.invokeMethod(endpoint.getMethod(), endpoint.getBean(),
						args.toArray(new Object[args.size()]));
			} catch (Exception e) {
				e.printStackTrace();
				return Mono.empty();
			}

			if (invoke == null) {
				return Mono.empty();
			}

			if (invoke instanceof Flux) {
				return ((Flux<Object>) invoke).onErrorResume(e -> {
					e.printStackTrace();
					return Mono.empty();
				});
			} else if (invoke instanceof Mono) {
				return ((Mono<Object>) invoke).onErrorResume(e -> {
					e.printStackTrace();
					return Mono.empty();
				});
			} else {
				return Mono.just(invoke).onErrorResume(e -> {
					e.printStackTrace();
					return Mono.empty();
				});
			}
		}).subscribe();
	}

	private Set<ReactiveKafkaListener> findListenerAnnotations(Method method) {
		Set<ReactiveKafkaListener> listeners = new HashSet<>();
		ReactiveKafkaListener ann = AnnotatedElementUtils.findMergedAnnotation(method, ReactiveKafkaListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		return listeners;
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					} catch (NoSuchMethodException noMethod) {
					}
				}
			} catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			} catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"@ReactiveKafkaListener method '%s' found on bean target class '%s', "
								+ "but not found in any interface(s) for bean JDK proxy. Either "
								+ "pull the method up to an interface or switch to subclass (CGLIB) "
								+ "proxies by setting proxy-target-class/proxyTargetClass " + "attribute to 'true'",
						method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	protected Collection<TopicPartition> resolveTopicPartitions(ReactiveKafkaListener kafkaListener) {
		ReactiveTopicPartition[] topicPartitions = kafkaListener.topicPartitions();

		if (topicPartitions.length > 0) {
			List<TopicPartition> list = new ArrayList<TopicPartition>();
			for (ReactiveTopicPartition reactiveTopicPartition : topicPartitions) {
				String[] partitions = reactiveTopicPartition.partitions();
				for (String partition : partitions) {
					TopicPartition topicPartition = new TopicPartition(reactiveTopicPartition.topic(),
							resolveExpressionAsInteger(partition, "partitions"));

					list.add(topicPartition);
				}
			}

			return list;
		}

		return null;
	}

	protected String[] resolveTopics(ReactiveKafkaListener kafkaListener) {
		String[] topics = kafkaListener.topics();
		List<String> result = new ArrayList<>();
		if (topics.length > 0) {
			for (int i = 0; i < topics.length; i++) {
				Object topic = resolveExpression(topics[i]);
				resolveAsString(topic, result);
			}
		}
		return result.toArray(new String[result.size()]);
	}

	protected Pattern resolvePattern(ReactiveKafkaListener kafkaListener) {
		Pattern pattern = null;
		String text = kafkaListener.topicPattern();
		if (StringUtils.hasText(text)) {
			Object resolved = resolveExpression(text);
			if (resolved instanceof Pattern) {
				pattern = (Pattern) resolved;
			} else if (resolved instanceof String) {
				pattern = Pattern.compile((String) resolved);
			} else if (resolved != null) {
				throw new IllegalStateException(
						"topicPattern must resolve to a Pattern or String, not " + resolved.getClass());
			}
		}
		return pattern;
	}

	protected void resolveAsString(Object resolvedValue, List<String> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolveAsString(object, result);
			}
		} else if (resolvedValue instanceof String) {
			result.add((String) resolvedValue);
		} else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolveAsString(object, result);
			}
		} else {
			throw new IllegalArgumentException(
					String.format("@KafKaListener can't resolve '%s' as a String", resolvedValue));
		}
	}

	protected String resolveExpressionAsString(String value, String attribute) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String) {
			return (String) resolved;
		} else if (resolved != null) {
			throw new IllegalStateException("The [" + attribute + "] must resolve to a String. " + "Resolved to ["
					+ resolved.getClass() + "] for [" + value + "]");
		}
		return null;
	}

	protected Integer resolveExpressionAsInteger(String value, String attribute) {
		Object resolved = resolveExpression(value);
		Integer result = null;
		if (resolved instanceof String) {
			result = Integer.parseInt((String) resolved);
		} else if (resolved instanceof Number) {
			result = ((Number) resolved).intValue();
		} else if (resolved != null) {
			throw new IllegalStateException(
					"The [" + attribute + "] must resolve to an Number or a String that can be parsed as an Integer. "
							+ "Resolved to [" + resolved.getClass() + "] for [" + value + "]");
		}
		return result;
	}

	protected Boolean resolveExpressionAsBoolean(String value, String attribute) {
		Object resolved = resolveExpression(value);
		Boolean result = null;
		if (resolved instanceof Boolean) {
			result = (Boolean) resolved;
		} else if (resolved instanceof String) {
			result = Boolean.parseBoolean((String) resolved);
		} else if (resolved != null) {
			throw new IllegalStateException(
					"The [" + attribute + "] must resolve to a Boolean or a String that can be parsed as a Boolean. "
							+ "Resolved to [" + resolved.getClass() + "] for [" + value + "]");
		}
		return result;
	}

	private Object resolveExpression(String value) {
		return this.resolver.evaluate(resolve(value), this.expressionContext);
	}

	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	protected static class ListenerScope implements Scope {

		private final Map<String, Object> listeners = new HashMap<>();

		ListenerScope() {
			super();
		}

		public void addListener(String key, Object bean) {
			this.listeners.put(key, bean);
		}

		public void removeListener(String key) {
			this.listeners.remove(key);
		}

		@Override
		public Object get(String name, ObjectFactory<?> objectFactory) {
			return this.listeners.get(name);
		}

		@Override
		public Object remove(String name) {
			return null;
		}

		@Override
		public void registerDestructionCallback(String name, Runnable callback) {
		}

		@Override
		public Object resolveContextualObject(String key) {
			return this.listeners.get(key);
		}

		@Override
		public String getConversationId() {
			return null;
		}

	}

}
