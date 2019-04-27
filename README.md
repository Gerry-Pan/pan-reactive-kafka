# reactive kafka
reactive kafka

```
@Bean
public ReactiveKafkaListenerAnnotationBeanPostProcessor reactiveKafkaListenerAnnotationBeanPostProcessor() {
  Map<String, Object> consumerProperties = new HashMap<String, Object>();
  consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers));
  consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
  consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
  consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
  consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

  return new ReactiveKafkaListenerAnnotationBeanPostProcessor(consumerProperties);
}
```

```
@ReactiveKafkaListener(topics = "receive")
public Mono<Void> receive(ReceiverRecord<String, String> receiverRecord) {
  System.out.println(receiverRecord);

  receiverRecord.receiverOffset().acknowledge();
  return Mono.empty();
}
```
