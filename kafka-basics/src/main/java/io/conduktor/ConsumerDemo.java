package io.conduktor;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

  private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
  /**
   * If no other offsets are found, don't even start
   */
  private static final String OFFSET_NONE = "none";
  /**
   * Read from earliest (all historical)
   */
  private static final String OFFSET_EARLIEST = "earliest";
  /**
   * Read from now
   */
  private static final String OFFSET_LATEST = "latest";

  public static void main(String[] args) {
    log.info("I am a Kafka Consumer");
    // create consumer configs
    final Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(GROUP_ID_CONFIG, "my-second-app");
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, OFFSET_EARLIEST);
    // create consumer
    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    // consume the messages, by subscribing consumer to our topics
    consumer.subscribe(Collections.singletonList("demo_java"));
    // poll for new data
    while (true) {
      log.info("Polling");
      final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // will wait 100 milliseconds for records when polling.
      records.forEach(record -> {
        log.info("key: " + record.key() + ", Value: " + record.value());
        log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
      });
    }
  }
}
