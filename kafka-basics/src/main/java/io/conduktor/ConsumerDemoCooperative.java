package io.conduktor;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoCooperative {

  private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);
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
    properties.setProperty(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    // create consumer
    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    // get a reference to the currentr thread
    final Thread mainThread = Thread.currentThread();
    // adding the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        log.info("detected a shutdown. Exiting with consumer.wakeup()...");
        consumer.wakeup(); // this will break out of the while loop
        // join the main thread to allow the execution of the code in the main thread
        try {
          mainThread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    try {
      // consume the messages, by subscribing consumer to our topics
      consumer.subscribe(Collections.singletonList("demo_java"));
      // poll for new data
      while (true) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // will wait 100 milliseconds for records when polling.
        records.forEach(record -> {
          log.info("key: " + record.key() + ", Value: " + record.value());
          log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        });
      }
    } catch (WakeupException e) {
      log.info("wake up exception, this is expected");
    } catch (Exception e) {
      log.error("Unexpected exception");
    } finally {
      consumer.close();
    }
  }
}
