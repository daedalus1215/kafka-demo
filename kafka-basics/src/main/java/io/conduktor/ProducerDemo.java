package io.conduktor;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

  public static void main(String[] args) {
    log.info("yes");
    // create Producer Properties
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the Producer
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // create a producer record
    final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

    // send data - async
    producer.send(producerRecord);

    // flush data - synch
    producer.flush(); // block up to this line of code, until my producer sends the producer record.
    producer.close(); // This actually invokes producer.flush(). So the call above is redundant. Demo purposes.

  }
}
