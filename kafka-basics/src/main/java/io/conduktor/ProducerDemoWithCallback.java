package io.conduktor;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
    producer.send(producerRecord, new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        // executes everytime a record is successfully sent or an exception is thrown
        if (exception == null) {
          log.info("Received new metadata \n"
              .concat("Topic: " + metadata.topic() + "\n")
              .concat("Partition: " + metadata.partition() + "\n")
              .concat("Offset: " + metadata.offset() + "\n")
              .concat("Timestamp: " + metadata.timestamp() + "\n")
          );
        } else {
          log.error("Error while producing " + exception);
        }
      }
    });

    // flush data - synch
    producer.flush(); // block up to this line of code, until my producer sends the producer record.
    producer.close(); // This actually invokes producer.flush(). So the call above is redundant. Demo purposes.

  }
}
