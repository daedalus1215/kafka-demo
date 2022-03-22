package wikimedia;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.launchdarkly.eventsource.EventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikimediaChangesProducer {

  public static void main(String[] args) throws InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    EventSource.Builder builder = new EventSource.Builder(
        new WikimediaChangeHandler(producer, "wikimedia.recentchange"),
        URI.create("https://stream.wikimedia.org/v2/stream/recentchange"));

    final EventSource eventSource = builder.build();
    eventSource.start();
    // we produce for 10 minutes and block the program until then
    TimeUnit.MINUTES.sleep(10);
  }
}
