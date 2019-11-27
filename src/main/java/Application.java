import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {
    static class ConsumerThread implements Runnable {
        private Consumer<Long, String> consumer;

        ConsumerThread(Consumer<Long, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            final int giveUp = Integer.parseInt((String) get("poll_retries"));
            int noRecordsCount = 0;

            while (true) {
                final ConsumerRecords<Long, String> consumerRecords =
                        consumer.poll(Duration.ofMillis(Long.parseLong(get("poll_interval").toString())));
                if (consumerRecords.count() == 0 && noRecordsCount++ > giveUp)
                    break;
                else {
                    consumerRecords.forEach(record ->
                            System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset()));
                    consumer.commitAsync();
                }
            }
            consumer.close();
            System.out.println("DONE");
        }
    }

    private static final Properties kafkaInputProps = new Properties();
    private static Consumer<Long, String> consumer;

    private static Object get(String key) {
        return kafkaInputProps.get(key);
    }

    private static void init() throws IOException {
        kafkaInputProps.load(new FileReader("src/main/resources/kafka-input.properties"));
        consumer = new KafkaConsumer<>(kafkaInputProps);
        consumer.subscribe(Collections.singletonList((String) get("topic")));
    }

    private static void startConsumer() {
        ConsumerThread consumerThread = new ConsumerThread(consumer);
        Thread thread = new Thread(consumerThread);
        thread.start();
    }

    public static void main(String[] args) throws IOException {
        init();
        startConsumer();
    }


}
