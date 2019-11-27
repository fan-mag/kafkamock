import javafx.util.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Application {
    static class Buffer {
        private static final List<Pair<String, String>> messages = new ArrayList<>();

        synchronized static List<Pair<String, String>> messages() {
            return messages;
        }

        synchronized static void putIntoBuffer(String key, String value) {
            /* Input queue actions */


            /*                     */
            messages.add(new Pair<>(key, value));
        }

        synchronized static Pair<String, String> getFromBuffer() {
            String key = messages.get(0).getKey();
            String value = messages.get(0).getValue();
            messages.remove(0);
            /* Output message actions */


            /*                        */
            return new Pair<>(key, value);
        }
    }

    static class ConsumerThread implements Runnable {
        private Consumer<String, String> consumer;

        ConsumerThread(Consumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            final int giveUp = Integer.parseInt((String) getInputProperty("poll_retries"));
            int noRecordsCount = 0;
            while (true) {
                final ConsumerRecords<String, String> consumerRecords =
                        consumer.poll(Duration.ofMillis(Long.parseLong(getInputProperty("poll_interval").toString())));
                if (consumerRecords.count() == 0 && noRecordsCount++ > giveUp)
                    break;
                else {
                    consumerRecords.forEach(record ->
                    {
                        System.out.printf("Consumer Record:(Key:%s, Value:%s, Partition:%d, Offset:%d)%n",
                                record.key(), record.value(),
                                record.partition(), record.offset());
                        Buffer.putIntoBuffer(record.key(), record.value());
                    });
                    consumer.commitAsync();
                }
            }
            consumer.close();
            System.out.println("Consumer done");
            stop = true;
        }
    }

    static class ProducerThread implements Runnable {
        private Producer<String, String> producer;
        private String topic;

        ProducerThread(Producer<String, String> producer, String topic) {
            this.producer = producer;
            this.topic = topic;
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(1);
                    if (!Buffer.messages().isEmpty()) {
                        Pair<String, String> pair = Buffer.getFromBuffer();
                        String key = pair.getKey();
                        String value = pair.getValue();
                        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                        RecordMetadata metadata = producer.send(record).get();
                        System.out.printf("Producer record:(Key:%s, Value:%s, Partition:%d, Offset:%d) %n",
                                key, value, metadata.partition(), metadata.offset());
                        Thread.sleep(Long.parseLong(getOutputProperty("pull_interval").toString()) - 1);
                        System.out.printf("Messages in the queue: %d%n", Buffer.messages().size());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }

            }
            producer.flush();
            producer.close();
            System.out.println("Producer done");
        }
    }

    private static final Properties kafkaInputProps = new Properties();
    private static final Properties kafkaOutputProps = new Properties();
    private static Consumer<String, String> consumer;
    private static Producer<String, String> producer;
    private static String producerTopic;
    private static volatile boolean stop = false;

    private static Object getInputProperty(String key) {
        return kafkaInputProps.get(key);
    }

    private static Object getOutputProperty(String key) {
        return kafkaOutputProps.get(key);
    }

    private static void init() throws IOException {
        kafkaInputProps.load(new FileReader("src/main/resources/kafka-input.properties"));
        kafkaOutputProps.load(new FileReader("src/main/resources/kafka-output.properties"));
        consumer = new KafkaConsumer<>(kafkaInputProps);
        consumer.subscribe(Collections.singletonList((String) getInputProperty("topic")));
        producer = new KafkaProducer<>(kafkaOutputProps);
        producerTopic = (String) getOutputProperty("topic");
    }

    private static void startConsumer() {
        ConsumerThread consumerThread = new ConsumerThread(consumer);
        Thread thread = new Thread(consumerThread);
        thread.start();
    }

    private static void startProducer() {
        ProducerThread producerThread = new ProducerThread(producer, producerTopic);
        Thread thread = new Thread(producerThread);
        thread.start();
    }

    public static void main(String[] args) throws IOException {
        init();
        startConsumer();
        startProducer();
    }
}
