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

public class Application<KC, VC, KP, VP, KB, VB> {
    class Buffer {
        private final List<Pair<KB, VB>> messages = new ArrayList<>();

        synchronized List<Pair<KB, VB>> messages() {
            return messages;
        }

        synchronized void putIntoBuffer(KC key, VC value) {
            /* INPUT MESSAGE ACTIONS START */
            KB keyBuffer = (KB) key;
            VB valueBuffer = (VB) value;
            /* INPUT MESSAGE ACTIONS END */
            messages.add(new Pair<>(keyBuffer, valueBuffer));
        }

        synchronized Pair<KP, VP> getFromBuffer() {
            KB key = messages.get(0).getKey();
            VB value = messages.get(0).getValue();
            messages.remove(0);
            /* OUTPUT MESSAGE ACTIONS START */
            KP keyProd = (KP) key;
            VP valueProd = (VP) value;
            /* OUTPUT MESSAGE ACTIONS END */
            return new Pair<>(keyProd, valueProd);
        }
    }

    class ConsumerThread implements Runnable {
        private Consumer<KC, VC> consumer;

        ConsumerThread(Consumer<KC, VC> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            final int giveUp = Integer.parseInt((String) getInputProperty("poll_retries"));
            int noRecordsCount = 0;
            while (true) {
                final ConsumerRecords<KC, VC> consumerRecords =
                        consumer.poll(Duration.ofMillis(Long.parseLong(getInputProperty("poll_interval").toString())));
                if (consumerRecords.count() == 0 && noRecordsCount++ > giveUp)
                    break;
                else {
                    if (!consumerRecords.isEmpty()) noRecordsCount = 0;
                    consumerRecords.forEach(record ->
                    {
                        System.out.printf("Consumer Record:(Key:%s, Value:%s, Partition:%d, Offset:%d)%n",
                                record.key(), record.value(),
                                record.partition(), record.offset());
                        buffer.putIntoBuffer(record.key(), record.value());
                    });
                    consumer.commitAsync();
                }
            }
            consumer.close();
            System.out.println("Consumer done");
            stop = true;
        }
    }

    class ProducerThread implements Runnable {
        private Producer<KP, VP> producer;
        private String topic;

        ProducerThread(Producer<KP, VP> producer, String topic) {
            this.producer = producer;
            this.topic = topic;
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(1);
                    if (!buffer.messages().isEmpty()) {
                        Pair<KP, VP> pair = buffer.getFromBuffer();
                        KP key = pair.getKey();
                        VP value = pair.getValue();
                        final ProducerRecord<KP, VP> record = new ProducerRecord<>(topic, key, value);
                        RecordMetadata metadata = producer.send(record).get();
                        System.out.printf("Producer record:(Key:%s, Value:%s, Partition:%d, Offset:%d) %n",
                                key, value, metadata.partition(), metadata.offset());
                        Thread.sleep(Long.parseLong(getOutputProperty("pull_interval").toString()) - 1);
                        System.out.printf("Messages in the queue: %d%n", buffer.messages().size());
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

    private final Properties kafkaInputProps = new Properties();
    private final Properties kafkaOutputProps = new Properties();
    private volatile boolean stop = false;
    private Buffer buffer = new Buffer();

    private Object getInputProperty(String key) {
        return kafkaInputProps.get(key);
    }

    private Object getOutputProperty(String key) {
        return kafkaOutputProps.get(key);
    }

    public void startConsumer(String inputPropertiesPath) throws IOException {
        kafkaInputProps.load(new FileReader(inputPropertiesPath));
        Consumer<KC, VC> consumer = new KafkaConsumer<>(kafkaInputProps);
        consumer.subscribe(Collections.singletonList((String) getInputProperty("topic")));
        ConsumerThread consumerThread = new ConsumerThread(consumer);
        Thread thread = new Thread(consumerThread);
        thread.start();
    }

    public void startProducer(String outputPropertiesPath) throws IOException {
        kafkaOutputProps.load(new FileReader(outputPropertiesPath));
        Producer<KP, VP> producer = new KafkaProducer<>(kafkaOutputProps);
        String producerTopic = (String) getOutputProperty("topic");
        ProducerThread producerThread = new ProducerThread(producer, producerTopic);
        Thread thread = new Thread(producerThread);
        thread.start();
    }

    public static void main(String[] args) throws IOException {
        Application<String, String, String, Integer, String, String> app = new Application<>();
        app.startConsumer("src/main/resources/kafka-input.properties");
        app.startProducer("src/main/resources/kafka-output.properties");
    }
}
