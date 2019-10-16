package io.streamthoughts.sparkstreaming.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkEnv;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * KafkaProducerContainer.
 *
 * Simple usages is :
 * <pre>
 *  Map<String, Object> config = new HashMap<>();
 *  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *  config.put(ProducerConfig.ACKS_CONFIG, "all");
 *  config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
 *  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
 *
 *  Broadcast<KafkaProducerContainer<String, String>> producer = ssc
 *      .sparkContext()
 *      .broadcast(KafkaProducerContainer.apply(producerConfig));
 * </pre>
 *
 * @param <K>   the record-key type.
 * @param <V>   the record value type
 */
public final class KafkaProducerContainer<K, V> implements Serializable {

    private static final String PRODUCER_CLIENT_PREFIX = "producer-spark-exec-";

    @FunctionalInterface
    public interface KafkaProducerSupplier<K, V> extends Supplier<KafkaProducer<K, V>>, Serializable { }

    /**
     * Creates a new {@link KafkaProducer} container that can be broadcast using the spark context.
     *
     * @param config    the producer config.
     * @param <K>       the record-key type.
     * @param <V>       the record-value type.
     *
     * @return          a new {@link KafkaProducerSupplier} instance.
     */
    public static <K, V>  KafkaProducerContainer<K, V> apply(final Map<String, Object> config) {
        Objects.requireNonNull(config, "config cannot be null");
        KafkaProducerSupplier<K, V> f = () -> {
            // Can be useful for debugging and auditing on kafka broker side.
            config.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_PREFIX + SparkEnv.get().executorId());

            var producer = new KafkaProducer<K, V>(config);
            Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
            return producer;
        };
        return new KafkaProducerContainer<>(f);
    }

    private final Supplier<KafkaProducer<K, V>> supplier;

    private volatile KafkaProducer<K, V> producer;

    /**
     * Creates a new {@link KafkaProducerContainer} instance.
     *
     * @param supplier  the supplier which is used to lazily initialize the {@link KafkaProducer} instance.
     */
    private KafkaProducerContainer(final Supplier<KafkaProducer<K, V>> supplier) {
        this.supplier = supplier;
    }

    /**
     * Asynchronously send a record to the a topic with the specified value.
     *
     * @param topic     the topic name.
     * @param value     the record value.
     *
     * @return          the {@link RecordMetadata} future.
     */
    public Future<RecordMetadata> send(final String topic, final V value) {
        return send(topic, null, value, null);
    }

    /**
     * Asynchronously send a record to the a topic with the specified value.
     *
     * @param topic     the topic name.
     * @param key       the record key.
     * @param value     the record value.
     *
     * @return          the {@link RecordMetadata} future.
     */
    public Future<RecordMetadata> send(final String topic, final K key, final V value) {
        return send(topic, key, value, null);
    }

    /**
     * Asynchronously send a record to the specified topic and invoke the provided callback
     * when the send has been acknowledged.
     *
     * @param topic     the topic name.
     * @param key       the record key.
     * @param value     the record value.
     * @param callback  the {@link Callback} instance.
     *
     * @return          the {@link RecordMetadata} future.
     */
    public Future<RecordMetadata> send(final String topic,
                                       final K key,
                                       final V value,
                                       final Callback callback) {

        return getOrCreateProducer().send(new ProducerRecord<>(topic, key, value), callback);
    }

    private KafkaProducer<K, V> getOrCreateProducer() {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    producer = supplier.get();
                }
            }
        }
        return producer;
    }
}
