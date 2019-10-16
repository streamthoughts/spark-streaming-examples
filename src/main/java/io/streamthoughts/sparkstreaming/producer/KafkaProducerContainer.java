package io.streamthoughts.sparkstreaming.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public final class KafkaProducerContainer<K, V> implements Serializable {

    public static <K, V>  KafkaProducerContainer<K, V> apply(final Map<String, Object> config) {
        KafkaProducerSupplier<K, V> f = () -> {
            var producer = new KafkaProducer<K, V>(config);
            Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
            return producer;
        };
        return new KafkaProducerContainer<>(f);
    }

    private final Supplier<KafkaProducer<K, V>> supplier;

    private volatile KafkaProducer<K, V> producer;

    private KafkaProducerContainer(final Supplier<KafkaProducer<K, V>> supplier) {
        this.supplier = supplier;
    }

    public Future<RecordMetadata> send(final String topic, final K key, final V value) {
        return getOrCreateProducer().send(new ProducerRecord<>(topic, key, value));
    }

    public Future<RecordMetadata> send(final String topic, final V value) {
        return getOrCreateProducer().send(new ProducerRecord<>(topic, value));
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

    @FunctionalInterface
    public interface KafkaProducerSupplier<K, V> extends Supplier<KafkaProducer<K, V>>, Serializable {

    }
}
