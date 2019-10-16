package io.streamthoughts.sparkstreaming;

import io.streamthoughts.sparkstreaming.producer.KafkaProducerContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Skeleton for a Spark Streaming application using Kafka DStream.
 */
public class SparkDStreamKafkaExample {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .setAppName("kafka-streams-demo")
            .setMaster("local[3]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-streaming-example");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singleton("input-topic");

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Broadcast<KafkaProducerContainer<String, String>> producer =
                ssc.sparkContext().broadcast(KafkaProducerContainer.apply(producerConfig));

        final JavaInputDStream<ConsumerRecord<String, String>> dstream =
            KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
            );

        dstream.foreachRDD( rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            rdd.foreachPartition( iterator -> {

                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    System.out.println(record);
                    producer.value().send("output-topic", record.key(), record.value());
                }
            });

            ((CanCommitOffsets) dstream.inputDStream()).commitAsync(offsetRanges);
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
