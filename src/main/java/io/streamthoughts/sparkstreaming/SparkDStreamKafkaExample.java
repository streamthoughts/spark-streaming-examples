package io.streamthoughts.sparkstreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
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
        kafkaParams.put("bootstrap.servers", "localhost:9092,");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-streaming-example");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singleton("streams-plaintext-input");

        final JavaInputDStream<ConsumerRecord<String, String>> dstream =
            KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                // Fist, Spark will create one Kafka Consumer on driver (DirectKafkaInputDStream#start:260)
                // On batch interval, the DStreams consumer is used to lookup latest offset (DirectKafkaInputDStream#latestOffsets).
                // Then, one consumer will be created per Kafka partition (task) using KafkaConsumer#assign().
                ConsumerStrategies.Subscribe(topics, kafkaParams)
            );

        // RDD is instance of KafkaRDD
        dstream.foreachRDD( rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            // iterator is instance of KafkaRDDIterator
            rdd.foreachPartition( iterator -> {

                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    System.out.println(record);
                    // For producing into Kafka see : https://stackoverflow.com/questions/31590592/spark-streaming-read-and-write-on-kafka-topic
                }
            });

            // Commit async to ensure no data-loss.
            // Note : business logic should support duplicates (at-least once semantic).
            ((CanCommitOffsets) dstream.inputDStream()).commitAsync(offsetRanges);
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
