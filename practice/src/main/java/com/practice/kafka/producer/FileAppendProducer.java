package com.practice.kafka.producer;

import com.practice.kafka.evnet.EvnetHandler;
import com.practice.kafka.evnet.FileEventHandler;
import com.practice.kafka.evnet.FileEventSource;
import com.practice.kafka.evnet.MessageEvnet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileAppendProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class.getName());
    public static void main(String[] args) {

        String topicName = "file-topic";
        String filePath = "/Users/uclick/Desktop/SpringBoot/토이프로젝트/KafkaProjcect_01/KafkaProject1/practice/src/main/resources/pizza_append.txt";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        File file = new File(filePath);

        boolean sync = false;
        EvnetHandler evnetHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        FileEventSource fileEventSource = new FileEventSource(1000, file, evnetHandler);

        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }finally {
            kafkaProducer.close();
        }

    }


}
