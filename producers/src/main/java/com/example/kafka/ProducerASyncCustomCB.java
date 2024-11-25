package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerASyncCustomCB {
    public static final Logger logger= LoggerFactory.getLogger(ProducerASyncCustomCB.class.getName());
    public static void main(String[] args){


        String topicName = "multipart-topic";

        Properties props = new Properties();


//        props.setProperty("bootstrap.servers","192.168.1.163:9092");  이것도 된다. 아래것도 되고.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        for(int seq =0; seq<20; seq++ ) {

            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq,"iterator22 hello World new "+seq);
//            logger.info("seq = {}",seq);
            CustomCallBack callback = new CustomCallBack(seq);

            kafkaProducer.send(producerRecord, callback);

        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
            e.printStackTrace();
        }
        kafkaProducer.close();


    }

}
