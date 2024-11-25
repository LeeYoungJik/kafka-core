package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerASyncWithKey {
    public static final Logger logger= LoggerFactory.getLogger(ProducerASyncWithKey.class.getName());
    public static void main(String[] args){


        String topicName = "multipart-topic";

        Properties props = new Properties();


//        props.setProperty("bootstrap.servers","192.168.1.163:9092");  이것도 된다. 아래것도 되고.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for(int seq =0; seq<20; seq++ ) {

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq),"iterator22 hello World new "+seq);

//        Future<RecordMetadata> send = kafkaProducer.send(producerRecord);//이렇게 해도 되는데 보통은 아래 방법으로 한다.
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if (exception == null) {
                        logger.info("\n##### onCompletion received ### \n" +
                                "partition:" + recordMetadata.partition() + "\n" +
                                "offset : " + recordMetadata.offset() + "\n" +
                                "timestamp : " + recordMetadata.timestamp());
                    } else {
                        logger.error("exception error from broker " + exception.getMessage());
                    }
                }
            });

//            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
//                if (exception == null) {
//                    logger.info("\n##### onCompletion received ### \n" +
//                            "partition:" + recordMetadata.partition() + "\n" +
//                            "offset : " + recordMetadata.offset() + "\n" +
//                            "timestamp : " + recordMetadata.timestamp());
//                } else {
//                    logger.error("exception error from broker= " + exception.getMessage());
//                }
//            });
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
