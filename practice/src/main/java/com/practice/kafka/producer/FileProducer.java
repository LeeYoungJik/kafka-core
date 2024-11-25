package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {
    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "file-topic";
        String filePath = "/Users/uclick/Desktop/SpringBoot/토이프로젝트/KafkaProjcect_01/KafkaProject1/practice/src/main/resources/pizza_sample.txt";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // kafkaProducer 객체 생성 -> producerRecord생성 -> send() 비동기 방식 전송

        sendFileMessages(kafkaProducer, topicName, filePath);


        kafkaProducer.close();

    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimiter =",";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            //ex) P001, order_id:ord0, shop:P001, pizza_name:Cheese Pizza, customer_name:Erick Koelpin, phone_number:(235) 592-3785 x9190, address:6373 Gulgowski Path, time:2024-11-01 16:39:27
            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for(int i =1; i<tokens.length; i++){
                    if(i != (tokens.length-1)) {
                        value.append(tokens[i] + ",");
                    }else{
                        value.append(tokens[i]);
                    }
                }
                sendMessage(kafkaProducer, topicName, key, value.toString());
            }

        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,key, value);
        logger.info("key :{}, value :{}",key,value);

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if(exception == null){
                logger.info("record metadata received \n" +
                        "partition: "+metadata.partition()+"\n" +
                        "offset: "+metadata.offset()+ "\n" +
                        "timestamp: "+metadata.timestamp());
            }else{
                logger.error("exception error from broker "+exception.getMessage());
            }
        });




//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>()






    }


}
