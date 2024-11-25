package com.practice.kafka.evnet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class FileEventHandler implements EvnetHandler {
    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());
    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvnet messageEvnet) throws InterruptedException, ExecutionException {
        //Producer
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvnet.key, messageEvnet.value);

        //callBack
        //sync
        if (this.sync) {
            RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
            logger.info("\n##### recordMetadata received ### \n" +
                    "partition:" + recordMetadata.partition() + "\n" +
                    "offset : " + recordMetadata.offset() + "\n" +
                    "timestamp : " + recordMetadata.timestamp());
        } else {
            this.kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("\n##### onCompletion received ### \n" +
                            "partition:" + recordMetadata.partition() + "\n" +
                            "offset : " + recordMetadata.offset() + "\n" +
                            "timestamp : " + recordMetadata.timestamp());
                } else {
                    logger.error("exception error from broker= " + exception.getMessage());
                }
            });

        }
    }

    public static void main(String[] args) {

        String topicName = "file-topic";
        String filePath = "/Users/uclick/Desktop/SpringBoot/토이프로젝트/KafkaProjcect_01/KafkaProject1/practice/src/main/resources/pizza_sample.txt";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // kafkaProducer 객체 생성 -> producerRecord생성 -> send() 비동기 방식 전송
        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer,topicName,false);

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
                MessageEvnet messageEvnet = new MessageEvnet(key,value.toString());
                fileEventHandler.onMessage(messageEvnet);
            }

        } catch (IOException e) {
            logger.info(e.getMessage());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }


}
