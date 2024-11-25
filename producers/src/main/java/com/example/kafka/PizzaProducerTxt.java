package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;


public class PizzaProducerTxt {
    public static final Logger logger= LoggerFactory.getLogger(PizzaProducerTxt.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMills, int intervalMillis
                                        , int intervalCount, boolean sync){//메인에서 호출할거라 static

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq != iterCount){
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                    pMessage.get("key"),pMessage.get("message"));
            sendMessage(kafkaProducer,producerRecord,pMessage,sync);

            if((intervalCount > 0) && (iterSeq % intervalCount == 0)){
                try {
                    logger.info("##### intervalCount "+intervalCount+"intervalMills = "+interIntervalMills);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            if(interIntervalMills > 0){
                try {
                    logger.info("interintervalMills = "+interIntervalMills);
                    Thread.sleep(interIntervalMills);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }

        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String>pMessage, boolean sync){

        String filePath = "/Volumes/home/cafe/docker/test/pizza_sample.txt"; // 파일 이름과 경로 설정
//        String content = ""; // 파일에 이어서 쓸 내용

        if(!sync) {

            String key = pMessage.get("key");
            String message =pMessage.get("message");

            String writeTxt = String.format("%s, %s",key,message);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) { // append 모드(true)
                writer.write(writeTxt); // 파일에 내용 이어서 쓰기
                writer.newLine();
//                System.out.println("Content appended successfully to " + filePath);
            } catch (IOException e) {
//            throw new RuntimeException(e);
                e.printStackTrace();
            }


            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if (exception == null) {
                        logger.info("call back async meeesage: " + pMessage.get("key")+"partition:" + recordMetadata.partition() +
                                "offset : " + recordMetadata.offset());
                    } else {
                        logger.error("exception error from broker " + exception.getMessage());
                    }
                }
            });
        }else {

            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("async meeesage: " + pMessage.get("key")+"partition:" + metadata.partition() +
                        "offset : " + metadata.offset());
            } catch (InterruptedException e) {  //스레드다보니까.
                e.printStackTrace();
//            throw new RuntimeException(e);
            } catch (ExecutionException e) {  //동기화 작업이다보니까.
                e.printStackTrace();
//            throw new RuntimeException(e);
            }

        }

    }


    public static void main(String[] args){

        String topicName = "pizza-topic";

        Properties props = new Properties();

//        props.setProperty("bootstrap.servers","192.168.1.163:9092");  이것도 된다. 아래것도 되고.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"6");
//        props.setProperty(ProducerConfig.ACKS_CONFIG,"all");z
//        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");


//        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"50000");
//        props.setProperty(ProducerConfig.ACKS_CONFIG,"0");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        sendPizzaMessage(kafkaProducer,topicName,
                -1,1,0,0,false);

        kafkaProducer.close();
    }

}
