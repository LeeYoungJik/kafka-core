package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallBack implements Callback {
    public static final Logger logger= LoggerFactory.getLogger(CustomCallBack.class.getName());
    private int seq;
    public CustomCallBack(int seq) {
        this.seq=seq;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        {
            if (exception == null) {
                logger.info("seq = {} / partition = {} / offset = {}",seq,recordMetadata.partition(),recordMetadata.offset());
            } else {
                logger.error("exception error from broker " + exception.getMessage());
            }
        };
    }
}
