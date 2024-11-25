package com.practice.kafka.evnet;

import java.util.concurrent.ExecutionException;

public class MessageEvnet {
    public String key;
    public String value;
    public MessageEvnet(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
