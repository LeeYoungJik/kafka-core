package com.practice.kafka.evnet;

import java.util.concurrent.ExecutionException;

public interface EvnetHandler {

    void onMessage(MessageEvnet messageEvnet) throws InterruptedException, ExecutionException;

}
