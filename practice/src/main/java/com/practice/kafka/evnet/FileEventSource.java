package com.practice.kafka.evnet;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {
    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(FileEventSource.class.getName());
    public boolean keepRunning = true;
    private int updateInterval;
    private File file;
    private long filePointer = 0; // 파일의 위치 어디까지 읽었는지.
    private EvnetHandler evnetHandler;

    public FileEventSource(int updateInterval, File file, EvnetHandler evnetHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.evnetHandler = evnetHandler;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);
                //file의 크기 계산 -> 파일 크기의 증가를 감지하기 위해서
                long len = this.file.length();

                if (len < this.filePointer) {
                    logger.info("file was reset as filePointer is longer than file length");
                    filePointer = len;
                } else if (len > this.filePointer) {       //filePointer이전의 읽었던 file의 length
                    readAppendAndSend();
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file,"r");  // 모드를 r로 read만 하겠다.
        raf.seek(this.filePointer);  //여기서 부터 읽는다.
        String line = null;

        while((line = raf.readLine()) != null){
            sendMessage(line);
        }
        //file이 변경 되었으니까, file의 filePointer를 현재 file의 마지막으로재 설정한다.
        this.filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String[] tokens = line.split(",");
        String key = tokens[0];
        StringBuffer value = new StringBuffer();

        for(int i =1; i<tokens.length; i++){
            if(i != (tokens.length-1)) {
                value.append(tokens[i] + ",");
            }else{
                value.append(tokens[i]);
            }
        }
        MessageEvnet messageEvnet = new MessageEvnet(key, value.toString());
        this.evnetHandler.onMessage(messageEvnet);
    }
}
