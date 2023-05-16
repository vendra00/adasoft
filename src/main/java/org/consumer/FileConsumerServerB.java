package org.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FileConsumerServerB {
    private static final String FOLDER_PATH_B = "C:\\Users\\gabri\\Downloads\\test\\server_b"; // Update with the actual folder path for Server B
    private static final String FOLDER_PATH_A = "C:\\Users\\gabri\\Downloads\\test\\server_a"; // Update with the actual folder path for Server A
    private static final String SERVER_IDENTIFIER = "B";

    public static void main(String[] args) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        while (!Thread.currentThread().isInterrupted()) {
            try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
                FileConsumer fileConsumerB = new FileConsumer(FOLDER_PATH_B, FOLDER_PATH_A, channel, SERVER_IDENTIFIER);
                fileConsumerB.start();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}




