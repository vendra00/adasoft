package org.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FileConsumerServerA {
    private static final String FOLDER_PATH_A = "C:\\Users\\gabri\\Downloads\\test\\server_a"; // Update with the actual folder path for Server A
    private static final String FOLDER_PATH_B = "C:\\Users\\gabri\\Downloads\\test\\server_b"; // Update with the actual folder path for Server B
    private static final String SERVER_IDENTIFIER = "A";

    public static void main(String[] args) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        while (!Thread.currentThread().isInterrupted()) {
            try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
                FileConsumer fileConsumerA = new FileConsumer(FOLDER_PATH_A, FOLDER_PATH_B, channel, SERVER_IDENTIFIER);
                fileConsumerA.start();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}








