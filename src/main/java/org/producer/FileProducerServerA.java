package org.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.producer.FileProducer.QUEUE_NAME;

public class FileProducerServerA {
    private static final String FOLDER_PATH_A = "C:\\Users\\gabri\\Downloads\\test\\server_a"; // Update with the actual folder path for Server A
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDelete(QUEUE_NAME);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);


            FileProducer fileProducerA = new FileProducer(FOLDER_PATH_A, channel);
            fileProducerA.start();
        }
    }
}




