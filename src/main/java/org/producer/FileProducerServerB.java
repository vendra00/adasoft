package org.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.producer.FileProducer.QUEUE_NAME;

public class FileProducerServerB {
    private static final String FOLDER_PATH_B = "C:\\Users\\gabri\\Downloads\\test\\server_b"; // Update with the actual folder path for Server B
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDelete(QUEUE_NAME);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            FileProducer fileProducerB = new FileProducer(FOLDER_PATH_B, channel);
            fileProducerB.start();
        }
    }
}



