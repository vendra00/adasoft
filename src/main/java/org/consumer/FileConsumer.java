package org.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

public class FileConsumer {
    private final String folderPath;
    private final String destinationFolderPath;
    private final Channel channel;
    private static final String QUEUE_NAME = "file-queue";
    private final String serverIdentifier;

    public FileConsumer(String folderPath, String destinationFolderPath, Channel channel, String serverIdentifier) {
        this.folderPath = folderPath;
        this.destinationFolderPath = destinationFolderPath;
        this.channel = channel;
        this.serverIdentifier = serverIdentifier;
    }

    public void start() throws IOException {
        channel.basicQos(1);
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties properties = delivery.getProperties();
            Map<String, Object> headers = properties.getHeaders();

            if (headers != null) {
                String fileName = headers.get("fileName").toString();
                String action = headers.get("action").toString();

                // Check if the message is from the same server
                if (headers.containsKey("sourceIdentifier") && headers.get("sourceIdentifier").equals(serverIdentifier)) {
                    // Skip processing the message to avoid a loop
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    return;
                }

                String fileContent = new String(delivery.getBody());

                // Save the file
                Path filePath = Paths.get(folderPath, fileName);
                try {
                    Files.write(filePath, fileContent.getBytes());
                    System.out.println("Received and saved: " + fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // Copy to destination folder
                Path destinationPath = Paths.get(destinationFolderPath, fileName);
                try {
                    Files.copy(filePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
                    System.out.println("Copied to destination folder: " + fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // Publish the message with the source identifier
                try {
                    String newSourceIdentifier = serverIdentifier.equals("A") ? "B" : "A";
                    publishToQueue(channel, fileName, fileContent, action, newSourceIdentifier);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } else {
                System.out.println("Received a message without headers");
            }
        };

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
        //System.out.println("Consumer started. Waiting for messages...");
    }

    private void publishToQueue(Channel channel, String fileName, String fileContent, String action, String sourceIdentifier) throws IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .headers(
                        Map.of(
                                "fileName", fileName,
                                "action", action,
                                "sourceIdentifier", sourceIdentifier
                        )
                )
                .build();

        channel.basicPublish("", QUEUE_NAME, properties, fileContent.getBytes());
    }
}