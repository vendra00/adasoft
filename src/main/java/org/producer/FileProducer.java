package org.producer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FileProducer {
    public static final String QUEUE_NAME = "file-queue";
    private final String folderPath;
    private final Channel channel;
    private final Set<String> processedFiles;

    public FileProducer(String folderPath, Channel channel) {
        this.folderPath = folderPath;
        this.channel = channel;
        this.processedFiles = new HashSet<>();
    }

    public void start() throws IOException, InterruptedException {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            Path folder = Paths.get(folderPath);
            folder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);

            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> eventKind = event.kind();
                    Path filePath = (Path) event.context();
                    String fileName = filePath.getFileName().toString();
                    Path absoluteFilePath = folder.resolve(filePath);

                    if (processedFiles.contains(fileName)) {
                        // Skip processing if the file has already been processed
                        continue;
                    }

                    if (eventKind == StandardWatchEventKinds.ENTRY_CREATE || eventKind == StandardWatchEventKinds.ENTRY_MODIFY) {
                        String action = (eventKind == StandardWatchEventKinds.ENTRY_CREATE) ? "ADD" : "MODIFY";
                        String fileContent = new String(Files.readAllBytes(absoluteFilePath));

                        // Publish to message queue
                        try {
                            publishToQueue(channel, fileName, fileContent, action);
                            System.out.println("Sent: " + fileName + ", Action: " + action);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        processedFiles.add(fileName);
                    } else if (eventKind == StandardWatchEventKinds.ENTRY_DELETE) {
                        String action = "DELETE";

                        // Publish to message queue
                        try {
                            publishToQueue(channel, fileName, "", action);
                            System.out.println("Sent: " + fileName + ", Action: " + action);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        processedFiles.add(fileName);
                    }
                }
                key.reset();
            }
        }
    }

    private void publishToQueue(Channel channel, String fileName, String fileContent, String action) throws IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .headers(
                        Map.of(
                                "fileName", fileName,
                                "action", action
                        )
                )
                .build();

        channel.basicPublish("", QUEUE_NAME, properties, createMessage(fileName, fileContent, action));
    }

    private byte[] createMessage(String fileName, String fileContent, String action) {
        return String.format("%s,%s", fileName, action).getBytes();
    }
}