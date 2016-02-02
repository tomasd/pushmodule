package com.drencak.push.server;

import java.util.Queue;

public class QueuePublisher implements StatePublisher {
    private final Queue<Message> queue;

    public QueuePublisher(Queue<Message> queue) {
        this.queue = queue;
    }

    @Override
    public void accept(Message message) {
        queue.offer(message);
    }
}
