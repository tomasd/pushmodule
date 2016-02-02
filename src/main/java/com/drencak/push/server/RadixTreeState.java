package com.drencak.push.server;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultByteArrayNodeFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class RadixTreeState implements StateProvider, StatePublisher{
    private final RadixTree<Message> state;
    private final Queue<Message> pendingChanges;

    public RadixTreeState(RadixTree<Message> state, Queue<Message> pendingChanges) {
        this.state = state;
        this.pendingChanges = pendingChanges;
    }

    public static RadixTreeState empty() {
        return new RadixTreeState(new ConcurrentRadixTree<Message>(new DefaultByteArrayNodeFactory()), new ConcurrentLinkedDeque<>());
    }

    @Override
    public Iterable<Message> apply(String s) {
        return state.getValuesForKeysStartingWith(s);
    }

    @Override
    public void accept(Message message) {
        state.put(message.key, message);
        pendingChanges.offer(message);
    }

    public Queue getQueue() {
        return pendingChanges;
    }
}
