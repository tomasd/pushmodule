package com.drencak.push;

import com.drencak.push.client.StateClient;
import com.drencak.push.server.*;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import javaslang.collection.List;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by tomas on 02.02.16.
 */
public class Test {
    public static void main(String[] args) throws InterruptedException {
        String publishAddress = "tcp://localhost:5000";
        String snapshotAddress = "tcp://localhost:5001";
        ConcurrentRadixTree<Message> state = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());

        Thread client = new Thread(new StateClient(publishAddress, snapshotAddress, "/", state));
        client.start();

        RadixTreeState publisher = RadixTreeState.empty();
        Thread server = new Thread(new StateServer(publishAddress, snapshotAddress, publisher, publisher.getQueue()));
        server.start();


        publisher.accept(new Message("/overview/1/", "", "futbal"));
        Thread.sleep(2000);
        publisher.accept(new Message("/overview/2/", "", "hokej"));


    }
}
