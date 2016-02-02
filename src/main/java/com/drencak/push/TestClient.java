package com.drencak.push;

import com.drencak.push.client.StateClient;
import com.drencak.push.server.Message;
import com.drencak.push.server.RadixTreeState;
import com.drencak.push.server.StateServer;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by tomas on 02.02.16.
 */
public class TestClient {
    public static void main(String[] args) throws InterruptedException {
        String publishAddress = "tcp://localhost:5000";
        String snapshotAddress = "tcp://localhost:5001";
        ConcurrentRadixTree<Message> state = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());

        Thread client = new Thread(new StateClient(publishAddress, snapshotAddress, "/", state));
        client.start();
    }
}
