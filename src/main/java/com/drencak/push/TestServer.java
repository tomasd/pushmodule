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
public class TestServer {
    public static void main(String[] args) throws InterruptedException {
        String publishAddress = "tcp://localhost:5000";
        String snapshotAddress = "tcp://localhost:5001";

        RadixTreeState publisher = RadixTreeState.empty();
        Thread server = new Thread(new StateServer(publishAddress, snapshotAddress, publisher, publisher.getQueue()));
        server.start();


        int i = 0;
        while (true) {
            publisher.accept(new Message("/overview/" + i++ + "/", "", "futbal"));
            Thread.sleep(500);
        }
    }
}
