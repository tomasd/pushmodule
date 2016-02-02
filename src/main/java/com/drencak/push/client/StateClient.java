package com.drencak.push.client;

import btspn.push.Messages;
import com.drencak.push.server.Message;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sun.tools.doclint.Entity.times;

public class StateClient implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StateClient.class);
    private final String publishAddress;
    private final String snapshotAddress;
    private final String path;
    private final ConcurrentRadixTree<Message> state;

    public StateClient(String publishAddress, String snapshotAddress, String path, ConcurrentRadixTree<Message> state) {
        this.publishAddress = publishAddress;
        this.snapshotAddress = snapshotAddress;
        this.path = path;
        this.state = state;
    }

    @Override
    public void run() {
        ZContext ctx = new ZContext();

        ZMQ.Socket publish = ctx.createSocket(ZMQ.SUB);
        publish.subscribe("".getBytes());
        publish.connect(publishAddress);

        ZMQ.Socket snapshot = ctx.createSocket(ZMQ.DEALER);
        snapshot.connect(snapshotAddress);


        AtomicLong lastHugz = new AtomicLong(System.currentTimeMillis());
        AtomicLong time = new AtomicLong(0);
        AtomicBoolean synced = new AtomicBoolean(false);
        sendIcanhaz(synced, snapshot);

        ZLoop loop = new ZLoop();
        ZMQ.PollItem snapshotItem = new ZMQ.PollItem(snapshot, ZPoller.IN);
        loop.addPoller(snapshotItem, (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());

                msg.unwrap();
                Messages.KvSyncT kvSyncT = Messages.KvSyncT.parse(msg);

                if (!kvSyncT.isKthxbai()) {
                    put(kvSyncT.key, new Message(kvSyncT.key, kvSyncT.props, kvSyncT.value));
                    time.set(kvSyncT.version);
                } else {
                    synced.set(true);
//                    loop.removePoller(snapshotItem);
                }
            }
            return 0;
        }, null);
        loop.addPoller(new ZMQ.PollItem(publish, ZPoller.IN), (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                String cmd = msg.peekFirst().toString();

                if ("HUGZ".equals(cmd)) {

                } else if (synced.get()) {
                    Messages.KvSync kvSync = Messages.KvSync.parse(msg);
                    if (kvSync.version - 1 == time.get()) {
                        put(kvSync.key, new Message(kvSync.key, kvSync.props, kvSync.value));
                        time.set(kvSync.version);
                    } else if (kvSync.version > time.get()) {
                        // lost message
                        sendIcanhaz(synced, snapshot);
                    }
                }
                lastHugz.set(System.currentTimeMillis());
            }
            return 0;
        }, null);

        loop.addTimer(50, 0, (loop1, item, arg) -> {
            long now = System.currentTimeMillis();
            if (now - lastHugz.get() > 1000) {
                sendIcanhaz(synced, snapshot);
                lastHugz.set(System.currentTimeMillis());
            }
            return 0;
        }, null);

        loop.start();
        loop.destroy();

        ctx.close();
    }

    private void sendIcanhaz(AtomicBoolean synced, ZMQ.Socket snapshot) {
        synced.set(false);

        LOG.debug("RECONNECT");
        ZMsg m = new Messages.Icanhaz("1", path).toMsg();
        m.push("".getBytes());
        m.send(snapshot);
    }

    private void put(String key, Message value) {
        LOG.debug("KVSYNC: {} - {}", key, value.value);
        state.put(key, value);
    }
}
