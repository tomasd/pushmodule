package com.drencak.push.server;

import btspn.push.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.io.Closeable;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StateServer implements Runnable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(StateServer.class);
    private final String publisherAddress;
    private final String snapshotAddress;
    private final StateProvider stateProvider;
    private final Queue<Message> queue;

    public StateServer(String publisherAddress, String snapshotAddress, StateProvider stateProvider, Queue queue) {
        this.publisherAddress = publisherAddress;
        this.snapshotAddress = snapshotAddress;
        this.stateProvider = stateProvider;
        this.queue = queue;
    }

    @Override
    public void run() {
        ZContext ctx = new ZContext();

        ZMQ.Socket snapshot = ctx.createSocket(ZMQ.ROUTER);
        ZMQ.Socket publisher = ctx.createSocket(ZMQ.PUB);

        snapshot.bind(this.snapshotAddress);
        publisher.bind(this.publisherAddress);
        AtomicLong time = new AtomicLong(0);
        AtomicLong lastHugz = new AtomicLong(System.currentTimeMillis());
        ZLoop loop = new ZLoop();
        loop.addPoller(new ZMQ.PollItem(snapshot, ZPoller.IN), new ZLoop.IZLoopHandler() {
            @Override
            public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
                if (item.isReadable()) {
                    ZMsg msg = ZMsg.recvMsg(item.getSocket());
                    ZFrame identity = msg.unwrap();
                    msg.popString();

                    Messages.Icanhaz icanhaz = Messages.Icanhaz.parse(msg);
                    LOG.debug("ICANHAZ: {}", icanhaz.path);
                    for (Message message : stateProvider.apply(icanhaz.path)) {
                        ZMsg kvsync = new Messages.KvSyncT(icanhaz.client, message.key, time.longValue(), message.props, message.value).toMsg();
                        kvsync.wrap(identity.duplicate());
                        kvsync.send(item.getSocket(), true);
                    }

                    ZMsg kthxbai = Messages.KvSyncT.KTHXBAI(icanhaz.client, icanhaz.path).toMsg();
                    kthxbai.wrap(identity);
                    kthxbai.send(item.getSocket(), true);
                }
                return 0;
            }
        }, null);

        Random rnd = new Random();

        loop.addPoller(new ZMQ.PollItem(publisher, ZPoller.OUT), new ZLoop.IZLoopHandler() {
            @Override
            public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
                if (item.isWritable()) {
                    Message message = queue.poll();
                    while (message != null) {
                        ZMsg msg = new Messages.KvSync(message.key, time.incrementAndGet(), message.props, message.value).toMsg();
                        msg.send(item.getSocket(), true);
                        message = queue.poll();
                        lastHugz.set(System.currentTimeMillis());

                        if (rnd.nextInt(10) == 1) {
                            time.incrementAndGet();
                        }
                    }
                }
                return 0;
            }
        }, null);

        loop.addTimer(15, 0, (loop1, item, arg) -> {
            if (System.currentTimeMillis() - lastHugz.get() > 50) {
                publisher.send("HUGZ");
                lastHugz.set(System.currentTimeMillis());
            }
            return 0;
        }, null);


        loop.start();
        loop.destroy();
        snapshot.close();
        publisher.close();
        ctx.close();
    }

    @Override
    public void close() {
    }
}
