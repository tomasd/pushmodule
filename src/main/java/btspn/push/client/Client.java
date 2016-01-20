package btspn.push.client;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.Node;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Client implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(Client.class);

    private final String pipeAddress;
    private final AtomicInteger counter;
    private final String snapshotAddress;
    private final String publisherAddress;

    public Client(String pipeAddress, AtomicInteger counter, String snapshotAddress, String publisherAddress) {
        this.pipeAddress = pipeAddress;
        this.counter = counter;
        this.snapshotAddress = snapshotAddress;
        this.publisherAddress = publisherAddress;
    }

    public Client(String address, int port, String pipeAddress, AtomicInteger counter) {
        this(pipeAddress, counter, "tcp://" + address + ":" + port, "tcp://" + address + ":" + (port + 1));
    }

    public void run() {
        final ZContext ctx = new ZContext();
        ZMQ.Socket snapshot = ctx.createSocket(ZMQ.DEALER);
        ZMQ.Socket subscriber = ctx.createSocket(ZMQ.SUB);
        ZMQ.Socket pipe = ctx.createSocket(ZMQ.PAIR);


        connect("snapshot", snapshot, snapshotAddress);
        connect("subscriber", subscriber, publisherAddress);
        subscriber.subscribe("HUGZ".getBytes());
        bind("pipe", pipe, pipeAddress);
        ConcurrentRadixTree<Set<String>> subscriptions = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());
        Map<String, Set<String>> pathSubscriptions = new HashMap<>();

        ZLoop loop = new ZLoop();
        loop.addPoller(new ZMQ.PollItem(pipe, ZMQ.Poller.POLLIN), new PipePoller(subscriptions, snapshot, pathSubscriptions, subscriber), null);
        loop.addPoller(new ZMQ.PollItem(subscriber, ZMQ.Poller.POLLIN), new SubscriberPoller(subscriptions, pipe, counter), null);
        loop.addPoller(new ZMQ.PollItem(snapshot, ZMQ.Poller.POLLIN), new SnapshotPoller(pipe), null);
        loop.start();
        loop.destroy();
        snapshot.close();
        subscriber.close();
        ctx.close();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ctx.close();
            }
        });
    }

    private void bind(String type, ZMQ.Socket pipe, String pipeAddress) {
        LOG.info("Binding {} to {}", type, pipeAddress);
        pipe.bind(pipeAddress);
    }

    private void connect(String type, ZMQ.Socket socket, String addr) {
        LOG.info("Connecting {} to {}", type, addr);
        socket.connect(addr);
    }

    private class SubscriberPoller implements ZLoop.IZLoopHandler {
        private final ConcurrentRadixTree<Set<String>> subscriptions;
        private final ZMQ.Socket pipe;
        private final AtomicInteger counter;

        public SubscriberPoller(ConcurrentRadixTree<Set<String>> subscriptions, ZMQ.Socket pipe, AtomicInteger counter) {
            this.subscriptions = subscriptions;
            this.pipe = pipe;
            this.counter = counter;
        }

        public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                String path = msg.peekFirst().toString();
                final String originalPath = path;
                if ("HUGZ".equals(path)) {
                    LOG.debug("HUGZ");
                } else {
                    LOG.debug("KVSYNC: {}", msg);
                    Node node = subscriptions.getNode();
                    Set<String> publish = new HashSet<>();
                    do {
                        if (node.getValue() != null && path.startsWith(node.getIncomingEdge().toString())) {
                            publish.addAll((Set<String>) node.getValue());
                        }
                        if (!node.getOutgoingEdges().isEmpty()) {
                            path = path.substring(node.getIncomingEdge().length());
                            node = node.getOutgoingEdge(path.charAt(0));
                        } else {
                            subscriptions.getValuesForKeysStartingWith(originalPath).forEach(publish::addAll);
                            node = null;
                        }
                    } while (node != null);
                    String publishStr = StringUtils.join(publish, "|");
                    msg.push(publishStr);
                    msg.send(pipe, true);
                    counter.incrementAndGet();
                }
            }

            return 0;
        }
    }

    private class PipePoller implements ZLoop.IZLoopHandler {
        private final ConcurrentRadixTree<Set<String>> subscriptions;
        private final ZMQ.Socket snapshot;
        private final Map<String, Set<String>> pathSubscriptions;
        private final ZMQ.Socket subscriber;

        public PipePoller(ConcurrentRadixTree<Set<String>> subscriptions, ZMQ.Socket snapshot, Map<String, Set<String>> pathSubscriptions, ZMQ.Socket subscriber) {
            this.subscriptions = subscriptions;
            this.snapshot = snapshot;
            this.pathSubscriptions = pathSubscriptions;
            this.subscriber = subscriber;
        }

        @Override
        public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                LOG.debug("PIPE: {}", msg);
                String cmd = msg.popString();
                if ("SUBSCRIBE".equals(cmd)) {
                    String identity = msg.popString();
                    String path = msg.popString();
                    Set<String> set = subscriptions.getValueForExactKey(path);
                    pathSubscriptions.compute(identity, (s, strings) -> {
                        if (strings == null) {
                            strings = new HashSet<String>();
                        }
                        strings.add(path);
                        return strings;
                    });
                    if (set == null) {
                        set = new HashSet<>();
                        subscriber.subscribe(path.getBytes());
                    } else {
                        set = new HashSet<>(set);
                    }
                    set.add(identity);
                    subscriptions.put(path, Collections.unmodifiableSet(set));

                    ZMsg subMsg = new ZMsg();
                    subMsg.add(identity);
                    subMsg.add("ICANHAZ?");
                    subMsg.add(path);
                    subMsg.send(snapshot, true);
                } else if ("UNSUBSCRIBE".equals(cmd)) {
                    String identity = msg.popString();
                    if (!msg.isEmpty()) {
                        unsubscribe(identity, msg.popString());
                    } else {
                        Set<String> paths = pathSubscriptions.get(identity);
                        if (paths != null) {
                            for (String path : paths) {
                                unsubscribe(identity, path);
                            }
                        }
                    }
                }
            }
            return 0;
        }

        private void unsubscribe(String identity, String path) {
            Set<String> set = subscriptions.getValueForExactKey(path);

            if (set != null) {
                set = new HashSet<>(set);
                set.remove(identity);
                if (set.isEmpty()) {
                    subscriptions.remove(path);
                    subscriber.unsubscribe(path.getBytes());
                } else {
                    subscriptions.put(path, Collections.unmodifiableSet(set));
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String pipeAddress = "tcp://localhost:5011";

        new Client("localhost", 5000, pipeAddress, new AtomicInteger(0)).run();
    }

    private static class SnapshotPoller implements ZLoop.IZLoopHandler {
        private final ZMQ.Socket pipe;

        public SnapshotPoller(ZMQ.Socket pipe) {
            this.pipe = pipe;
        }


        @Override
        public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                LOG.debug("SNAPSHOT: {}", msg);
                msg.send(pipe, true);
            }
            return 0;
        }
    }
}
