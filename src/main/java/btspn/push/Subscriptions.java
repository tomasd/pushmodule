package btspn.push;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.Node;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.*;
import org.slf4j.Logger;
import org.zeromq.*;

import java.util.*;
import java.util.function.Supplier;

public class Subscriptions implements Runnable {


    private static final Logger LOG = LoggerFactory.getLogger(Subscriptions.class);
    private final String pubAddress;
    private final String snapshotAddress;
    private final String pipeAddress;
    private final Supplier<ConcurrentRadixTree<Set<String>>> subscriptionsFactory;
    private ConcurrentRadixTree<Set<String>> subscriptions;
    private final Map<String, Set<String>> pathSubscriptions;


    public Subscriptions(String pubAddress, String snapshotAddress, String pipeAddress, Supplier<ConcurrentRadixTree<Set<String>>> subscriptionsFactory) {
        this.pubAddress = pubAddress;
        this.snapshotAddress = snapshotAddress;
        this.pipeAddress = pipeAddress;
        this.subscriptionsFactory = subscriptionsFactory;
        this.subscriptions = subscriptionsFactory.get();
        this.pathSubscriptions = new HashMap<>();
    }

    @Override
    public void run() {
        ZContext ctx = new ZContext();
        ZMQ.Socket events = ctx.createSocket(ZMQ.SUB);
        events.connect(pubAddress);
        events.subscribe("".getBytes());
        ZMQ.Socket snapshot = ctx.createSocket(ZMQ.DEALER);
        snapshot.connect(snapshotAddress);
        ZMQ.Socket pipe = ctx.createSocket(ZMQ.PAIR);
        pipe.bind(pipeAddress);

        ZLoop loop = new ZLoop();
        loop.addPoller(new ZMQ.PollItem(events, ZPoller.IN), (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                if (msg.size() == 1) {
                    String cmd = msg.peekFirst().toString();

                    if ("RESET".equals(cmd)) {
                        subscriptions = subscriptionsFactory.get();
                        pathSubscriptions.clear();
                        msg.send(pipe, true);
                    } else if ("HUGZ".equals(cmd)) {

                    } else {
                        LOG.warn("Invalid msg {}", msg);
                        msg.destroy();
                    }
                } else if (msg.size() == 4) {
                    // KVSYNC
                    Messages.KvSync kvSync = Messages.KvSync.parse(msg.duplicate());

                    Set<String> publish = find(subscriptions, kvSync.key);
                    if (!publish.isEmpty()) {
                        String publishStr = StringUtils.join(publish, "|");

                        msg.push(publishStr);
                        msg.send(pipe, true);
                    } else {
                        msg.destroy();
                    }
                }
            }
            return 0;
        }, null);
        loop.addPoller(new ZMQ.PollItem(snapshot, ZPoller.IN), (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                msg.unwrap();
                if (msg.size() == 5) {
                    // KVSYNCT
                    msg.send(pipe, true);
                } else {
                    LOG.warn("Invalid msg {}", msg);
                    msg.destroy();
                }
            }
            return 0;
        }, null);
        loop.addPoller(new ZMQ.PollItem(pipe, ZPoller.IN), (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                String cmd = msg.peekFirst().toString();
                if ("RESET".equals(cmd)) {
                    subscriptions = subscriptionsFactory.get();
                    pathSubscriptions.clear();
                } else if ("SUB".equals(cmd)) {
                    Messages.Sub sub = Messages.Sub.parse(msg);
                    Set<String> set = subscriptions.getValueForExactKey(sub.path);
                    pathSubscriptions.compute(sub.client, (s, strings) -> {
                        if (strings == null) {
                            strings = new HashSet<>();
                        }
                        strings.add(sub.path);
                        return strings;
                    });
                    if (set == null) {
                        set = new HashSet<>();
                    } else {
                        set = new HashSet<>(set);
                    }
                    set.add(sub.client);
                    subscriptions.put(sub.path, Collections.unmodifiableSet(set));

                    new Messages.Icanhaz(sub.client, sub.path).send(snapshot);
                } else if ("UNSUB".equals(cmd)) {
                    msg.popString();
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
        }, null);
        loop.start();
        loop.destroy();

        events.close();
        snapshot.close();
        ctx.close();
    }

    public static Set<String> find(ConcurrentRadixTree<Set<String>> subscriptions, String path) {
        String originalPath = path;
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
        return publish;
    }

    private void unsubscribe(String identity, String path) {
        Set<String> set = subscriptions.getValueForExactKey(path);

        if (set != null) {
            set = new HashSet<>(set);
            set.remove(identity);
            if (set.isEmpty()) {
                subscriptions.remove(path);
            } else {
                subscriptions.put(path, Collections.unmodifiableSet(set));
            }
        }
    }

    public static void main(String[] args) {
        Thread t = new Thread(new Subscriptions("tcp://127.0.0.1:5001", "tcp://127.0.0.1:5002", "tcp://127.0.0.1:5004", () -> new ConcurrentRadixTree<Set<String>>(new DefaultCharSequenceNodeFactory())));
        t.setDaemon(false);
        t.start();

        ZContext ctx = new ZContext();
        ZMQ.Socket pipe = ctx.createSocket(ZMQ.PAIR);
        pipe.connect("tcp://127.0.0.1:5004");

        new Messages.Sub("1", "/overview").send(pipe);

        ZLoop loop = new ZLoop();
        loop.addPoller(new ZMQ.PollItem(pipe, ZPoller.IN), (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                msg.dump();
            }
            return 0;
        }, null);
        loop.start();
        loop.destroy();
        ctx.close();
    }
}
