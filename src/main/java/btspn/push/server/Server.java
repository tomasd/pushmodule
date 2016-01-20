package btspn.push.server;

import com.googlecode.concurrenttrees.common.KeyValuePair;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

public class Server implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(Server.class);
    private final String snapshotAddress;
    private final String publisherAddress;
    private final String collectorAddress;

    public Server(String snapshotAddress, String publisherAddress, String collectorAddress) {
        this.snapshotAddress = snapshotAddress;
        this.publisherAddress = publisherAddress;
        this.collectorAddress = collectorAddress;
    }

    public Server(int port) {
        this("tcp://*:" + port, "tcp://*:" + (port + 1), "tcp://*:" + (port + 2));
    }

    static ZMsg event(CharSequence path, Long version, byte[] cmdId, String properties, String value) {
        ZMsg resp = new ZMsg();
        event(resp, path, version, cmdId, properties, value);
        return resp;
    }

    static void event(ZMsg resp, CharSequence path, Long version, byte[] cmdId, String properties, String value) {
        resp.add(path.toString());
        resp.add(bytes(version));
        resp.add(cmdId);
        resp.add(properties);
        resp.add(value);
    }

    private static byte[] bytes(Long version) {
        return BigInteger.valueOf(version).toByteArray();
    }

    public static void handleSnapshot(ZMQ.Socket socket, ConcurrentRadixTree<Pair<Long, String>> state) {
        ZMsg msg = ZMsg.recvMsg(socket);
        LOG.debug("ICANHAZ: {}", msg);
        byte[] identity = msg.pop().getData();
        byte[] clientIdentity = msg.pop().getData();
        String cmd = msg.popString();
        if ("ICANHAZ?".equals(cmd)) {
            String path = msg.popString();

            Iterable<KeyValuePair<Pair<Long, String>>> entries = state.getKeyValuePairsForKeysStartingWith(path);
            long maxVersion = 0l;
            for (KeyValuePair<Pair<Long, String>> entry : entries) {
                Long version = entry.getValue().getKey();
                maxVersion = Math.max(maxVersion, version);

                ZMsg resp = new ZMsg();
                resp.add(identity);
                resp.add(clientIdentity);
                event(resp, entry.getKey(), version, new byte[0], null, entry.getValue().getValue());
                resp.send(socket, true);
            }

            ZMsg resp = new ZMsg();
            resp.add(identity);
            resp.add(clientIdentity);
            event(resp, "KTHXBAI", maxVersion, new byte[0], null, null);
            resp.send(socket, true);
        } else {
            LOG.error("Ignoring invalid command: {}", cmd);
        }
        msg.destroy();
    }

    public static void handleCollector(ZMQ.Socket socket, ConcurrentRadixTree<Pair<Long, String>> state, ZMQ.Socket publisher) {
        ZMsg msg = ZMsg.recvMsg(socket);
//                LOG.debug("COLLECTOR: {}", msg);
        String path = msg.popString();
        long version = new BigInteger(msg.pop().getData()).longValue();
        byte[] cmdId = msg.pop().getData();
        String properties = msg.popString();
        String value = msg.popString();

        if (value == null) {
            state.remove(path);
            ZMsg resp = event(path, version, new byte[0], null, null);
            resp.send(publisher);
        } else {
            Pair<Long, String> p = state.getValueForExactKey(path);
            if (p == null || p.getKey() < version) {
                LOG.info("put {} {} {}", path, version, value);
                state.put(path, ImmutablePair.of(version, value));
                ZMsg resp = event(path, version, cmdId, properties, value);
                resp.send(publisher);
            }
        }
    }

    public void run() {
        final ZContext ctx = new ZContext();
        final ZMQ.Socket snapshot = ctx.createSocket(ZMQ.ROUTER);
        final ZMQ.Socket publisher = ctx.createSocket(ZMQ.PUB);
        final ZMQ.Socket collector = ctx.createSocket(ZMQ.SUB);


        bind("snapshot", snapshot, snapshotAddress);
        bind("publisher", publisher, publisherAddress);
        bind("collector", collector, collectorAddress);
        collector.subscribe("".getBytes());

        ConcurrentRadixTree<Pair<Long, String>> state = new ConcurrentRadixTree<Pair<Long, String>>(new DefaultCharSequenceNodeFactory());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                snapshot.close();
                publisher.close();
                collector.close();
                ctx.close();
            }
        });

        ZLoop loop = new ZLoop();
        loop.addPoller(new ZMQ.PollItem(snapshot, ZMQ.Poller.POLLIN), new SnapshotPoller(state), null);
        loop.addPoller(new ZMQ.PollItem(collector, ZMQ.Poller.POLLIN), new CollectorPoller(state, publisher), null);
        loop.start();
        loop.destroy();
        publisher.close();
        snapshot.close();
        collector.close();
        ctx.close();
    }

    private void bind(String type, ZMQ.Socket snapshot, String addr) {
        LOG.info("Binding {} to {}", type, addr);
        snapshot.bind(addr);
    }

    public static void main(String[] args) throws InterruptedException {
        new Server(5000).run();
    }

    public static class SnapshotPoller implements ZLoop.IZLoopHandler {
        private final ConcurrentRadixTree<Pair<Long, String>> state;

        public SnapshotPoller(ConcurrentRadixTree<Pair<Long, String>> state) {
            this.state = state;
        }

        public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
            if (item.isReadable()) {
                ZMQ.Socket socket = item.getSocket();
                handleSnapshot(socket, state);
            }
            return 0;
        }

    }

    static class CollectorPoller implements ZLoop.IZLoopHandler {
        private final ConcurrentRadixTree<Pair<Long, String>> state;
        private final ZMQ.Socket publisher;

        public CollectorPoller(ConcurrentRadixTree<Pair<Long, String>> state, ZMQ.Socket publisher) {

            this.state = state;
            this.publisher = publisher;
        }

        public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
            if (item.isReadable()) {
                ZMQ.Socket socket = item.getSocket();
                handleCollector(socket, state, publisher);
            }
            return 0;
        }

        private UUID popUUID(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            UUID uuid = new UUID(buffer.getLong(), buffer.getLong());
            return uuid;
        }
    }
}
