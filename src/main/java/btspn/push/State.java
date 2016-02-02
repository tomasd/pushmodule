package btspn.push;

import com.googlecode.concurrenttrees.common.KeyValuePair;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.util.concurrent.atomic.AtomicLong;

public class State implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(State.class);
    private final String publish;
    private final String snapshot;
    private final String update;
    private final RadixTree<Pair<Long, String>> state;

    public State(String publish, String snapshot, String update, RadixTree<Pair<Long, String>> state) {
        this.publish = publish;
        this.snapshot = snapshot;
        this.update = update;
        this.state = state;
    }
    public static void main(String[] args) throws InterruptedException {
        ConcurrentRadixTree<Pair<Long, String>> state = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());
        new State("tcp://127.0.0.1:5001", "tcp://127.0.0.1:5002", "tcp://127.0.0.1:5003", state).run();

    }

    public static void main1(String[] args) throws InterruptedException {
        ZContext ctx = new ZContext();

        ConcurrentRadixTree<Pair<Long, String>> state = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());
        Thread thread = new Thread(new State("tcp://127.0.0.1:5001", "tcp://127.0.0.1:5002", "tcp://127.0.0.1:5003", state));
        thread.setDaemon(false);
        thread.start();

        ZMQ.Socket update = ctx.createSocket(ZMQ.REQ);
        update.connect("tcp://127.0.0.1:5003");
        ZMQ.Socket snapshot = ctx.createSocket(ZMQ.DEALER);
        snapshot.connect("tcp://127.0.0.1:5002");


        int i = 0;
        while (true) {
            new Messages.KvSync("/overview/1", i++, "", "futbal").send(update);
            System.out.println(update.recvStr());
            Thread.sleep(5000);
            if (false) {
                break;
            }
        }

        new Messages.Icanhaz("1", "/overview").send(snapshot);
        while (true) {
            ZMsg msg1 = ZMsg.recvMsg(snapshot);
            msg1.unwrap();
            Messages.KvSyncT msg = Messages.KvSyncT.parse(msg1);
            LOG.info("KVSYNCT {}", msg);
            if ("KTHXBAI".equals(msg.key)) {
                break;
            }
        }

        LOG.info("Closing");
        update.close();
        snapshot.close();
        ctx.close();
    }

    @Override
    public void run() {
        ZContext ctx = new ZContext();

        ZMQ.Socket publish = ctx.createSocket(ZMQ.PUB);
        publish.bind(this.publish);

        ZMQ.Socket snapshot = ctx.createSocket(ZMQ.ROUTER);
        snapshot.bind(this.snapshot);

        ZMQ.Socket update = ctx.createSocket(ZMQ.ROUTER);
        update.bind(this.update);
        update.setRcvHWM(50000);

        ZLoop loop = new ZLoop();

        AtomicLong lastHugz = new AtomicLong(System.currentTimeMillis());
        ZMQ.PollItem x = new ZMQ.PollItem(publish, ZPoller.OUT);
        loop.addPoller(x, (loop1, item, arg) -> {
            if (item.isWritable()) {
                item.getSocket().send("RESET");
                loop.removePoller(x);
            }
            return 0;
        }, null);
        loop.addPoller(new ZMQ.PollItem(update, ZPoller.IN), (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(update);
                ZFrame identity = msg.unwrap();
                if (handlePut(publish, state, msg)) {
                    lastHugz.set(System.currentTimeMillis());
                }
                ZMsg ack = new ZMsg();
                ack.add("ACK");
                ack.wrap(identity);
                ack.send(update, true);
            }
            return 0;
        }, null);
        loop.addTimer(50, 0, (loop1, item, arg) -> {
            if (System.currentTimeMillis() - lastHugz.get() > 200) {
                publish.send("HUGZ");
                lastHugz.set(System.currentTimeMillis());
            }
            return 0;
        }, null);
        loop.addPoller(new ZMQ.PollItem(snapshot, ZPoller.IN), (loop1, item, arg) -> {
            if (item.isReadable()) {
                ZMsg msg = ZMsg.recvMsg(item.getSocket());
                ZFrame identity = msg.unwrap();
                if (msg.size() == 3 && "ICANHAZ?".equals(msg.peek().toString())) {
                    msg.popString();

                    Messages.Icanhaz icanhaz = Messages.Icanhaz.parse(msg);

                    for (KeyValuePair<Pair<Long, String>> entry : state.getKeyValuePairsForKeysStartingWith(icanhaz.path)) {
                        ZMsg kvsync = new Messages.KvSyncT(
                                icanhaz.client,
                                entry.getKey().toString(),
                                entry.getValue().getKey(),
                                null,
                                entry.getValue().getValue()
                        ).toMsg();
                        kvsync.wrap(identity.duplicate());
                        kvsync.send(item.getSocket(), true);
                    }

                    ZMsg kthxbai = Messages.KvSyncT.KTHXBAI(icanhaz.client, icanhaz.path).toMsg();
                    kthxbai.wrap(identity);
                    kthxbai.send(item.getSocket(), true);
                }
            }
            return 0;
        }, null);
        loop.start();
        loop.destroy();

        update.close();
        publish.close();
        snapshot.close();
        ctx.close();
    }

    private static boolean handlePut(ZMQ.Socket publish, RadixTree<Pair<Long, String>> state, ZMsg kvsync) {
        if (kvsync.size() == 4) {
            LOG.trace("KVSYNC {}", kvsync);
            Messages.KvSync kvSync = Messages.KvSync.parse(kvsync);
            if (put(state, kvSync)) {
                kvSync.send(publish);
                return true;
            }
        } else {
            LOG.debug("Invalid PUT msg {}", kvsync);
            kvsync.destroy();
        }
        return false;
    }

    private static boolean put(RadixTree<Pair<Long, String>> state, Messages.KvSync kvSync) {
        if (kvSync.value.isEmpty()) {
            return state.remove(kvSync.key);
        } else {
            Pair<Long, String> pair = state.getValueForExactKey(kvSync.key);
            if (pair == null || pair != null && pair.getKey() < kvSync.version) {
                state.put(kvSync.key, ImmutablePair.of(kvSync.version, kvSync.value));
                return true;
            }
        }
        return false;
    }
}
