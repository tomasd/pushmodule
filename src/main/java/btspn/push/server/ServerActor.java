package btspn.push.server;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import org.apache.commons.lang3.tuple.Pair;
import org.zeromq.*;

import java.math.BigInteger;
import java.nio.channels.SelectableChannel;
import java.util.Arrays;
import java.util.List;


public class ServerActor {
    private final ZActor actor;

    private static class Actor extends ZActor.SimpleActor {
        private final ConcurrentRadixTree<Pair<Long, String>> state;

        public Actor(ConcurrentRadixTree<Pair<Long, String>> state) {
            this.state = state;
        }

        @Override
        public List<ZMQ.Socket> createSockets(ZContext ctx, Object... args) {
            final ZMQ.Socket snapshot = ctx.createSocket(ZMQ.ROUTER);
            final ZMQ.Socket publisher = ctx.createSocket(ZMQ.PUB);
            final ZMQ.Socket collector = ctx.createSocket(ZMQ.SUB);

            snapshot.bind((String) args[0]);
            publisher.bind((String) args[1]);
            collector.bind((String) args[2]);
            return Arrays.asList(snapshot, publisher, collector);
        }

        @Override
        public void start(ZMQ.Socket pipe, List<ZMQ.Socket> sockets, ZPoller poller) {
            ZMQ.Socket snapshot = sockets.get(0);
            ZMQ.Socket publisher = sockets.get(1);
            ZMQ.Socket collector = sockets.get(2);

            poller.register(snapshot, new ZPoller.EventsHandler() {
                @Override
                public boolean events(ZMQ.Socket socket, int events) {
                    Server.handleSnapshot(socket, state);
                    return true;
                }

                @Override
                public boolean events(SelectableChannel channel, int events) {
                    return true;
                }
            }, ZPoller.POLLIN);
            poller.register(collector, new ZPoller.EventsHandler() {
                @Override
                public boolean events(ZMQ.Socket socket, int events) {
                    Server.handleCollector(socket, state, publisher);
                    return true;
                }

                @Override
                public boolean events(SelectableChannel channel, int events) {
                    return true;
                }
            }, ZPoller.POLLIN);

            poller.register(pipe, new ZPoller.EventsHandler() {
                @Override
                public boolean events(ZMQ.Socket socket, int events) {
                    String cmd = socket.recvStr();
                    if ("KVPUB".equals(cmd)) {
                        Server.handleCollector(socket, state, publisher);
                    }
                    return true;
                }

                @Override
                public boolean events(SelectableChannel channel, int events) {
                    return true;
                }
            }, ZPoller.IN);
        }

        @Override
        public boolean backstage(ZMQ.Socket pipe, ZPoller poller, int events) {

            return super.backstage(pipe, poller, events);
        }
    }

    public ServerActor(String snapshotAddress, String publisherAddress, String collectorAddress, ConcurrentRadixTree<Pair<Long, String>> state) {
        this.actor = new ZActor(new Actor(state), "CLOSE", snapshotAddress, publisherAddress, collectorAddress);
    }

    public void put(String path, long version, String value) {
        ZMQ.Socket pipe = this.actor.pipe();
        ZMsg msg = new ZMsg();
        msg.add("KVPUB");
        msg.add(path);
        msg.add(BigInteger.valueOf(version).toByteArray());
        msg.add(new byte[0]);
        msg.add(new byte[0]);
        msg.add(value);
        msg.send(pipe, true);
    }

    public static void main(String[] args) {
        ZActor.Actor acting = new ZActor.SimpleActor() {
            public List<ZMQ.Socket> createSockets(ZContext ctx, Object[] args) {
                assert ("TEST".equals(args[0]));
                return Arrays.asList(ctx.createSocket(ZMQ.PUB));
            }

            public boolean backstage(ZMQ.Socket pipe, ZPoller poller, int events) {
                String cmd = pipe.recvStr();
                if ("HELLO".equals(cmd)) {
                    pipe.send("WORLD");
                    // end of the actor
                    return false;
                }
                return true;
            }
        };
        ZActor actor = new ZActor(acting, "LOCK", Arrays.asList("TEST").toArray());
        ZMQ.Socket pipe = actor.pipe();
        boolean rc = pipe.send("HELLO");
        assert (rc);
        ZMsg msg = actor.recv();
        String world = msg.popString();
        assert ("WORLD".equals(world));
        msg = actor.recv();
        assert (msg == null);
        rc = actor.sign();
        assert (!rc);
        rc = actor.send("whatever");
        assert (!rc);
        // don't try to use the pipe
    }
}
