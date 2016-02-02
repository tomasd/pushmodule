package btspn.push;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.math.BigInteger;
import java.util.function.Supplier;

public class Messages {
    public static class Icanhaz {
        public final String client;
        public final String path;

        public Icanhaz(String client, String path) {
            this.client = client;
            this.path = path;
        }

        public static Icanhaz parse(ZMsg msg) {
            return new Icanhaz(msg.popString(), msg.popString());
        }

        public void send(ZMQ.Socket socket) {
            ZMsg msg = toMsg();
            msg.send(socket, true);
        }

        public ZMsg toMsg() {
            ZMsg msg = new ZMsg();
            msg.add("ICANHAZ?");
            msg.add(client);
            msg.add(path);
            return msg;
        }
    }



    public static class KvSyncT {
        public final String client;
        public final String key;
        public final long version;
        public final String props;
        public final String value;

        public KvSyncT(String client, String key, long version, String props, String value) {
            this.client = client;
            this.key = key;
            this.version = version;
            this.props = props;
            this.value = value;
        }

        public boolean isKthxbai() {
            return "KTHXBAI".equals(key);
        }
        public static KvSyncT KTHXBAI(String client, String path) {
            return new KvSyncT(client, "KTHXBAI", 0, "", path);
        }
        public static KvSyncT parse(ZMsg msg) {
            KvSyncT value = new KvSyncT(
                    msg.popString(),
                    msg.popString(),
                    new BigInteger(msg.pop().getData()).longValue(),
                    msg.popString(),
                    msg.popString()
            );
            msg.destroy();
            return value;
        }

        public void send(ZMQ.Socket publish) {
            toMsg().send(publish, true);
        }

        public ZMsg toMsg() {
            ZMsg zFrames = new ZMsg();
            zFrames.add(client);
            zFrames.add(key);
            zFrames.add(BigInteger.valueOf(version).toByteArray());
            zFrames.add(props);
            zFrames.add(value);
            return zFrames;
        }

        @Override
        public String toString() {
            return toMsg().toString();
        }
    }

    public static class KvSync {
        public final String key;
        public final long version;
        public final String props;
        public final String value;

        public KvSync(String key, long version, String props, String value) {
            this.key = key;
            this.version = version;
            this.props = props;
            this.value = value;
        }

        public static KvSync parse(ZMsg msg) {
            KvSync value = new KvSync(
                    msg.popString(),
                    new BigInteger(msg.pop().getData()).longValue(),
                    msg.popString(),
                    msg.popString()
            );
            msg.destroy();
            return value;
        }

        public void send(ZMQ.Socket publish) {
            toMsg().send(publish, true);
        }

        public ZMsg toMsg() {
            ZMsg zFrames = new ZMsg();
            zFrames.add(key);
            zFrames.add(BigInteger.valueOf(version).toByteArray());
            zFrames.add(props);
            zFrames.add(value);
            return zFrames;
        }
    }

    public static class Sub {
        public final String client;
        public final String path;

        public Sub(String client, String path) {
            this.client = client;
            this.path = path;
        }

        public static Sub parse(ZMsg msg) {
            String cmd = msg.popString();
            assert "SUB".equals(cmd);
            return new Sub(msg.popString(), msg.popString());
        }

        public void send(ZMQ.Socket socket) {
            toMsg().send(socket, true);
        }

        private ZMsg toMsg() {
            ZMsg msg = new ZMsg();
            msg.add("SUB");
            msg.add(client);
            msg.add(path);
            return msg;
        }
    }

    public static ZMsg readKvSync(ZMQ.Socket socket) {
        return ZMsg.recvMsg(socket);
    }
}
