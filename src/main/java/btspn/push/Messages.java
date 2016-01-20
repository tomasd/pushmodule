package btspn.push;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.math.BigInteger;

public class Messages {
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

    public static ZMsg readKvSync(ZMQ.Socket socket) {
        return ZMsg.recvMsg(socket);
    }
}
