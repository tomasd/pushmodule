package btspn.push;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZPoller;

import java.io.IOException;
import java.nio.channels.Selector;

/**
 * Created by tomas on 20.01.16.
 */
public class Logger {
    public static void main(String[] args) throws IOException {
        ZContext ctx = new ZContext();

        ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
        sub.connect("tcp://127.0.0.1:5001");
        sub.subscribe("".getBytes());


        ZPoller poller = new ZPoller(Selector.open());
        poller.register(sub, ZPoller.IN);

        System.out.println("Running...");
        try {
            while (true) {
                int poll = poller.poll(1000);
                if (poll == -1 ){
                    break;
                }
                if (poller.isReadable(sub)) {
                    ZMsg msg = ZMsg.recvMsg(sub);
                    msg.dump();
                }
            }
        } finally {
            sub.close();
            ctx.close();

        }
    }
}
