package btspn.push;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Subscriptions implements Runnable{

    @Override
    public void run() {
        ZContext ctx = new ZContext();
        ZMQ.Socket events = ctx.createSocket(ZMQ.SUB);
        ZMQ.Socket snapshot = ctx.createSocket(ZMQ.DEALER);

        ctx.close();
    }
}
