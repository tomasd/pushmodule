package btspn.push.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Writer implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(Writer.class);
    private final String pubAddress;
    private final AtomicInteger inCounter;

    public Writer(String pubAddress, AtomicInteger inCounter) {
        this.pubAddress = pubAddress;
        this.inCounter = inCounter;
    }

    public static void main(String[] args) throws InterruptedException {
        new Writer("tcp://localhost:5002", new AtomicInteger(0)).run();
    }

    @Override
    public void run() {

        ZContext ctx = new ZContext();
        ZMQ.Socket update = ctx.createSocket(ZMQ.PUB);
        LOG.info("Connecting writer to {}", pubAddress);
        update.connect(pubAddress);


        int i = 1;
        Random rnd = new Random();
        while (true) {
            ZMsg up = new ZMsg();
            up.add("/overview/" + rnd.nextInt(10) + "/sport");
            up.add(BigInteger.valueOf(i++).toByteArray());
            up.add(new byte[0]);
            up.add(new byte[0]);
            up.add("Futbal");
            up.send(update, true);
            inCounter.incrementAndGet();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}
