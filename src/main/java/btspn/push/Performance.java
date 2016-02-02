package btspn.push;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.zeromq.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Performance {
    public static class Reader implements Runnable {

        private final String address;
        private final AtomicLong counter;
        private final CountDownLatch waitLatch;
        private final CountDownLatch latch;

        public Reader(String address, AtomicLong counter, CountDownLatch waitLatch, CountDownLatch latch) {
            this.address = address;
            this.counter = counter;
            this.waitLatch = waitLatch;
            this.latch = latch;
        }

        @Override
        public void run() {
            ZContext ctx = new ZContext();
            ZMQ.Socket pipe = ctx.createSocket(ZMQ.PAIR);
            pipe.connect(address);

            new Messages.Sub("1", "/overview").send(pipe);
            AtomicBoolean kx = new AtomicBoolean();
            ZLoop loop = new ZLoop();
            loop.addPoller(new ZMQ.PollItem(pipe, ZPoller.IN), (loop1, item, arg) -> {
                if (item.isReadable()) {
                    ZMsg msg = ZMsg.recvMsg(item.getSocket());

                    if (msg.size() == 5) {
                        Messages.KvSyncT kvsync = Messages.KvSyncT.parse(msg);
                        if ("1".equals(kvsync.client)) {
                            if (kx.get()) {
                                counter.incrementAndGet();
                            } else if ("KTHXBAI".equals(kvsync.key)) {
                                kx.set(true);
                            }
                        }
                    }
                }
                return 0;
            }, null);
            try {
                waitLatch.countDown();
                latch.await();
            } catch (InterruptedException e) {
                return;
            }
            loop.start();
            loop.destroy();
            ctx.close();
        }
    }

    public static class Writer implements Runnable {

        private final String address;
        private final AtomicLong counter;
        private final int sleep;
        private final CountDownLatch waitLatch;
        private final CountDownLatch latch;

        public Writer(String address, AtomicLong counter, int sleep, CountDownLatch waitLatch, CountDownLatch latch) {
            this.address = address;
            this.counter = counter;
            this.sleep = sleep;
            this.waitLatch = waitLatch;
            this.latch = latch;
        }

        @Override
        public void run() {
            ZContext ctx = new ZContext();

            ZMQ.Socket update = ctx.createSocket(ZMQ.DEALER);
            update.connect(address);
            update.setHWM(50000);


            try {
                waitLatch.countDown();
                latch.await();
            } catch (InterruptedException e) {
                return;
            }
            int i = 0;
            Random random = new Random();
            long start = System.currentTimeMillis();
            while (true) {
                new Messages.KvSync("/overview/" + random.nextInt(1000), i++, "", RandomStringUtils.random(10)).send(update);
//                update.recvStr();
                counter.incrementAndGet();
                if (sleep > 0) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                if (false) {
                    break;
                }
                if (System.currentTimeMillis() - start > 10000) {
                    break;
                }
            }

            update.close();
            ctx.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch waitLatch = new CountDownLatch(2);
        AtomicLong out = new AtomicLong(0);
        Thread reader = new Thread(new Reader("tcp://127.0.0.1:5004", out, waitLatch, startLatch));
        AtomicLong in = new AtomicLong(0);
        Thread writer = new Thread(new Writer("tcp://127.0.0.1:5003", in, 0, waitLatch, startLatch));
        reader.start();
        writer.start();
        waitLatch.await();
        startLatch.countDown();

        while (true) {
            long os = out.get();
            long is = in.get();

            Thread.sleep(1000);

            long oe = out.get();
            long ie = in.get();

            long written = ie - is;
            long read = oe - os;
            long delta = in.get() - out.get();
            System.out.println(written + "\t" + read + "\t" + delta + "\t" + is + "\t" + os);
        }
    }
}
