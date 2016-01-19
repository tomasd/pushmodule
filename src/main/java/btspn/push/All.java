package btspn.push;

import btspn.push.client.Client;
import btspn.push.server.Server;
import btspn.push.server.Writer;
import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.atomic.AtomicInteger;

public class All {
    public static void main(String[] args) throws InterruptedException {
        AtomicInteger outCounter = new AtomicInteger(0);
        Thread server = new Thread(new Server(5000));
        Thread client = new Thread(new Client("localhost", 5000, "tcp://localhost:5011", outCounter));
        AtomicInteger inCounter = new AtomicInteger(0);
        Thread writer = new Thread(new Writer("tcp://localhost:5002", inCounter));


        server.start();
        client.start();
        writer.start();

        StopWatch stopWatch = new StopWatch();
        while (true) {
            int startOut = outCounter.get();
            int startIn = inCounter.get();
            stopWatch.start();

            Thread.sleep(2000);
            int stopOut = outCounter.get();
            int stopIn = inCounter.get();
            stopWatch.stop();
            System.out.println((stopIn-startIn)/(stopWatch.getTime()/1000f) + " - " + (stopOut-startOut)/(stopWatch.getTime()/1000f));
            stopWatch = new StopWatch();

        }
    }
}
