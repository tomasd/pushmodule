package btspn.push;

import btspn.push.client.Client;
import btspn.push.server.Server;
import btspn.push.server.ServerActor;
import btspn.push.server.Writer;
import com.googlecode.concurrenttrees.common.PrettyPrinter;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicInteger;

public class All1 {
    public static void main(String[] args) throws InterruptedException {
        AtomicInteger outCounter = new AtomicInteger(0);


        String snapshotAddress = "tcp://localhost:5000";
        String publisherAddress = "tcp://localhost:5001";
        String collectorAddress = "tcp://localhost:5002";
        ConcurrentRadixTree<Pair<Long, String>> state = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());
        ServerActor server = new ServerActor(snapshotAddress, publisherAddress, collectorAddress, state);
        Thread client = new Thread(new Client("tcp://localhost:5011", outCounter, snapshotAddress, publisherAddress));
        AtomicInteger inCounter = new AtomicInteger(0);


        client.start();

        server.put("/overview/1", 1, "asdf");
        PrettyPrinter.prettyPrint(state, System.out);
        server.close();
    }
}
