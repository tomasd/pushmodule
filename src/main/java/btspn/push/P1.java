package btspn.push;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tomas on 22.01.16.
 */
public class P1 {
    public static class Test implements Runnable{

        private final AtomicLong counter;

        public Test(AtomicLong counter) {

            this.counter = counter;
        }

        @Override
        public void run() {

            ConcurrentRadixTree<Pair<Long, String>> state = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());

            Random random = new Random();
            int i = 0;
            while (true) {
                Messages.KvSync kvSync = new Messages.KvSync("/overview/" + random.nextInt(1000), i++, "", RandomStringUtils.random(10));

                if (kvSync.value.isEmpty()) {
                    state.remove(kvSync.key);
                } else {
                    Pair<Long, String> pair = state.getValueForExactKey(kvSync.key);
                    if (pair == null || pair.getKey() < kvSync.version) {
                        state.put(kvSync.key, ImmutablePair.of(kvSync.version, kvSync.value));
                    }
                }
                counter.incrementAndGet();
            }
        }
    }
    public static void main(String[] args) throws InterruptedException {
        AtomicLong in = new AtomicLong();
        new Thread(new Test(in)).start();

        while (true) {
            long is = in.get();

            Thread.sleep(1000);

            long ie = in.get();

            long written = ie - is;
            System.out.println(written + "\t" + is + "\t");
        }

    }
}
