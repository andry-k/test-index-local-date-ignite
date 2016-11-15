import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.cache.event.EventType.UPDATED;

public class TestCQ {

    @Test
    public void testWithPeerClassLoadingEnabled() throws IgniteCheckedException, InterruptedException {
        test(true, false);
    }

    @Test
    public void testWithIndexedTypes() throws IgniteCheckedException, InterruptedException {
        test(false, true);
    }

    @Test
    public void testPassed() throws IgniteCheckedException, InterruptedException {
        test(false, false);
    }

    public static void test(boolean peerClassLoadingEnabled, boolean indexedTypes) throws InterruptedException, IgniteCheckedException {

        Set<CacheEntryEvent<? extends Integer, ? extends TestVal>> events = new HashSet<>();

        try (Ignite ignite = Ignition.start(createCfg(peerClassLoadingEnabled))) {

            try (IgniteCache<Integer, TestVal> cache = ignite.getOrCreateCache(createCacheCfg(indexedTypes))) {

                ContinuousQuery<Integer, TestVal> qry = new ContinuousQuery<>();
                qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(remoteFilter()));
                qry.setLocalListener(evts -> evts.forEach(events::add));
                cache.query(qry);

                cache.put(1, new TestVal("old"));

                TestVal oldVal = cache.get(1);

                oldVal.setVal("new");

                cache.put(1, oldVal);

                await().atMost(5, SECONDS).until(() -> events.size() == 1);

            } finally {
                ignite.destroyCache("test");
            }
        }
    }

    private static CacheEntryEventSerializableFilter<Integer, TestVal> remoteFilter() {
        return e -> e.getEventType() == UPDATED &&
                !Objects.equals(e.getOldValue().getVal(), e.getValue().getVal());
    }

    private static CacheConfiguration<Integer, TestVal> createCacheCfg(boolean withIndexedTypes) {
        CacheConfiguration<Integer, TestVal> cfg = new CacheConfiguration<>();
        cfg.setCopyOnRead(true);

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName("test");

        if (withIndexedTypes) {
            cfg.setIndexedTypes(Integer.class, TestVal.class);
        }

        return cfg;
    }

    private static IgniteConfiguration createCfg(Boolean peerClassLoadingEnabled) throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(peerClassLoadingEnabled);

        return cfg;
    }

}
