import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.time.LocalDate;

public class TestLocalDate {

    public static void main(String ... args) {

        CacheConfiguration<Integer, CacheItem> cfg = new CacheConfiguration<>();
        cfg.setName("test");
        cfg.setIndexedTypes(Integer.class, CacheItem.class);

        try (Ignite ignite = Ignition.start(new IgniteConfiguration())) {
            try (IgniteCache<Integer, CacheItem> cache = ignite.getOrCreateCache(cfg)) {
                cache.put(1, new CacheItem(LocalDate.now()));
                cache.put(2, new CacheItem(LocalDate.now()));
            }
        }
    }

    public static class CacheItem {

        @QuerySqlField(index = true)
        private LocalDate date;

        public CacheItem(LocalDate date) {
            this.date = date;
        }
    }

}
