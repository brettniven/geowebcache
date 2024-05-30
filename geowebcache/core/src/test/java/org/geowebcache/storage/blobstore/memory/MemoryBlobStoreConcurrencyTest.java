package org.geowebcache.storage.blobstore.memory;

import org.geotools.util.logging.Logging;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.io.Resource;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.TileObject;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class MemoryBlobStoreConcurrencyTest {

    static final Logger LOG = Logging.getLogger(MemoryBlobStoreWithFilesComformanceTest.class.getName());

    @Test
    public void testConcurrencyWithOriginalSettings() throws Exception {
        // ensure that the default settings are used
        System.clearProperty("memoryblobstore.executor.threads");
        issue1000PutsMimickingSlowFileWrites();
        // no assertions
    }

    @Test
    public void testConcurrencyWith10ExecutorThreads() throws Exception {
        // configure MemoryBlobStore to use 10 executor threads
        System.setProperty("memoryblobstore.executor.threads", "10");
        issue1000PutsMimickingSlowFileWrites();
        // no assertions
    }


    private void issue1000PutsMimickingSlowFileWrites() throws Exception {

        // Construct a MemoryBlobStore with a mocked delegate store, which we'll use to mimic slow file writes
        MemoryBlobStore memoryBlobStore = buildMemoryBlobStoreWithMockedDelegateStore();

        LOG.info("Issuing 1000 puts");

        // invoke store.put 1000 times, in 20 parallel threads, using an executor service
        // this mimics, for example, 20 http request worker threads
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 1000; i++) {
            int finalI = i + 1;
            executorService.submit(() -> {
                try {
                    memoryBlobStore.put(createTileObject(finalI));
                } catch (Exception e) {
                    LOG.severe(e.getMessage());
                }
            });
        }

        LOG.info("Waiting for puts to complete");
        long startTimeMillis = System.currentTimeMillis();

        executorService.shutdown();
        executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS);

        LOG.info("Finished waiting for puts. Total time: " + (System.currentTimeMillis() - startTimeMillis) + "ms");
    }

    private static MemoryBlobStore buildMemoryBlobStoreWithMockedDelegateStore() {
        MemoryBlobStore memoryBlobStore = new MemoryBlobStore();
        // use an overridden NullBlobStore to mimic slow file writes
        NullBlobStore nullBlobStore = new NullBlobStore() {
            @Override
            public void put(TileObject stObj) throws StorageException {
                // simply sleep, mimicking a slow delegate store write (like a FileBlobStore write)
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    throw new StorageException(e.getMessage());
                }
                super.put(stObj);
            }
        };
        memoryBlobStore.setStore(nullBlobStore);
        return memoryBlobStore;
    }

    private TileObject createTileObject(int differentiator) {
        Resource bytes = new ByteArrayResource("1 2 3 4 5 6 test".getBytes());
        long[] xyz = {differentiator, 2L, 3L};
        Map<String, String> parameters = new HashMap<>();
        return TileObject.createCompleteTileObject(
                "someLayerName", xyz, "EPSG:4326", "application/vnd.mapbox-vector-tile", parameters, bytes);
    }

}
