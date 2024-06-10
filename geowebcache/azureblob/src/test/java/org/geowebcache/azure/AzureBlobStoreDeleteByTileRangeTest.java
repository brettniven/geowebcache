package org.geowebcache.azure;

import org.geowebcache.config.DefaultGridsets;
import org.geowebcache.grid.*;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.locks.LockProvider;
import org.geowebcache.locks.NoOpLockProvider;
import org.geowebcache.mime.ApplicationMime;
import org.geowebcache.mime.MimeType;
import org.geowebcache.storage.BlobStoreListener;
import org.geowebcache.storage.TileRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class AzureBlobStoreDeleteByTileRangeTest {

    private static final Logger log = Logger.getLogger(AzureBlobStoreDeleteByTileRangeTest.class.getName());

    private static final String DEFAULT_LAYER_NAME = "natmap:grp_natmap_reference_lines";
    private static final String DEFAULT_LAYER_ID = "LayerGroupInfoImpl--3b14cfc4:18ba4921419:-7f3f";

    private static final String DEFAULT_FORMAT = "application/vnd.mapbox-vector-tile";

    private static final String DEFAULT_GRIDSET = "EPSG:3857";
    //private static final String DEFAULT_GRIDSET = "EPSG:3857x2"; // for 512

    private final BoundingBox BBOX_AUSTRALIA = new BoundingBox(12579102, -5465442, 17143201, -1118889);

    private final BoundingBox BBOX_SAMPLE_LAYER = new BoundingBox(15433782.552690899, -4667097.42039626, 16442064.6836236, -4049627.6833519107);

    // Sample BBox for a Road Management truncate following a claim/transfer
    private final BoundingBox BBOX_SAMPLE_SMALL = new BoundingBox(16195721.53295515, -4591004.456779404, 16196808.263378767, -4590394.688197427);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testDeleteByTileRangeUnbounded0To1() throws Exception {
        runTest(null, 0, 1);
    }

    @Test
    public void testDeleteByTileRangeUnbounded0To4() throws Exception {
        runTest(null, 0, 4);
    }

    @Test
    public void testDeleteByTileRangeUnbounded0To8() throws Exception {
        runTest(null, 0, 8);
    }

    @Test
    public void testDeleteByTileRangeAustraliaBBox0To1() throws Exception {
        runTest(BBOX_AUSTRALIA, 0, 1);
    }

    @Test
    public void testDeleteByTileRangeAustraliaBBox0To4() throws Exception {
        runTest(BBOX_AUSTRALIA, 0, 4);
    }

    @Test
    public void testDeleteByTileRangeAustraliaBBox0To8() throws Exception {
        runTest(BBOX_AUSTRALIA, 0, 8);
    }

    @Test
    public void testDeleteByTileRangeSampleLayerBBox0To1() throws Exception {
        runTest(BBOX_SAMPLE_LAYER, 0, 1);
    }

    @Test
    public void testDeleteByTileRangeSampleLayerBBox0To4() throws Exception {
        runTest(BBOX_SAMPLE_LAYER, 0, 4);
    }

    @Test
    public void testDeleteByTileRangeSampleLayerBBox0To8() throws Exception {
        runTest(BBOX_SAMPLE_LAYER, 0, 8);
    }

    @Test
    public void testDeleteByTileRangeSampleSmallBBox0To1() throws Exception {
        runTest(BBOX_SAMPLE_SMALL, 0, 1);
    }

    @Test
    public void testDeleteByTileRangeSampleSmallBBox0To4() throws Exception {
        runTest(BBOX_SAMPLE_SMALL, 0, 4);
    }

    @Test
    public void testDeleteByTileRangeSampleSmallBBox0To8() throws Exception {
        runTest(BBOX_SAMPLE_SMALL, 0, 8);
    }

    @Test
    public void testDeleteByTileRangeSampleSmallBBox0To15() throws Exception {
        runTest(BBOX_SAMPLE_SMALL, 0, 15);
    }

    @Test
    public void testDeleteByTileRangeSampleSmallBBox0To20() throws Exception {
        runTest(BBOX_SAMPLE_SMALL, 0, 20);
    }

    private void runTest(BoundingBox boundingBox, int zoomStart, int zoomStop) throws Exception {

        long startTime = System.currentTimeMillis();

        AzureBlobStoreData config = getConfiguration();

        GridSet gridset =
                new GridSetBroker(Collections.singletonList(new DefaultGridsets(false, true)))
                        .getWorldEpsg3857();
        GridSubset gridSubSet = GridSubsetFactory.createGridSubSet(gridset);

        BlobStoreListener listener = mock(BlobStoreListener.class);

        final AtomicLong deletedCount = new AtomicLong();
        doAnswer(invocation -> {
            deletedCount.incrementAndGet();
            return null;
        }).when(listener).tileDeleted(anyString(), anyString(), anyString(), nullable(String.class), anyLong(), anyLong(), anyInt(), anyLong());

        TileLayerDispatcher layers = mock(TileLayerDispatcher.class);
        LockProvider lockProvider = new NoOpLockProvider();
        TileLayer layer = mock(TileLayer.class);
        when(layers.getTileLayer(eq(DEFAULT_LAYER_NAME))).thenReturn(layer);
        when(layer.getName()).thenReturn(DEFAULT_LAYER_NAME);
        when(layer.getId()).thenReturn(DEFAULT_LAYER_ID);
        AzureBlobStore blobStore = new AzureBlobStore(config, layers, lockProvider);
        blobStore.addListener(listener);

        MimeType mimeType = getMimeType();
        log.info("mimeType: " + mimeType);

        Map<String, String> parameters = null;

        long[][] rangeBounds = calculateRangeBounds(boundingBox, gridSubSet, layer);

        TileRange tileRange =
                new TileRange(
                        DEFAULT_LAYER_NAME,
                        DEFAULT_GRIDSET,
                        zoomStart,
                        zoomStop,
                        rangeBounds,
                        mimeType,
                        parameters);

        blobStore.delete(tileRange);

        log.info("AzureBlobStore.delete completed. Deleted " + deletedCount.get() + " tiles in total in " + (System.currentTimeMillis() - startTime) + "ms");
    }
    
    protected AzureBlobStoreData getConfiguration() {
        AzureBlobStoreData config = new AzureBlobStoreData();
        config.setContainer(System.getenv("STORAGE_GWC_CONTAINER"));
        config.setAccountName(System.getenv("STORAGE_GWC_ACCOUNT"));
        config.setAccountKey(System.getenv("STORAGE_GWC_ACCESS_KEY"));
        config.setUseHTTPS(true);
        config.setMaxConnections(100);

        return config;
    }

    private MimeType getMimeType() throws Exception {
        return ApplicationMime.mapboxVector;
    }

    /**
     * calculateRangeBounds mimics the logic in TileBreeder.createTileRange, which is called from FormService
     *
     */
    private long[][] calculateRangeBounds(BoundingBox requestBounds, GridSubset gridSubset, TileLayer tl) {

        long[][] coveredGridLevels;

        //BoundingBox bounds = requestBounds;
        if (requestBounds == null) {
            coveredGridLevels = gridSubset.getCoverages();
        } else {
            coveredGridLevels = gridSubset.getCoverageIntersections(requestBounds);
        }

        //int[] metaTilingFactors = tl.getMetaTilingFactors();
        // NOTE; Here, i'm using 4, 4, which is from metaTilingX and metaTilingY from our geowebcache.xml
        final int[] metaTilingFactors = {4, 4};
        coveredGridLevels = gridSubset.expandToMetaFactors(coveredGridLevels, metaTilingFactors);

        return coveredGridLevels;
    }
}
