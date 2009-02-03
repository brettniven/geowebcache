package org.geowebcache.layer.wms;

import java.util.Arrays;

import junit.framework.TestCase;

import org.geowebcache.layer.SRS;
import org.geowebcache.layer.Grid;
import org.geowebcache.mime.ImageMime;
import org.geowebcache.util.wms.BBOX;
import org.geowebcache.layer.GridCalculator;

public class MetaTileTest extends TestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void test1MetaTile() throws Exception {
        BBOX bbox = new BBOX(0, 0, 180, 90);
        BBOX gridBase = new BBOX(-180, -90, 180, 90);
        int metaHeight = 1;
        int metaWidth = 1;

        Grid grid = new Grid(SRS.getEPSG4326(), bbox, gridBase, GridCalculator.get4326Resolutions());
        GridCalculator gridCalc = grid.getGridCalculator();
        
        int[] gridPos = { 0, 0, 0 };
        //int[] gridBounds, int[] tileGridPosition, int metaX, int metaY
        WMSMetaTile mt = new WMSMetaTile(
                null, grid.getSRS(), ImageMime.png, 
                gridCalc.getGridBounds(gridPos[2]),
                gridPos, metaWidth, metaHeight, null);

        int[] solution = { 0, 0, 0, 0, 0 };
        boolean test = Arrays.equals(mt.getMetaTileGridBounds(), solution);
        if (!test) {
            System.out.println("1 - " + mt.debugString());
            System.out.println("test1MetaTile {" + Arrays.toString(solution)
                    + "} {" + Arrays.toString(mt.getMetaTileGridBounds()) + "}");
        }
        assertTrue(test);
    }

    public void test2MetaTile() throws Exception {
        BBOX bbox = new BBOX(0, 0, 180, 90);
        BBOX gridBase = new BBOX(-180, -90, 180, 90);
        int metaHeight = 3;
        int metaWidth = 3;

        Grid grid = new Grid(SRS.getEPSG4326(), bbox, gridBase, null);
        GridCalculator gridCalc = grid.getGridCalculator();
        
        int[] gridPos = { 127, 63, 6 };
        WMSMetaTile mt = new WMSMetaTile(
                    null, grid.getSRS(), ImageMime.png, 
                    gridCalc.getGridBounds(gridPos[2]), 
                    gridPos, metaWidth, metaHeight, null);

        int[] solution = { 126, 63, 127, 63, 6 };
        boolean test = Arrays.equals(mt.getMetaTileGridBounds(), solution);
        if (!test) {
            System.out.println("2 - " + mt.debugString());
            System.out.println("test2MetaTile {" + Arrays.toString(solution)
                    + "} {" + Arrays.toString(mt.getMetaTileGridBounds()) + "}");
        }
        assertTrue(test);
    }

    public void test3MetaTile() throws Exception {
        BBOX bbox = new BBOX(0, 0, 20037508.34, 20037508.34);
        BBOX gridBase = new BBOX(
        		-20037508.34, -20037508.34, 
        		20037508.34, 20037508.34);
        int metaHeight = 1;
        int metaWidth = 1;
        
        Grid grid = new Grid(SRS.getEPSG900913(), bbox, gridBase, GridCalculator.get900913Resolutions());
        GridCalculator gridCalc = grid.getGridCalculator();
              
        int[] gridPos = { 0, 0, 0 };
        WMSMetaTile mt = new WMSMetaTile(
                null, grid.getSRS(), ImageMime.png,
                gridCalc.getGridBounds(gridPos[2]), 
                gridPos, metaWidth, metaHeight, null);
        
        int[] solution = { 0, 0, 0, 0, 0 };
        boolean test = Arrays.equals(mt.getMetaTileGridBounds(), solution);
        if (!test) {
            System.out.println("3 - " + mt.debugString());
            System.out.println("test3MetaTile {" + Arrays.toString(solution)
                    + "} {" + Arrays.toString(mt.getMetaTileGridBounds()) + "}");
        }
        assertTrue(test);
    }

    public void test4MetaTile() throws Exception {
        BBOX bbox = new BBOX(0, 0, 20037508.34, 20037508.34);
        BBOX gridBase = new BBOX(
        		-20037508.34, -20037508.34, 
        		20037508.34, 20037508.34);
        
        int metaHeight = 3;
        int metaWidth = 3;
        
        Grid grid = new Grid(SRS.getEPSG900913(), bbox, gridBase, null);
        GridCalculator gridCalc = grid.getGridCalculator();
        
        int[] gridPos = { 70, 70, 6 };
        WMSMetaTile mt = new WMSMetaTile(
                null, grid.getSRS(), ImageMime.png,
        	gridCalc.getGridBounds(gridPos[2]), 
        	gridPos, metaWidth, metaHeight, null);
        
        int[] solution = { 69, 69, 63, 63, 6 };
        boolean test = Arrays.equals(mt.getMetaTileGridBounds(), solution);
        if (test) {

        } else {
            System.out.println("4 - " + mt.debugString());
            System.out.println("test4MetaTile {" + Arrays.toString(solution)
                    + "} {" + Arrays.toString(mt.getMetaTileGridBounds()) + "}");
        }
        assertTrue(test);
    }
}