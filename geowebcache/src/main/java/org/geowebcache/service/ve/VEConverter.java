/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author Arne Kepp, The Open Planning Project, Copyright 2008
 */
package org.geowebcache.service.ve;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.layer.SRS;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.mime.MimeException;
import org.geowebcache.mime.MimeType;
import org.geowebcache.service.Service;
import org.geowebcache.service.ServiceException;
import org.geowebcache.tile.Tile;
import org.geowebcache.util.ServletUtils;

/**
 * Class to convert from Virtual Earth quad keys to the internal representation
 * of a tile.
 */
public class VEConverter extends Service {
    public static final String SERVICE_VE = "ve";

    private static Log log = LogFactory
            .getLog(org.geowebcache.service.ve.VEConverter.class);

    public VEConverter() {
        super(SERVICE_VE);
    }

    public Tile getTile(HttpServletRequest request, HttpServletResponse response) 
    throws ServiceException  {
        Map<String,String[]> params = request.getParameterMap();
        
        String layerId = super.getLayersParameter(request);
        
        String strQuadKey = ServletUtils.stringFromMap(params, "quadkey");
        String strFormat = ServletUtils.stringFromMap(params, "format");
        String strCached = ServletUtils.stringFromMap(params, "cached");
        String strMetaTiled = ServletUtils.stringFromMap(params, "metatiled");
        
        int[] gridLoc = VEConverter.convert(strQuadKey);
        
        MimeType mimeType = null;
        if(strFormat != null) {
            try {
                mimeType = MimeType.createFromFormat(strFormat);
            } catch (MimeException me) {
                throw new ServiceException("Unable to determined requested format, " + strFormat);
            }
        }
        
        Tile ret = new Tile(layerId, SRS.getEPSG900913(), gridLoc, mimeType, request, response, null);
        
        if(strCached != null && ! Boolean.parseBoolean(strCached)) {
            ret.setRequestHandler(Tile.RequestHandler.SERVICE);
            if(strMetaTiled != null && ! Boolean.parseBoolean(strMetaTiled)) {
                ret.setHint("not_cached,not_metatiled");
            } else {
                ret.setHint("not_cached");
            }
        }
        
        return ret;
    }
    
    /** 
     * NB The following code is shared across Google Maps, Mobile Google Maps and Virtual Earth
     */
    public void handleRequest(TileLayerDispatcher tLD, Tile tile)
            throws GeoWebCacheException {
        if (tile.hint != null) {
            boolean requestTiled = true;
            if (tile.hint.equals("not_cached,not_metatiled")) {
                requestTiled = false;
            } else if (!tile.hint.equals("not_cached")) {
                throw new GeoWebCacheException("Hint " + tile.hint + " is not known.");
            }

            TileLayer tl = tLD.getTileLayer(tile.getLayerId());

            if (tl == null) {
                throw new GeoWebCacheException("Unknown layer " + tile.getLayerId());
            }
            
            if(! tl.isCacheBypassAllowed().booleanValue()) {
                throw new GeoWebCacheException("Layer " + tile.getLayerId() 
                        + " is not configured to allow bypassing the cache.");
            }

            tile.setTileLayer(tl);
            tl.getNoncachedTile(tile, requestTiled);
            
            Service.writeResponse(tile, false);
        }
    }

    /**
     * Convert a quadkey into the internal representation {x,y,z} of a grid
     * location
     * 
     * @param quadKey
     * @return internal representation
     */
    public static int[] convert(String strQuadKey) {
        char[] quadArray = strQuadKey.toCharArray();

        int zoomLevel = quadArray.length;

        int extent = (int) Math.pow(2, zoomLevel);
        int yPos = 0;
        int xPos = 0;

        // Now we traverse the quadArray from left to right, interpretation
        // 0 1
        // 2 3
        // see http://msdn2.microsoft.com/en-us/library/bb259689.aspx
        //
        // What we'll end up with is the top left hand corner of the bbox
        //
        for (int i = 0; i < zoomLevel; i++) {
            char curChar = quadArray[i];

            // For each round half as much is at stake
            extent = extent / 2;

            if (curChar == '0') {
                // X stays
                yPos += extent;
            } else if (curChar == '1') {
                xPos += extent;
                yPos += extent;
            } else if (curChar == '2') {
                // X stays
                // Y stays
            } else if (curChar == '3') {
                xPos += extent;
                // Y stays
            } else {
                log.error("Don't know how to interpret quadKey: " + strQuadKey);
                return null;
            }
        }

        int[] gridLoc = { xPos, yPos, zoomLevel };

        return gridLoc;
    }
}
