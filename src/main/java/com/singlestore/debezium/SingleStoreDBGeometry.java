package com.singlestore.debezium;

import io.debezium.util.HexConverter;
import io.github.matthieun.conversion.WktWkbConverter;

public class SingleStoreDBGeometry {

    /**
     * WKT to WKB converter
     */
    private static final WktWkbConverter WKT_WKB_CONVERTER = new WktWkbConverter();
    /**
     * Static Hex EKWB for a GEOMETRYCOLLECTION EMPTY.
     */
    private static final String HEXEWKB_EMPTY_GEOMETRYCOLLECTION = "010700000000000000";

    /**
     * Extended-Well-Known-Binary (EWKB) geometry representation. An extension of the
     * Open Geospatial Consortium Well-Known-Binary format. Since EWKB is a superset of
     * WKB, we use EWKB here.
     * https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT
     * http://www.opengeospatial.org/standards/sfa
     */
    private final byte[] wkb;

    /**
     * Coordinate reference system identifier. While it's technically user-defined,
     * the standard/common values in use are the EPSG code list http://www.epsg.org/
     * null if unset/unspecified
     */
    private final Integer srid;

    /**
     * Create a SingleStoreDBGeometry using the supplied Hex EWKB string.
     */
    public static SingleStoreDBGeometry fromHexEwkb(String hexEwkb) {
        byte[] ewkb = HexConverter.convertFromHex(hexEwkb);
        return fromWkb(ewkb);
    }

    /**
     * Create a SingleStoreDBGeometry using the supplied WKT.
     * srid is null as not specified by Single Store.
     */
    public static SingleStoreDBGeometry fromEkt(String wkt) {
        return new SingleStoreDBGeometry(WKT_WKB_CONVERTER.wktToWkb(wkt), null);
    }

    /**
     * Create a SingleStoreDBGeometry using the supplied WKB.
     */
    public static SingleStoreDBGeometry fromWkb(byte[] wkb) {
        return new SingleStoreDBGeometry(wkb, null);
    }

    /**
     * Create a GEOMETRYCOLLECTION EMPTY SingleStoreDBGeometry
     *
     * @return a {@link SingleStoreDBGeometry} which represents a PostgisGeometry API
     */
    public static SingleStoreDBGeometry createEmpty() {
        return SingleStoreDBGeometry.fromHexEwkb(HEXEWKB_EMPTY_GEOMETRYCOLLECTION);
    }

    /**
     * Create a SingleStoreDBGeometry using the supplied EWKB and SRID.
     *
     * @param ewkb the Extended Well-Known binary representation of the coordinate in the standard format
     * @param srid the coordinate system identifier (SRID); null if unset/unknown
     */
    private SingleStoreDBGeometry(byte[] ewkb, Integer srid) {
        this.wkb = ewkb;
        this.srid = srid;
    }

    /**
     * Returns the standard well-known binary representation
     *
     * @return {@link byte[]} which represents the standard well-known binary
     */
    public byte[] getWkb() {
        return wkb;
    }

    /**
     * Returns the coordinate reference system identifier (SRID)
     *
     * @return srid
     */
    public Integer getSrid() {
        return srid;
    }
}
