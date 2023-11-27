/*This function returns the h3 indexes that are intersecting the borderpoints of a specified geometry*/
DROP FUNCTION IF EXISTS temporal.fill_polygon_h3;
CREATE OR REPLACE FUNCTION temporal.fill_polygon_h3(geom geometry)
RETURNS TABLE (h3_index h3index, h3_short smallint, h3_boundary geometry(linestring, 4326), h3_geom geometry(polygon, 4326))
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN query
    WITH border_points AS
    (
        SELECT ((ST_DUMPPOINTS(geom)).geom)::point AS geom
    ),
    polygons AS
    (
        SELECT ((ST_DUMP(geom)).geom)::polygon AS geom
    ),
    h3_ids AS
    (
        SELECT h3_lat_lng_to_cell(b.geom, 3) h3_index
        FROM border_points b
        UNION ALL
        SELECT h3_polygon_to_cells(p.geom, ARRAY[]::polygon[], 3) h3_index
        FROM polygons p
    )
    SELECT sub.h3_index, public.to_short_h3_3(sub.h3_index::bigint) AS h3_short,
        ST_ExteriorRing(ST_SetSRID(geometry(h3_cell_to_boundary(sub.h3_index)), 4326)) as h3_boundary,
        ST_SetSRID(geometry(h3_cell_to_boundary(sub.h3_index)), 4326) as h3_geom
    FROM h3_ids sub
    GROUP BY sub.h3_index;
END;
$function$
