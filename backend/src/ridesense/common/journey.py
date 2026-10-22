"""Stop aliases and nearby-stop matching for GTFS journey searches."""

from __future__ import annotations


STOP_ALIASES = {
    "whitefield": ("hope farm", "kadugodi", "itpl", "seegehalli", "whitefield"),
    "majestic": ("kempegowda bus station", "kbs", "majestic"),
    "kempegowda": ("kempegowda bus station", "majestic", "kbs"),
}


def stop_patterns(query: str) -> list[str]:
    value = " ".join(query.strip().lower().split())
    terms = [value, *STOP_ALIASES.get(value, ())]
    return list(dict.fromkeys(f"%{term}%" for term in terms if term))


def search_stops(cur, query: str, limit: int = 20) -> list[dict]:
    patterns = stop_patterns(query)
    term = query.strip()
    cur.execute(
        """SELECT s.stop_id,s.name,s.lat,s.lon,
                  (SELECT count(DISTINCT rs.route_id) FROM route_stops rs WHERE rs.stop_id=s.stop_id) AS route_count
           FROM stops s
           WHERE (%s='' OR s.stop_id ILIKE %s OR s.name ILIKE ANY(%s))
           ORDER BY CASE WHEN lower(s.name)=lower(%s) THEN 0
                         WHEN s.name ILIKE %s THEN 1 ELSE 2 END,
                    route_count DESC,s.name,s.stop_id LIMIT %s""",
        (term, f"%{term}%", patterns or ["%%"], term, f"%{term}%", limit),
    )
    return cur.fetchall()


def find_journey_options(cur, from_stop_id: str, to_stop_id: str, limit: int = 80,
                         region_id: str | None = None) -> list[dict]:
    """Match nearby GTFS platform stops, then return route segments and ordered stops."""
    cur.execute(
        """WITH anchors AS (
               SELECT 'start'::text AS role,stop_id,lat,lon FROM stops WHERE stop_id=%s
               UNION ALL
               SELECT 'destination'::text,stop_id,lat,lon FROM stops WHERE stop_id=%s
           ), candidates AS (
               SELECT a.role,s.stop_id,s.name,s.lat,s.lon,
                      sqrt(power((s.lat-a.lat)*111.32,2) +
                           power((s.lon-a.lon)*111.32*cos(radians(a.lat)),2)) AS distance_km
               FROM anchors a CROSS JOIN stops s
               WHERE sqrt(power((s.lat-a.lat)*111.32,2) +
                          power((s.lon-a.lon)*111.32*cos(radians(a.lat)),2)) <= 0.45
           ), journeys AS (
               SELECT DISTINCT ON (r.route_id,a.direction_id)
                      r.route_id,r.short_name,r.long_name,r.usable,r.simulated,
                      a.direction_id,a.seq AS start_seq,b.seq AS destination_seq,
                      a.progress_m AS start_progress_m,b.progress_m AS destination_progress_m,
                      (b.progress_m-a.progress_m)/1000.0 AS journey_distance_km,rd.length_m AS route_length_m,
                      ca.stop_id AS matched_start_stop_id,ca.name AS matched_start_stop,
                      cb.stop_id AS matched_destination_stop_id,cb.name AS matched_destination_stop,
                      ca.distance_km+cb.distance_km AS match_distance_km
               FROM candidates ca
               JOIN route_stops a ON ca.role='start' AND a.stop_id=ca.stop_id
               JOIN route_stops b ON b.route_id=a.route_id AND b.direction_id=a.direction_id
                                  AND b.seq>a.seq
               JOIN candidates cb ON cb.role='destination'
               JOIN route_stops rb ON rb.route_id=b.route_id AND rb.direction_id=b.direction_id
                                    AND rb.seq=b.seq AND rb.stop_id=cb.stop_id
               JOIN route_directions rd ON rd.route_id=a.route_id AND rd.direction_id=a.direction_id
               JOIN routes r ON r.route_id=a.route_id
               WHERE r.usable=true AND (%s::text IS NULL OR EXISTS (
                   SELECT 1 FROM route_regions rr WHERE rr.route_id=r.route_id AND rr.region_id=%s))
               ORDER BY r.route_id,a.direction_id,match_distance_km,a.seq,b.seq
           )
           SELECT j.route_id,j.short_name,j.long_name,j.usable,j.simulated,j.direction_id,
                  j.start_seq,j.destination_seq,j.start_progress_m,j.destination_progress_m,
                  j.journey_distance_km,j.route_length_m,j.matched_start_stop_id,j.matched_start_stop,
                  j.matched_destination_stop_id,j.matched_destination_stop,
                  (SELECT count(*) FROM vehicle_latest v WHERE v.route_id=j.route_id
                    AND v.updated_at>now()-(%s * interval '1 second')) AS live_buses,
                  (SELECT v.vehicle_id FROM vehicle_latest v WHERE v.route_id=j.route_id
                    AND v.updated_at>now()-(%s * interval '1 second') ORDER BY v.occ_est_pct,v.vehicle_id LIMIT 1) AS least_crowded_bus,
                  (SELECT v.occ_est_pct FROM vehicle_latest v WHERE v.route_id=j.route_id
                    AND v.updated_at>now()-(%s * interval '1 second') ORDER BY v.occ_est_pct,v.vehicle_id LIMIT 1) AS least_crowded_pct,
                  COALESCE((SELECT jsonb_agg(jsonb_build_object(
                      'stop_id',s.stop_id,'name',s.name,'lat',s.lat,'lon',s.lon
                  ) ORDER BY rs.seq) FROM route_stops rs JOIN stops s USING(stop_id)
                    WHERE rs.route_id=j.route_id AND rs.direction_id=j.direction_id
                      AND rs.seq BETWEEN j.start_seq AND j.destination_seq),'[]'::jsonb) AS journey_stops
           FROM journeys j
           ORDER BY j.simulated DESC,j.short_name,j.route_id LIMIT %s""",
        (from_stop_id, to_stop_id, region_id, region_id, 75, 75, 75, limit),
    )
    return cur.fetchall()


def find_transfer_options(cur, from_stop_id: str, to_stop_id: str, limit: int = 12) -> list[dict]:
    """Find two-route journeys through a shared usable GTFS interchange stop.

    Start/end coordinates are represented by the selected GTFS stop IDs. Nearby
    platform stops are considered for each end, while transfers require the same
    GTFS stop ID on both routes to avoid inventing unsafe walking connections.
    """
    cur.execute(
        """WITH anchors(role,stop_id) AS (
               VALUES ('start'::text,%s::text),('destination'::text,%s::text)
           ), candidates AS (
               SELECT a.role,n.stop_id,n.name,n.lat,n.lon,n.distance_km
               FROM anchors a
               CROSS JOIN LATERAL (
                   SELECT s.stop_id,s.name,s.lat,s.lon,
                          sqrt(power((s.lat-origin.lat)*111.32,2) +
                               power((s.lon-origin.lon)*111.32*cos(radians(origin.lat)),2)) AS distance_km
                   FROM stops origin CROSS JOIN stops s
                   WHERE origin.stop_id=a.stop_id
                     AND sqrt(power((s.lat-origin.lat)*111.32,2) +
                              power((s.lon-origin.lon)*111.32*cos(radians(origin.lat)),2)) <= 0.45
                   ORDER BY distance_km,s.stop_id LIMIT 8
               ) n
           ), interchanges AS (
               SELECT rs.stop_id
               FROM route_stops rs JOIN routes r USING(route_id)
               WHERE r.usable=true
               GROUP BY rs.stop_id
               HAVING count(DISTINCT rs.route_id)>1
           ), first_legs AS (
               SELECT origin.stop_id AS origin_stop_id,origin.distance_km AS origin_distance_km,
                      r1.route_id AS route1_id,r1.short_name AS route1_short_name,
                      r1.long_name AS route1_long_name,r1.simulated AS route1_simulated,
                      d1.length_m AS route1_length_m,
                      a.direction_id AS direction1,a.seq AS start1_seq,a.progress_m AS start1_progress_m,
                      b.seq AS end1_seq,b.progress_m AS end1_progress_m,
                      b.stop_id AS transfer_stop_id,t.name AS transfer_stop_name,
                      t.lat AS transfer_lat,t.lon AS transfer_lon
               FROM candidates origin
               JOIN route_stops a ON a.stop_id=origin.stop_id
               JOIN route_stops b ON b.route_id=a.route_id AND b.direction_id=a.direction_id AND b.seq>a.seq
               JOIN interchanges i ON i.stop_id=b.stop_id
               JOIN stops t ON t.stop_id=b.stop_id
               JOIN route_directions d1 ON d1.route_id=a.route_id AND d1.direction_id=a.direction_id AND d1.usable=true
               JOIN routes r1 ON r1.route_id=a.route_id AND r1.usable=true
               WHERE origin.role='start' AND b.progress_m-a.progress_m BETWEEN 500 AND 30000
           ), pairs AS (
               SELECT f.*,destination.stop_id AS destination_stop_id,
                      destination.distance_km AS destination_distance_km,
                      r2.route_id AS route2_id,r2.short_name AS route2_short_name,
                      r2.long_name AS route2_long_name,r2.simulated AS route2_simulated,
                      c.direction_id AS direction2,c.seq AS start2_seq,c.progress_m AS start2_progress_m,
                      e.seq AS end2_seq,e.progress_m AS end2_progress_m,
                      d2.length_m AS route2_length_m,
                      (f.end1_progress_m-f.start1_progress_m + e.progress_m-c.progress_m)/1000.0 AS total_distance_km,
                      (f.origin_distance_km+destination.distance_km) AS access_distance_km
               FROM first_legs f
               JOIN route_stops c ON c.stop_id=f.transfer_stop_id
               JOIN route_stops e ON e.route_id=c.route_id AND e.direction_id=c.direction_id AND e.seq>c.seq
               JOIN candidates destination ON destination.role='destination' AND destination.stop_id=e.stop_id
               JOIN route_directions d2 ON d2.route_id=c.route_id AND d2.direction_id=c.direction_id AND d2.usable=true
               JOIN routes r2 ON r2.route_id=c.route_id AND r2.usable=true
               WHERE destination.role='destination' AND r2.route_id<>f.route1_id
                 AND e.progress_m-c.progress_m BETWEEN 500 AND 30000
           ), best_pairs AS (
               SELECT DISTINCT ON (route1_id,direction1,route2_id,direction2) *
               FROM pairs
               ORDER BY route1_id,direction1,route2_id,direction2,
                        access_distance_km,total_distance_km,transfer_stop_id
           )
           SELECT route1_id,route2_id,transfer_stop_id,transfer_stop_name,transfer_lat,transfer_lon,
                  total_distance_km,access_distance_km,
                  jsonb_build_object(
                      'route_id',route1_id,'short_name',route1_short_name,'long_name',route1_long_name,
                      'simulated',route1_simulated,'direction_id',direction1,
                      'start_seq',start1_seq,'destination_seq',end1_seq,
                      'start_progress_m',start1_progress_m,'destination_progress_m',end1_progress_m,
                      'journey_distance_km',(end1_progress_m-start1_progress_m)/1000.0,
                      'route_length_m',route1_length_m,
                      'matched_start_stop_id',origin_stop_id,'matched_start_stop',
                          (SELECT name FROM stops WHERE stop_id=origin_stop_id),
                      'matched_destination_stop_id',transfer_stop_id,'matched_destination_stop',transfer_stop_name,
                      'journey_stops',COALESCE((SELECT jsonb_agg(jsonb_build_object(
                          'stop_id',s.stop_id,'name',s.name,'lat',s.lat,'lon',s.lon) ORDER BY rs.seq)
                          FROM route_stops rs JOIN stops s USING(stop_id)
                          WHERE rs.route_id=route1_id AND rs.direction_id=direction1
                            AND rs.seq BETWEEN start1_seq AND end1_seq),'[]'::jsonb)
                  ) AS leg1,
                  jsonb_build_object(
                      'route_id',route2_id,'short_name',route2_short_name,'long_name',route2_long_name,
                      'simulated',route2_simulated,'direction_id',direction2,
                      'start_seq',start2_seq,'destination_seq',end2_seq,
                      'start_progress_m',start2_progress_m,'destination_progress_m',end2_progress_m,
                      'journey_distance_km',(end2_progress_m-start2_progress_m)/1000.0,
                      'route_length_m',route2_length_m,
                      'matched_start_stop_id',transfer_stop_id,'matched_start_stop',transfer_stop_name,
                      'matched_destination_stop_id',destination_stop_id,'matched_destination_stop',
                          (SELECT name FROM stops WHERE stop_id=destination_stop_id),
                      'journey_stops',COALESCE((SELECT jsonb_agg(jsonb_build_object(
                          'stop_id',s.stop_id,'name',s.name,'lat',s.lat,'lon',s.lon) ORDER BY rs.seq)
                          FROM route_stops rs JOIN stops s USING(stop_id)
                          WHERE rs.route_id=route2_id AND rs.direction_id=direction2
                            AND rs.seq BETWEEN start2_seq AND end2_seq),'[]'::jsonb)
                  ) AS leg2
           FROM best_pairs
           ORDER BY total_distance_km,access_distance_km,route1_short_name,route2_short_name
           LIMIT %s""",
        (from_stop_id, to_stop_id, limit),
    )
    return cur.fetchall()
