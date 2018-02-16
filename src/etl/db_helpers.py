def make_point_geom(lat: float, lon: float) -> str:
    """Helper function to create GEO-Alchemy geometry string for a OSM node"""
    return 'POINT({} {})'.format(lon, lat)

def make_bbox_polygon(min_lat:float, min_long:float, max_lat:float, max_long:float) -> str:
    points = [(min_long, min_lat), (min_long, max_lat), (max_long, max_lat), (max_long, min_lat), (min_long, min_lat)]
    merged = ",".join(["{} {}".format(lon, lat) for lat, lon in points])
    return 'POLYGON(({}))'.format(merged)