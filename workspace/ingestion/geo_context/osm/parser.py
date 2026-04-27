from typing import Any, Dict, List


def _extract_lat_lon(el: Dict[str, Any]) -> tuple[float | None, float | None]:
    center = el.get("center", {})
    lat = el.get("lat") or center.get("lat")
    lon = el.get("lon") or center.get("lon")
    return lat, lon


def parse_osm_pois(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []

    for el in elements:
        tags = el.get("tags", {})
        lat, lon = _extract_lat_lon(el)

        rows.append({
            "osm_id": el.get("id"),
            "element_type": el.get("type"),
            "poi_type": tags.get("amenity") or tags.get("shop"),
            "name": tags.get("name"),
            "lat": lat,
            "lon": lon,
            "tags": tags,
        })

    return rows


def parse_osm_roads(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []

    for el in elements:
        tags = el.get("tags", {})
        lat, lon = _extract_lat_lon(el)

        rows.append({
            "osm_id": el.get("id"),
            "element_type": el.get("type"),
            "road_name": tags.get("name"),
            "highway_type": tags.get("highway"),
            "lat": lat,
            "lon": lon,
            "tags": tags,
        })

    return rows


def parse_osm_transit_stops(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []

    for el in elements:
        tags = el.get("tags", {})
        lat, lon = _extract_lat_lon(el)

        rows.append({
            "osm_id": el.get("id"),
            "element_type": el.get("type"),
            "name": tags.get("name"),
            "stop_type": (
                tags.get("highway")
                or tags.get("amenity")
                or tags.get("public_transport")
                or tags.get("railway")
            ),
            "highway": tags.get("highway"),
            "amenity": tags.get("amenity"),
            "public_transport": tags.get("public_transport"),
            "railway": tags.get("railway"),
            "bus": tags.get("bus"),
            "tram": tags.get("tram"),
            "train": tags.get("train"),
            "subway": tags.get("subway"),
            "lat": lat,
            "lon": lon,
            "tags": tags,
        })

    return rows


def parse_osm_railways(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []

    for el in elements:
        tags = el.get("tags", {})
        lat, lon = _extract_lat_lon(el)

        rows.append({
            "osm_id": el.get("id"),
            "element_type": el.get("type"),
            "name": tags.get("name"),
            "railway": tags.get("railway"),
            "route": tags.get("route"),
            "service": tags.get("service"),
            "usage": tags.get("usage"),
            "operator": tags.get("operator"),
            "network": tags.get("network"),
            "electrified": tags.get("electrified"),
            "gauge": tags.get("gauge"),
            "layer": tags.get("layer"),
            "bridge": tags.get("bridge"),
            "tunnel": tags.get("tunnel"),
            "lat": lat,
            "lon": lon,
            "tags": tags,
        })

    return rows