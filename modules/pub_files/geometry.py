from typing import Tuple, NamedTuple


class Geometry(NamedTuple):
    geometry: str
    srid: int
    latitude: float
    longitude: float
    elevation: float
    formatted_coordinates: str


def build_geometry(*, geometry: str, srid: int) -> Geometry:
    (latitude, longitude, elevation) = parse_coordinates(geometry)
    formatted_coordinates = f'{latitude} {longitude} WGS 84'
    return Geometry(geometry=geometry,
               srid=srid,
               latitude=latitude,
               longitude=longitude,
               elevation=elevation,
               formatted_coordinates=formatted_coordinates)

def parse_coordinates(geometry: str) -> Tuple[float, float, float]:
    """Parse the Geo coordinates from the geometry string."""
    # Point example: POINT Z (-104.745591 40.815536 1653.9151)
    if geometry.startswith('POINT'):
        coordinates = geometry.split('(')[1].replace(')', '')
        parts = coordinates.split(' ')
    elif geometry.startswith('POLYGON'):
        # Polygon example: POLYGON Z ((-104.746013 40.815892 1654.009392,-104.745973 40.815922 1654.052064, ...))
        trimmed = geometry.split('((')[1].replace('))', '')
        first_point = trimmed.split(',')[0]
        parts = first_point.split(' ')
    else:
        raise Exception(f'Location geometry {geometry} is not point or polygon.')
    longitude = float(parts[0])
    latitude = float(parts[1])
    elevation = float(parts[2])
    return latitude, longitude, elevation
