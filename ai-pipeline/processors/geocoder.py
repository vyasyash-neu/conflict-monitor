import logging
from functools import lru_cache
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

log = logging.getLogger(__name__)

# Known locations for the US-Iran conflict (faster than geocoding API)
KNOWN_LOCATIONS = {
    "tehran": (35.6892, 51.3890),
    "isfahan": (32.6546, 51.6680),
    "qom": (34.6399, 50.8759),
    "karaj": (35.8400, 50.9391),
    "kermanshah": (34.3142, 47.0650),
    "bandar abbas": (27.1832, 56.2666),
    "bushehr": (28.9234, 50.8203),
    "minab": (27.1064, 57.0810),
    "shiraz": (29.5918, 52.5837),
    "tabriz": (38.0800, 46.2919),
    "mashhad": (36.2605, 59.6168),
    "ahvaz": (31.3183, 48.6706),
    "strait of hormuz": (26.5667, 56.2500),
    "persian gulf": (26.0000, 52.0000),
    "gulf of oman": (24.5000, 58.5000),
    # Israel
    "tel aviv": (32.0853, 34.7818),
    "jerusalem": (31.7683, 35.2137),
    "haifa": (32.7940, 34.9896),
    "beersheba": (31.2518, 34.7913),
    # US bases / Gulf
    "al udeid": (25.1175, 51.3150),
    "bahrain": (26.0667, 50.5577),
    "fifth fleet": (26.2361, 50.5860),
    "camp arifjan": (29.1500, 48.0833),
    "kuwait": (29.3759, 47.9774),
    "prince sultan air base": (24.0627, 47.5802),
    "al dhafra": (24.2500, 54.5500),
    "abu dhabi": (24.4539, 54.3773),
    "erbil": (36.1911, 44.0094),
    # Lebanon
    "beirut": (33.8938, 35.5018),
    "beqaa valley": (33.8500, 35.9000),
    "beqaa": (33.8500, 35.9000),
    # Saudi Arabia
    "riyadh": (24.7136, 46.6753),
    "jeddah": (21.4858, 39.1925),
    # Iraq
    "baghdad": (33.3152, 44.3661),
    "basra": (30.5085, 47.7804),
    # Yemen
    "sanaa": (15.3694, 44.1910),
    # Jordan
    "amman": (31.9454, 35.9284),
    # Syria
    "damascus": (33.5138, 36.2765),
    # Cyprus
    "cyprus": (35.1264, 33.4299),
    "nicosia": (35.1856, 33.3823),
}


class Geocoder:
    def __init__(self):
        self._nominatim = Nominatim(user_agent="conflict-monitor-v1", timeout=5)

    @lru_cache(maxsize=2000)
    def geocode(self, location_name: str):
        """Return (lat, lng) for a location name, or None."""
        if not location_name:
            return None

        normalized = location_name.lower().strip()

        # Check known locations first
        for key, coords in KNOWN_LOCATIONS.items():
            if key in normalized:
                return coords

        # Fallback to Nominatim
        try:
            result = self._nominatim.geocode(location_name, language="en")
            if result:
                return (result.latitude, result.longitude)
        except GeocoderTimedOut:
            log.warning(f"⚠️ Geocoder timeout for: {location_name}")
        except Exception as e:
            log.warning(f"⚠️ Geocoder error for {location_name}: {e}")

        return None