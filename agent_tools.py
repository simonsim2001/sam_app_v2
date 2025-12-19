
import requests
import geopandas as gpd
from shapely.geometry import Point, MultiPolygon, Polygon
import math

# IGN Altimetry API (Free, no key required currently for low vol)
IGN_ALTI_URL = "https://data.geopf.fr/altimetrie/1.0/calcul/alti/rest/elevation.json"

def get_elevation_points(points):
    """
    Fetch elevations for a list of (lon, lat) tuples.
    Returns list of Z values.
    """
    # Join with |
    lons = "|".join([str(p[0]) for p in points])
    lats = "|".join([str(p[1]) for p in points])
    
    params = {
        'lon': lons,
        'lat': lats,
        'resource': 'ign_rge_alti_wld', # RGE ALTI (1m or 5m)
        'delimiter': '|',
        'indent': 'false',
        'measures': 'false',
        'zonly': 'false'
    }
    
    try:
        r = requests.get(IGN_ALTI_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        elevations = [item['z'] for item in data['elevations']]
        return elevations
    except Exception as e:
        print(f"IGN API Error: {e}")
        return None

def compute_slope(geometry):
    """
    Estimates slope by probing the Center and 4 surrounding points (N,S,E,W) 10m away.
    """
    try:
        # 1. Get Centroid
        centroid = geometry.centroid
        cx, cy = centroid.x, centroid.y
        
        # We need coords in Degrees for API, but Meters for offset.
        # Assuming geometry passed in is WGS84 (deg). 
        # But wait, offset in degrees is tricky.
        # Ideally we convert to Lambert 93, offset, then convert back.
        # For simplicity in this Agent prototype: 
        # 0.0001 deg ~ 11 meters.
        
        offset = 0.0001
        
        # Probe points: Center, North, South, East, West
        probes_deg = [
            (cx, cy),
            (cx, cy + offset), # N
            (cx, cy - offset), # S
            (cx + offset, cy), # E
            (cx - offset, cy)  # W
        ]
        
        zs = get_elevation_points(probes_deg)
        if not zs:
            return None, None
            
        z_c, z_n, z_s, z_e, z_w = zs
        
        # Calc standard slope (degrees/percent)
        # Rise/Run. Run ~ 11 meters (approx)
        dist = 11.0 
        
        # Simple gradient approx: max difference
        dz_ns = abs(z_n - z_s)
        dz_ew = abs(z_e - z_w)
        
        # Slope % = (rise / run) * 100
        # Check span is 2 * dist
        slope_ns = (dz_ns / (2*dist)) * 100
        slope_ew = (dz_ew / (2*dist)) * 100
        
        # Total approx slope
        slope_max = max(slope_ns, slope_ew)
        
        return round(slope_max, 1), round(z_c, 1)
        
    except Exception as e:
        print(f"Slope Analysis Error: {e}")
        return None, None

def get_owner_info(parcel_id):
    """
    Returns owner status and a pre-filled legal request text.
    """
    # In France, owner data is restricted (CNIL).
    # We return a specific status and a helper text.
    
    status = "🔒 Protected (CNIL)"
    
    # Determine Commune
    commune_code = parcel_id[:5]
    
    commune_data = {
        '73057': {'name': 'Brides-les-Bains', 'email': 'autorisation-urbanisme@mairie-brideslesbains.fr'},
        '73284': {'name': 'Salins-les-Thermes (Salins-Fontaine)', 'email': 'mairie@salins-fontaine.fr'},
        '73055': {'name': 'Bozel', 'email': 'accueil@mairiebozel.fr'},
        '73227': {'name': 'Courchevel', 'email': 'urbanisme@courchevel.com'}
    }
    
    info = commune_data.get(commune_code, {'name': 'Unknown', 'email': 'contact@commune.fr'})
    
    # Template for "Demande de matrice cadastrale" (Cerfa 11565*04 equivalent request)
    request_text = f"""Objet : Demande de relevé de propriété - Parcelle {parcel_id}

Madame, Monsieur,

Je souhaite obtenir, conformément aux articles L.107 A et R.107 A-1 à R.107 A-7 du Livre des procédures fiscales, les informations cadastrales (identité du propriétaire) concernant la parcelle suivante :

Commune : {info['name']} ({commune_code})
Référence cadastrale : {parcel_id}

Je vous remercie de bien vouloir me transmettre ces informations par retour de courriel.

Cordialement,"""

    return status, request_text, info['email']

def get_address(lat, lon):
    """
    Reverse geocoding via French Govt API.
    """
    try:
        url = "https://api-adresse.data.gouv.fr/reverse/"
        params = {'lat': lat, 'lon': lon}
        r = requests.get(url, params=params, timeout=5)
        if r.ok:
            data = r.json()
            if data['features']:
                props = data['features'][0]['properties']
                return props.get('label', 'Unknown Address')
    except Exception as e:
        print(f"Address API Error: {e}")
    return None

def get_transport_info(lat, lon):
    """
    Calculates distance to Olympe Gondola.
    Returns string description and distance in meters.
    """
    # Olympe Gondola (Brides-les-Bains)
    # Approx coords: 45.4526, 6.5658
    hub_lat = 45.4526
    hub_lon = 6.5658
    
    # Haversine
    R = 6371000 # meters
    phi1 = math.radians(lat)
    phi2 = math.radians(hub_lat)
    dphi = math.radians(hub_lat - lat)
    dlambda = math.radians(hub_lon - lon)

    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2) * math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    dist = round(R * c)
    
    dist = round(R * c)
    
    return dist, "Olympe Gondola"

def get_land_price_estimate(lat, lon):
    """
    Queries DVF (Demande de Valeur Foncière) via api.cquest.org.
    Returns estimated Price/m2 based on recent 'Terrain' sales in 1km radius.
    """
    try:
        # 1km radius search
        url = f"http://api.cquest.org/dvf?lat={lat}&lon={lon}&dist=1000"
        r = requests.get(url, timeout=5)
        
        if r.ok:
            data = r.json()
            features = data.get('features', [])
            
            prices_m2 = []
            
            for f in features:
                props = f['properties']
                
                # Filter for "Vente" (Sale)
                if props.get('nature_mutation') != 'Vente':
                    continue
                    
                # Filter for Lands (Terrain)
                # DVF often lumps house+land. We want pure land if possible, 
                # or at least sales where land value is significant.
                # 'brouillon' heuristic: check type_local
                
                # type_local: "Terrain"
                # If "Maison", price is inflated by building. 
                # Let's look for type_local = "Terrain" OR cases where sur_terrain > 0 and val_foncier > 0
                
                # Ideally, we filter purely for "Terrain" type batches
                is_terrain = "Terrain" in [m.get('type_local') for m in props.get('lots', [])]
                # Fallback: check raw string
                # Note: API structure varies. Let's safely calculate Price/m2
                
                valeur = props.get('valeur_fonciere')
                surface = props.get('surface_terrain')
                
                if valeur and surface and surface > 0:
                    # Simple heuristic: Only keep if price/m2 is "reasonable" for land (e.g. < 1000)
                    # to filter out built properties which might have huge vals.
                    # Or check 'nombre_lots_principaux' == 0 (no building).
                    
                    # Better: Check if there is a 'Maison' or 'Appartement' in the transaction
                    types = [l.get('type_local') for l in props.get('lots', []) if l.get('type_local')]
                    if 'Maison' in types or 'Appartement' in types or 'Local industriel' in types:
                        continue # Skip built plots
                        
                    pm2 = valeur / surface
                    if 10 < pm2 < 2000: # Sanity check for Alps
                        prices_m2.append(pm2)
            
            if prices_m2:
                # Return Median
                prices_m2.sort()
                median = prices_m2[len(prices_m2) // 2]
                return round(median)
                
    except Exception as e:
        print(f"Price API Error: {e}")
        
    return None


