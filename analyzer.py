
import geopandas as gpd
import rasterio
import pandas as pd
from rasterstats import zonal_stats
from shapely.geometry import shape
import os

# France standard projection
EPSG_LAMBERT_93 = 2154

def calculate_slope(dem_path, parcels_gdf):
    """
    Calculates mean slope for each parcel using the DEM.
    """
    if not dem_path or not os.path.exists(dem_path):
        print("No DEM found. Skipping slope calculation.")
        # retain existing if present
        if 'slope_mean' not in parcels_gdf.columns:
            parcels_gdf['slope_mean'] = None
        return parcels_gdf

    # Ensure parcels are in the same CRS as DEM (usually Lambert 93 for IGN)
    try:
        with rasterio.open(dem_path) as src:
            dem_crs = src.crs
            if parcels_gdf.crs != dem_crs:
                print(f"Reprojecting parcels to {dem_crs}...")
                parcels_gdf = parcels_gdf.to_crs(dem_crs)
            
            print("Calculating zonal statistics for slope...")
            # For simplicity, if input is DEM (elevation), we get mean elevation
            # If input is Slope dict (derived), we get mean slope.
            # Real slope calc requires gradient. 
            # We will use 'mean' of the raster provided.
            stats = zonal_stats(parcels_gdf, dem_path, stats="mean")
            parcels_gdf['slope_mean'] = [s['mean'] for s in stats] # Labeling as slope for the app
    except Exception as e:
        print(f"Error in slope calc: {e}")

    return parcels_gdf

def calculate_buildable(plu_gdf, parcels_gdf):
    """
    Intersect parcels with PLU buildable zones.
    """
    if plu_gdf is None:
        print("No PLU data. Skipping buildable area calculation.")
        if 'buildable_area_sqm' not in parcels_gdf.columns:
            parcels_gdf['buildable_area_sqm'] = 0
        return parcels_gdf

    # Reproject
    if parcels_gdf.crs != plu_gdf.crs:
        print("Reprojecting PLU...")
        plu_gdf = plu_gdf.to_crs(parcels_gdf.crs)

    print("Intersecting parcels with PLU...")
    # Intersection
    # We might loose parcels that don't intersect
    # We want to keep all parcels and annotate them
    
    # 1. Overlay to find intersection pieces
    try:
        intersection = gpd.overlay(parcels_gdf, plu_gdf, how='intersection')
        if intersection.empty:
             print("Intersection returned empty.")
             parcels_gdf['buildable_area_sqm'] = 0
             return parcels_gdf
             
        # Calculate area of intersection
        intersection['inter_area'] = intersection.geometry.area
        
        # Group by parcel ID
        buildable = intersection.groupby('id')['inter_area'].sum().reset_index()
        buildable.rename(columns={'inter_area': 'buildable_area_temp'}, inplace=True)
        
        # Merge back
        parcels_gdf = parcels_gdf.merge(buildable, on='id', how='left')
        parcels_gdf['buildable_area_sqm'] = parcels_gdf['buildable_area_temp'].fillna(0)
        parcels_gdf.drop(columns=['buildable_area_temp'], inplace=True)
        
    except Exception as e:
        print(f"Error in PLU calc: {e}")
    
    return parcels_gdf

def add_owners(owners_path, parcels_gdf):
    if not owners_path or not os.path.exists(owners_path):
        if 'owner_name' not in parcels_gdf.columns:
            parcels_gdf['owner_name'] = "Unknown"
        return parcels_gdf
        
    print(f"Loading owners from {owners_path}")
    try:
        # Expect CSV: id,owner_name
        df = pd.read_csv(owners_path)
        # Ensure ID matches string type
        df['id'] = df['id'].astype(str)
        # Check duplicates?
        df = df.drop_duplicates(subset=['id'])
        
        # Merge
        if 'owner_name' in parcels_gdf.columns:
             parcels_gdf = parcels_gdf.drop(columns=['owner_name'])
        
        parcels_gdf = parcels_gdf.merge(df[['id', 'owner_name']], on='id', how='left')
        parcels_gdf['owner_name'] = parcels_gdf['owner_name'].fillna("Unknown")
        
    except Exception as e:
        print(f"Error merging owners: {e}")
        
    return parcels_gdf

def analyze_parcels(cadastre_gdf, dem_path=None, plu_gdf=None):
    # Reproject to Lambert 93 for accurate area processing
    print("Reprojecting to Lambert 93...")
    cadastre_gdf = cadastre_gdf.to_crs(epsg=EPSG_LAMBERT_93)
    
    # Calculate geometric area
    cadastre_gdf['total_area_sqm'] = cadastre_gdf.geometry.area
    
    # Slope
    cadastre_gdf = calculate_slope(dem_path, cadastre_gdf)
    
    # Buildable
    cadastre_gdf = calculate_buildable(plu_gdf, cadastre_gdf)
    
    return cadastre_gdf

def enrich_data(base_geojson_path, dem_path=None, plu_path=None, owners_path=None):
    # Load existing
    print(f"Loading base geojson: {base_geojson_path}")
    gdf = gpd.read_file(base_geojson_path)
    
    # Reproject to Metric (Lambert 93) for calc
    gdf = gdf.to_crs(epsg=EPSG_LAMBERT_93)
    
    if dem_path:
        print("Enriching with Slope...")
        gdf = calculate_slope(dem_path, gdf)
        
    if plu_path:
        print("Enriching with PLU...")
        plu_gdf = gpd.read_file(plu_path)
        gdf = calculate_buildable(plu_gdf, gdf)
        
    if owners_path:
        print("Enriching with Owners...")
        gdf = add_owners(owners_path, gdf)
        
    # Reproject back to WGS84 for App
    print("Reprojecting to WGS84...")
    gdf = gdf.to_crs(epsg=4326)
    
    # Save
    gdf.to_file(base_geojson_path, driver='GeoJSON')
    print("Enrichment complete.")
    return base_geojson_path
