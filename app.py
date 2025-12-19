from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import analyzer
import agent_tools
import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import shape
import threading

DATA_LOCK = threading.Lock()
app = Flask(__name__)
CORS(app)

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

# Ensure we have a base file
BASE_GEOJSON = os.path.join(DATA_DIR, "analysis_73057.geojson")
if not os.path.exists(BASE_GEOJSON):
    print("WARNING: Base analysis file missing. Please run main.py first.")
    GLOBAL_GDF = None
else:
    print("Loading Global GeoDataFrame in memory...")
    GLOBAL_GDF = gpd.read_file(BASE_GEOJSON)

@app.route("/")
def serve_index():
    return send_from_directory(".", "index.html")

@app.route("/data/<path:filename>")
def serve_data(filename):
    return send_from_directory(DATA_DIR, filename)

@app.route("/agent/fetch-parcel-data", methods=["POST"])
def agent_fetch():
    data = request.json
    parcel_id = data.get('id')
    
    if not parcel_id:
        return jsonify({"error": "Missing parcel ID"}), 400
        
    print(f"Agent received request for {parcel_id}")
    try:
        # Load GeoJSON (Performant enough for 6k rows for now)
        # gdf = gpd.read_file(BASE_GEOJSON) <-- REPLACED WITH GLOBAL
        
        with DATA_LOCK:
            if GLOBAL_GDF is None:
                 return jsonify({"error": "Server not initialized with data"}), 500

            # Find Parcel
            # Check if ID exists (column might be string or obj)
            target = GLOBAL_GDF[GLOBAL_GDF['id'] == parcel_id]
            if target.empty:
                return jsonify({"error": "Parcel not found"}), 404
                
            geom = target.iloc[0].geometry
            idx = target.index[0]
            
            # Release lock while doing external IO (Agent Tools) if possible?
            # actually geom is copied, so we can release lock if we want, 
            # BUT we need to ensure index doesn't shift. 
            # For simplicity, we hold lock or re-fetch index. 
            # Let's HOLD lock for safety in this simple iteration, 
            # although it blocks other reads. 
            
            # Wait, computing slope is fast (local raster). 
            # Owner info might be slow (LLM?). 
            # Optimality: Compute outside lock, Write inside lock.
            
        # --- AGENT ACTION (Outside Lock for concurrency) ---
        slope, elevation = agent_tools.compute_slope(geom)
        owner_status, request_text, owner_email = agent_tools.get_owner_info(parcel_id)
        
        # New Enrichments
        centroid = geom.centroid
        lat, lon = centroid.y, centroid.x
        
        address = agent_tools.get_address(lat, lon)
        dist, hub_name = agent_tools.get_transport_info(lat, lon)
        price_m2 = agent_tools.get_land_price_estimate(lat, lon)
        
        if slope is not None:
            # Update GDF (Acquire Lock again)
            with DATA_LOCK:
                # Re-verify index just in case? 
                GLOBAL_GDF.at[idx, 'slope_mean'] = slope
                GLOBAL_GDF.at[idx, 'address'] = address
                GLOBAL_GDF.at[idx, 'dist_to_hub'] = dist
                GLOBAL_GDF.at[idx, 'est_price_m2'] = price_m2
                
                # Save incrementally
                GLOBAL_GDF.to_file(BASE_GEOJSON, driver='GeoJSON')
            
            return jsonify({
                "message": "Agent analysis complete.",
                "slope": slope,
                "elevation": elevation,
                "owner_status": owner_status,
                "owner_request_text": request_text,
                "owner_email": owner_email,
                "address": address,
                "dist_to_hub": dist,
                "hub_name": hub_name,
                "est_price_m2": price_m2,
                "center_lat": lat,
                "center_lon": lon
            })
        else:
             return jsonify({"error": "Agent failed to fetch external data."}), 502
             
    except Exception as e:
        print(f"Agent Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/upload", methods=["POST"])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    file_type = request.form.get('type') # 'dem', 'plu', 'owners'
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        filename = file.filename
        # Save file
        if file_type == 'dem':
            save_path = os.path.join(DATA_DIR, "uploaded_dem.tif")
            # Or .asc, depending. Rasterio handles many.
        elif file_type == 'plu':
            save_path = os.path.join(DATA_DIR, "uploaded_plu.json") # simpler if geojson
            if filename.endswith('.zip') or filename.endswith('.shp'):
                 # We recommend GeoJSON for simplicity in this prototype
                 # But we can try saving arbitrary
                 save_path = os.path.join(DATA_DIR, filename)
        elif file_type == 'owners':
            save_path = os.path.join(DATA_DIR, "uploaded_owners.csv")
        else:
            return jsonify({"error": "Unknown type"}), 400
            
        file.save(save_path)
        print(f"File saved to {save_path}")
        
        # Trigger Re-Analysis
        try:
            print("Triggering enrichment...")
            new_geojson_path = analyzer.enrich_data(
                BASE_GEOJSON, 
                dem_path=save_path if file_type=='dem' else None,
                plu_path=save_path if file_type=='plu' else None,
                owners_path=save_path if file_type=='owners' else None
            )
            with DATA_LOCK:
                 # Reload Global Data since file changed on disk
                 global GLOBAL_GDF
                 GLOBAL_GDF = gpd.read_file(BASE_GEOJSON)
                 
            return jsonify({"message": f"File uploaded and analysis updated!", "path": new_geojson_path})
        except Exception as e:
            print(f"Analysis Failed: {e}")
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting Flask Server on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
