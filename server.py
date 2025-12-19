from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS
import os
import analyzer
import agent_tools
import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import shape
import threading
import re
from pypdf import PdfReader
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()
GEMINI_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_KEY:
    genai.configure(api_key=GEMINI_KEY)
    print("Gemini Configured with Key", flush=True)
else:
    print("WARNING: No Gemini Key found", flush=True)

DATA_LOCK = threading.Lock()
app = Flask(__name__)
app = Flask(__name__)
CORS(app)

PORTFOLIO_FILE = "./data/user_portfolio.json"

def load_portfolio():
    if not os.path.exists(PORTFOLIO_FILE):
        return {"saved_parcels": {}}
    try:
        with open(PORTFOLIO_FILE, 'r') as f:
            return json.load(f)
    except:
        return {"saved_parcels": {}}

def save_portfolio(data):
    with open(PORTFOLIO_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def analyze_with_gemini(file_path, mime_type, api_key=None):
    # Use provided key or fallback to env var
    key_to_use = api_key if api_key else GEMINI_KEY
    
    if not key_to_use:
        return None
    
    try:
        # Use Flash Latest (Stable)
        # Configure specifically for this call if key provided
        if api_key:
            genai.configure(api_key=api_key)
            
        model = genai.GenerativeModel("gemini-flash-latest")
        
        # Upload
        print("Uploading to Gemini...", flush=True)
        uploaded_file = genai.upload_file(file_path, mime_type=mime_type)
        
        # Prompt
        # Prompt
        prompt = """
        You are an expert Surveyor. Analyze this French 'Extrait du Plan Cadastral' carefully to identify the location and plot numbers.
        
        1. **Identify Commune**: Look for "Commune : [Name]" in the top-left or header. Return the EXACT name found (e.g. "Brides-les-Bains", "Bozel", "Salins", "Courchevel").

        2. **Find the Section**: Look in the top-left information box for "Section : [Letter]". It is usually a single letter (e.g. A, B, C, AE...) or double letter.

        3. **Find Plot Numbers**: Look at the map drawing. Extract ALL plot numbers you see inside the land parcels. 
           - **Important**: Read handwritten notes! e.g. "ce que l'on vend" pointing to a plot.
           - Ignore the surrounding text like "Moutiers", "Savoie", dates, or street names.
        
        Return ONLY valid JSON:
        {
            "commune_name": "Brides-les-Bains" (or extracted text),
            "section": "C" (or extracted letter),
            "ids": ["1228", "1230", "1042"] (list of strings found)
        }
        """
        
        print("Generating Analysis...", flush=True)
        response = model.generate_content([uploaded_file, prompt])
        
        # Parse JSON
        txt = response.text
        # Cleanup markdown
        txt = txt.replace('```json', '').replace('```', '')
        return json.loads(txt)
        
    except Exception as e:
        print(f"Gemini Error (Detailed): {e}", flush=True)
        return None

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

# Ensure we have a base file
BASE_GEOJSON = os.path.join(DATA_DIR, "analysis_73057.geojson")
if not os.path.exists(BASE_GEOJSON):
    print("WARNING: Base analysis file missing. Please run main.py first.")
    GLOBAL_GDF = None
else:
    print("Loading Global GeoDataFrame in memory...", flush=True)
    
    # List of files to load
    # 73057: Brides
    # 73284: Salins
    # 73055: Bozel
    # 73227: Courchevel
    FILES = [
        "analysis_73057.geojson", # Keep original with analysis
        "cadastre_73057.json",    # Fallback/Base
        "cadastre_73284.json",
        "cadastre_73055.json",
        "cadastre_73227.json"
    ]
    
    gdfs = []
    
    # helper
    def load_if_exists(fname):
        p = os.path.join(DATA_DIR, fname)
        if os.path.exists(p):
            try:
                print(f"Loading {fname}...", flush=True)
                return gpd.read_file(p)
            except Exception as e:
                print(f"Error loading {fname}: {e}", flush=True)
        return None

    # 1. Load Main Analysis (Brides 73057)
    # Prefer analysis file for 73057 if available, else raw
    if os.path.exists(os.path.join(DATA_DIR, "analysis_73057.geojson")):
        gdfs.append(load_if_exists("analysis_73057.geojson"))
    else:
        gdfs.append(load_if_exists("cadastre_73057.json"))
        
    # 2. Load Others
    for f in ["cadastre_73284.json", "cadastre_73055.json", "cadastre_73227.json"]:
        gdfs.append(load_if_exists(f))
        
    # Filter None
    gdfs = [g for g in gdfs if g is not None]
    
    if gdfs:
        GLOBAL_GDF = pd.concat(gdfs, ignore_index=True)
        # Deduplicate
        GLOBAL_GDF = GLOBAL_GDF.drop_duplicates(subset=['id'])
        print(f"Total Merged Parcels: {len(GLOBAL_GDF)}", flush=True)
        
        # FIX: Timestamp serialization error
        # Convert all datetime objects to string
        for col in GLOBAL_GDF.columns:
            if pd.api.types.is_datetime64_any_dtype(GLOBAL_GDF[col]):
                GLOBAL_GDF[col] = GLOBAL_GDF[col].astype(str)
                print(f"Converted {col} to string", flush=True)
        
        # Pre-compute JSON for performance
        print("Caching GeoJSON in memory... (this may take a moment)", flush=True)
        PARCELS_JSON = GLOBAL_GDF.to_json()
        print(f"GeoJSON Cached. Size: {len(PARCELS_JSON)/1024/1024:.2f} MB", flush=True)
    else:
        print("CRITICAL: No data loaded!", flush=True)
        GLOBAL_GDF = None
        PARCELS_JSON = None

@app.route("/")
def serve_index():
    return send_from_directory(".", "index.html")

@app.route("/data/<path:filename>")
def serve_data(filename):
    return send_from_directory("data", filename)

@app.route("/api/parcels")
def api_parcels():
    if PARCELS_JSON is not None:
        return Response(PARCELS_JSON, mimetype='application/json')
    return jsonify({"error": "No data loaded"}), 500

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
            print(f"Analysis Failed: {e}")
            return jsonify({"error": str(e)}), 500

@app.route("/api/portfolio", methods=["GET"])
def get_portfolio():
    return jsonify(load_portfolio())

@app.route("/api/portfolio/add", methods=["POST"])
def add_to_portfolio():
    data = request.json
    p_id = data.get('id')
    status = data.get('status', 'starred')
    
    if not p_id:
        return jsonify({"error": "Missing ID"}), 400
        
    pf = load_portfolio()
    
    # Update or Add
    if p_id not in pf['saved_parcels']:
        pf['saved_parcels'][p_id] = {
            "added_at": pd.Timestamp.now().isoformat(),
            "status": status,
            "notes": ""
        }
    else:
        # Just update status if exists
        pf['saved_parcels'][p_id]['status'] = status
        
    save_portfolio(pf)
    return jsonify({"message": "Saved", "portfolio": pf})

@app.route("/api/portfolio/remove", methods=["POST"])
def remove_from_portfolio():
    data = request.json
    p_id = data.get('id')
    
    pf = load_portfolio()
    if p_id in pf['saved_parcels']:
        del pf['saved_parcels'][p_id]
        save_portfolio(pf)
        
    pf = load_portfolio()
    if p_id in pf['saved_parcels']:
        del pf['saved_parcels'][p_id]
        save_portfolio(pf)
        
    return jsonify({"message": "Removed", "portfolio": pf})

@app.route("/api/upload-doc", methods=["POST"])
def upload_doc():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    # Save file
    safe_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', file.filename)
    save_path = os.path.join(DATA_DIR, "docs", safe_name)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    file.save(save_path)
    
    # Analyze
    found_ids = []
    section = None
    file_lower = safe_name.lower()
    
    # MIME Type
    mime = "application/pdf" if file_lower.endswith('.pdf') else "image/png"
    if file_lower.endswith('.jpg') or file_lower.endswith('.jpeg'): mime = "image/jpeg"
    
    # Try Gemini First
    print("Attempting Gemini Analysis...")
    # Get API Key from Form
    api_key = request.form.get('api_key')
    gemini_res = analyze_with_gemini(save_path, mime, api_key)
    
    if gemini_res:
        print("Gemini Success:", gemini_res)
        found_ids = gemini_res.get('ids', [])
        section = gemini_res.get('section')
        raw_commune_name = gemini_res.get('commune_name')
        
        # Robust Logic Mapping in Python
        commune_code = None
        commune_name = None
        
        if raw_commune_name:
            # Normalize
            cn = raw_commune_name.lower().strip()
            # Mapping Table
            MAPPING = {
                'brides': '73057',
                'brides-les-bains': '73057',
                'brides les bains': '73057',
                
                'bozel': '73055',
                
                'salins': '73284',
                'salins-les-thermes': '73284',
                'salins-fontaine': '73284',
                'salins les thermes': '73284',
                
                'courchevel': '73227',
                'le praz': '73227',
                'saint-bon-tarentaise': '73227'
            }
            
            # Substring key check
            for k, v in MAPPING.items():
                if k in cn:
                    commune_code = v
                    commune_name = raw_commune_name # Keep original pretty name
                    break
            
            if not commune_code:
                print(f"WARNING: Unknown commune name '{raw_commune_name}'")
    else:
        # Fallback for PDF Text
        if file_lower.endswith('.pdf'):
            try:
                reader = PdfReader(save_path)
                text = ""
                for page in reader.pages:
                    text += page.extract_text() or ""
                
                matches = re.findall(r'\b\d{2,4}\b', text)
                found_ids = sorted(list(set(matches)))
            except Exception as e:
                print(f"PDF Extract Error: {e}")
        commune_code = None
        commune_name = None
            
    # Return URL for frontend (we need to serve it)
    # create a route for /docs/
    
    return jsonify({
        "url": f"/docs/{safe_name}",
        "filename": safe_name,
        "found_ids": found_ids,
        "found_ids": found_ids,
        "section": section,
        "commune_code": commune_code,
        "commune_name": commune_name
    })


@app.route("/docs/<path:filename>")
def serve_docs(filename):
    return send_from_directory(os.path.join(DATA_DIR, "docs"), filename)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting Flask Server on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
