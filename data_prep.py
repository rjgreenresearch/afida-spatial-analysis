#!/usr/bin/env python3
"""
MTS Research Programme -- Data Preparation Pipeline
===================================================

Converts raw source files into the standardized CSVs that
spatial_analysis_primary.py expects.

Inputs:
  --mirta     Path to mirta-dod-sites-points-geojson.geojson
  --afida     Path to AFIDACurrentHoldingsYR2024.xlsx
  --output    Output directory (default: ./processed)

Outputs:
  cfius_appendix_a_geocoded.csv   -- Installation database from MIRTA
  afida_2024_holdings.csv         -- Cleaned AFIDA holdings
  county_centroids.csv            -- FIPS -> lat/lon lookup
  china_county_summary.csv        -- China holdings aggregated by county

Author: Robert J. Green
"""

import argparse
import json
import os
import sys
import csv
from pathlib import Path

# ----------------------------------------------------------
# 1. MIRTA GeoJSON -> cfius_appendix_a_geocoded.csv
# ----------------------------------------------------------

def convert_mirta(geojson_path, output_dir):
    """
    Convert MIRTA DoD Sites GeoJSON (points OR boundaries) to the CSV
    format expected by spatial_analysis_primary.py.
    
    Handles two MIRTA file variants:
      - Points version: geometry.type = "Point", coordinates = [lon, lat]
      - Boundaries version: geometry.type = "Polygon"/"MultiPolygon",
        coordinates = polygon rings -> computes centroid
    
    Also handles formatting differences between versions:
      - countryName: "usa" vs "United States of America"
      - stateNameCode: "va" vs "VA"
      - siteReportingComponent: "usn" vs "US Navy"
      - isFirrmaSite: "Yes"/"yes"/"No"/"tbd"
    """
    print("\n[1] Converting MIRTA GeoJSON...")
    
    # Handle zip files
    if geojson_path.endswith('.zip'):
        import zipfile, tempfile
        tmpdir = tempfile.mkdtemp()
        with zipfile.ZipFile(geojson_path) as zf:
            geojson_files = [n for n in zf.namelist() if n.endswith('.geojson')]
            if not geojson_files:
                raise ValueError(f"No .geojson file found in {geojson_path}")
            zf.extract(geojson_files[0], tmpdir)
            geojson_path = os.path.join(tmpdir, geojson_files[0])
            print(f"    Extracted: {geojson_files[0]}")
    
    with open(geojson_path) as f:
        data = json.load(f)
    
    features = data['features']
    geom_type = features[0]['geometry']['type'] if features else 'Unknown'
    print(f"    Total MIRTA features: {len(features)}")
    print(f"    Geometry type: {geom_type}")
    
    # CONUS state codes
    CONUS_STATES = {
        'al','ar','az','ca','co','ct','de','fl','ga','ia','id','il','in',
        'ks','ky','la','ma','md','me','mi','mn','mo','ms','mt','nc','nd',
        'ne','nh','nj','nm','nv','ny','oh','ok','or','pa','ri','sc','sd',
        'tn','tx','ut','va','vt','wa','wi','wv','wy','dc'
    }
    
    def polygon_centroid(coords):
        """Compute centroid from polygon coordinate rings."""
        # Use the outer ring (first ring)
        ring = coords[0] if isinstance(coords[0][0], list) else coords
        lons = [p[0] for p in ring]
        lats = [p[1] for p in ring]
        return sum(lons) / len(lons), sum(lats) / len(lats)
    
    def get_centroid(geometry):
        """Extract centroid from Point, Polygon, or MultiPolygon."""
        gtype = geometry['type']
        coords = geometry['coordinates']
        if gtype == 'Point':
            return coords[0], coords[1]  # lon, lat
        elif gtype == 'Polygon':
            return polygon_centroid(coords)
        elif gtype == 'MultiPolygon':
            # Use largest polygon (most points)
            largest = max(coords, key=lambda p: len(p[0]))
            return polygon_centroid(largest)
        else:
            return None, None
    
    rows = []
    for feat in features:
        props = feat['properties']
        lon, lat = get_centroid(feat['geometry'])
        if lon is None:
            continue
        
        # Normalize state (handle both "va" and "VA")
        state = props.get('stateNameCode', '').lower().strip()
        is_conus = 'Y' if state in CONUS_STATES else 'N'
        
        # Normalize isFirrmaSite (handle Yes/yes/tbd/No)
        is_firrma_raw = str(props.get('isFirrmaSite', 'No'))
        is_firrma = 'Yes' if is_firrma_raw.lower() in ('yes', 'tbd') else 'No'
        current_part = '2' if is_firrma == 'Yes' else 'none'
        
        # Normalize component (handle "usn" and "US Navy")
        component = props.get('siteReportingComponent', '').lower()
        component = component.replace('us ', 'us').replace(' ', '_')
        
        rows.append({
            'site_name': props.get('siteName', ''),
            'feature_name': props.get('featureName', ''),
            'latitude': round(lat, 6),
            'longitude': round(lon, 6),
            'state': state,
            'component': component,
            'installation_id': props.get('installationId', ''),
            'is_firrma': is_firrma,
            'status': (props.get('siteOperationalStatus') or 'unk').lower()[:3],
            'conus': is_conus,
            'current_part': current_part,
            'object_id': props.get('OBJECTID', ''),
        })
    
    # Write full database
    out_path = os.path.join(output_dir, 'mirta_all_sites.csv')
    fieldnames = list(rows[0].keys())
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"    All sites -> {out_path} ({len(rows)} rows)")
    
    # Write FIRRMA-only (the cfius_appendix_a_geocoded.csv the script expects)
    firrma_rows = [r for r in rows if r['is_firrma'] == 'Yes']
    firrma_conus = [r for r in firrma_rows if r['conus'] == 'Y']
    
    out_path_firrma = os.path.join(output_dir, 'cfius_appendix_a_geocoded.csv')
    with open(out_path_firrma, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(firrma_rows)
    
    print(f"    FIRRMA sites -> {out_path_firrma} ({len(firrma_rows)} total, {len(firrma_conus)} CONUS)")
    
    # Summary by component
    components = {}
    for r in firrma_conus:
        c = r['component']
        components[c] = components.get(c, 0) + 1
    print(f"    FIRRMA CONUS by component:")
    for c, n in sorted(components.items(), key=lambda x: -x[1]):
        print(f"      {c}: {n}")
    
    print(f"""
    [!]  NOTE: MIRTA contains {len(firrma_conus)} CONUS FIRRMA sites.
       The full CFIUS Appendix A lists ~230 sites including DOE
       nuclear weapons complex facilities (Los Alamos, Sandia,
       Pantex, Y-12, etc.) and other non-DoD sites that are NOT
       in the MIRTA database. For complete coverage, supplement
       this output with manually geocoded DOE/IC sites from the
       Federal Register Appendix A text (31 CFR Part 802).
       
       The current_part column defaults to '2' for all FIRRMA sites.
       To apply accurate Part 1/Part 2 classification, cross-reference
       with the Federal Register rule text and update the column.
       A supplemental mapping file (appendix_a_part_classification.csv)
       can be merged to add this classification.
    """)
    
    return firrma_rows


# ----------------------------------------------------------
# 1b. Merge legacy installations not in MIRTA
# ----------------------------------------------------------

def merge_legacy(firrma_rows, legacy_path, output_dir):
    """
    Merge legacy installation database with MIRTA FIRRMA sites.
    
    Adds sites from the legacy database that are NOT already in the
    MIRTA FIRRMA set (matched by proximity within 10 miles).
    This captures:
      - DOE nuclear weapons complex sites (not in MIRTA)
      - Defense contractor facilities (not flagged as FIRRMA)
      - Sites with MIRTA metadata errors (e.g., Whiteman/Barksdale
        flagged isFirrmaSite=No despite being on Appendix A)
    """
    print("\n[1b] Merging legacy installations...")
    
    import math
    def haversine(lat1, lon1, lat2, lon2):
        R = 3958.8
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat/2)**2 + 
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
             math.sin(dlon/2)**2)
        return R * 2 * math.asin(math.sqrt(a))
    
    with open(legacy_path) as f:
        reader = csv.DictReader(f)
        legacy = list(reader)
    
    print(f"    Legacy sites loaded: {len(legacy)}")
    
    # Find legacy sites not already in FIRRMA set
    added = []
    already_present = 0
    for leg in legacy:
        try:
            leg_lat = float(leg.get('latitude', 0))
            leg_lon = float(leg.get('longitude', 0))
        except (ValueError, TypeError):
            continue
        
        matched = False
        for fr in firrma_rows:
            try:
                dist = haversine(leg_lat, leg_lon, 
                                 float(fr['latitude']), float(fr['longitude']))
                if dist < 10:
                    matched = True
                    break
            except:
                continue
        
        if not matched:
            state = leg.get('state', '').lower()
            CONUS_STATES = {
                'al','ar','az','ca','co','ct','de','fl','ga','ia','id','il','in',
                'ks','ky','la','ma','md','me','mi','mn','mo','ms','mt','nc','nd',
                'ne','nh','nj','nm','nv','ny','oh','ok','or','pa','ri','sc','sd',
                'tn','tx','ut','va','vt','wa','wi','wv','wy','dc'
            }
            row = {
                'site_name': leg.get('name', ''),
                'feature_name': leg.get('name', ''),
                'latitude': leg_lat,
                'longitude': leg_lon,
                'state': state,
                'component': leg.get('category', 'legacy').lower(),
                'installation_id': '',
                'is_firrma': 'Yes',
                'status': 'act',
                'conus': 'Y' if state in CONUS_STATES else 'N',
                'current_part': '2',
                'object_id': f'legacy_{len(added)}',
            }
            added.append(row)
        else:
            already_present += 1
    
    print(f"    Already in MIRTA FIRRMA: {already_present}")
    print(f"    New sites from legacy: {len(added)}")
    
    if added:
        # Categories of added sites
        cats = {}
        for r in added:
            cat = r['component']
            cats[cat] = cats.get(cat, 0) + 1
        print(f"    By category: {dict(cats)}")
        print(f"    Added sites:")
        for r in added:
            print(f"      {r['site_name']:<50} {r['state']} ({r['component']})")
    
    # Merge and rewrite
    merged = firrma_rows + added
    
    out_path = os.path.join(output_dir, 'cfius_appendix_a_geocoded.csv')
    fieldnames = list(firrma_rows[0].keys())
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged)
    
    conus = [r for r in merged if r['conus'] == 'Y']
    print(f"    Merged total: {len(merged)} ({len(conus)} CONUS)")
    print(f"    -> {out_path}")
    
    return merged


# ----------------------------------------------------------
# 2. AFIDA Excel -> afida_2024_holdings.csv
# ----------------------------------------------------------

def convert_afida(excel_path, output_dir):
    """
    Convert AFIDA 2024 holdings Excel to clean CSV.
    
    Handles:
    - Header row offset (actual headers on row 3 in Excel)
    - Trailing spaces in column names
    - FIPS zero-padding (1001 -> "01001")
    - Country name normalization
    - Secondary interest flags for adversarial nations
    """
    print("\n[2] Converting AFIDA Excel...")
    
    try:
        import openpyxl
    except ImportError:
        print("    ERROR: openpyxl required. Install: pip install openpyxl")
        return None
    
    wb = openpyxl.load_workbook(excel_path, read_only=True)
    ws = wb['AFIDA_YR2024_Holdings_Data']
    
    # Read all rows
    all_rows = list(ws.iter_rows(values_only=True))
    
    # Headers are on row index 2 (row 3 in Excel)
    raw_headers = list(all_rows[2])
    
    # Clean headers: strip whitespace, remove special chars, lowercase
    COLUMN_MAP = {
        'State': 'state',
        'County': 'county',
        'FIPS': 'fips',
        'Owner Name 1/': 'owner_name',
        'Owner Id': 'owner_id',
        'Parcel Id': 'parcel_id',
        'Country': 'country',
        'Country Code': 'country_code',
        'US Code': 'us_code',
        'Principal Place of Business': 'principal_place_of_business',
        'Number of Acres': 'acres',
        'Owner Type': 'owner_type',
        'Percent of Ownership': 'pct_ownership',
        'Acquisition Method': 'acquisition_method',
        'Purchase Price': 'purchase_price',
        'Estimated Value': 'estimated_value',
        'Current Value': 'current_value',
        'Acquisition Month': 'acquisition_month',
        'Acquisition Year': 'acquisition_year',
        'Citizenship': 'citizenship',
        'Secondary Interest in China': 'secondary_china',
        'Secondary Interest in Iran': 'secondary_iran',
        'Secondary Interest in Russia': 'secondary_russia',
        'Secondary Interest in North Korea': 'secondary_nk',
    }
    
    # Map raw headers to clean names
    clean_headers = []
    raw_to_clean = {}
    for i, h in enumerate(raw_headers):
        h_stripped = str(h).strip() if h else None
        if h_stripped in COLUMN_MAP:
            clean = COLUMN_MAP[h_stripped]
            clean_headers.append(clean)
            raw_to_clean[i] = clean
        elif h_stripped:
            # Keep other columns with normalized names
            clean = h_stripped.lower().replace(' ', '_').replace('/', '').strip('_')
            # Handle duplicate column names
            if clean in clean_headers:
                clean = f"{clean}_{i}"
            clean_headers.append(clean)
            raw_to_clean[i] = clean
        else:
            clean_headers.append(f"col_{i}")
            raw_to_clean[i] = f"col_{i}"
    
    # Process data rows (starting from row index 3)
    data_rows = all_rows[3:]
    
    # Country codes for adversarial nations
    ADVERSARIAL_COUNTRIES = {
        'CHINA', "CHINA, PEOPLE'S REPUBLIC OF", 'CHINA (MAINLAND)',
        'RUSSIA', 'RUSSIAN FEDERATION',
        'IRAN', 'IRAN, ISLAMIC REPUBLIC OF',
        'NORTH KOREA', "KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF",
    }
    
    COUNTRY_NORMALIZE = {
        "CHINA, PEOPLE'S REPUBLIC OF": "CHINA",
        "CHINA (MAINLAND)": "CHINA",
        "RUSSIAN FEDERATION": "RUSSIA",
        "IRAN, ISLAMIC REPUBLIC OF": "IRAN",
        "KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF": "NORTH KOREA",
    }
    
    processed = []
    for row in data_rows:
        if row[0] is None:  # Skip blank rows
            continue
        
        record = {}
        for i, val in enumerate(row):
            if i in raw_to_clean:
                record[raw_to_clean[i]] = val
        
        # Zero-pad FIPS to 5 digits
        if 'fips' in record and record['fips'] is not None:
            record['fips'] = str(int(record['fips'])).zfill(5)
        
        # Normalize country name
        country = str(record.get('country', '')).upper().strip()
        record['country_normalized'] = COUNTRY_NORMALIZE.get(country, country)
        
        # Flag adversarial nation (primary or secondary)
        is_adversarial = country in ADVERSARIAL_COUNTRIES or \
                         record['country_normalized'] in ADVERSARIAL_COUNTRIES
        has_secondary = any([
            record.get('secondary_china'),
            record.get('secondary_iran'),
            record.get('secondary_russia'),
            record.get('secondary_nk'),
        ])
        record['is_adversarial'] = 'Y' if (is_adversarial or has_secondary) else 'N'
        record['is_china'] = 'Y' if (record['country_normalized'] == 'CHINA' or 
                                      record.get('secondary_china')) else 'N'
        
        # Ensure acres is numeric
        try:
            record['acres'] = float(record.get('acres', 0) or 0)
        except (ValueError, TypeError):
            record['acres'] = 0.0
        
        processed.append(record)
    
    # Define output columns (ordered)
    output_columns = [
        'fips', 'state', 'county', 'owner_name', 'owner_id', 'parcel_id',
        'country', 'country_normalized', 'country_code', 'acres',
        'acquisition_year', 'acquisition_month', 'owner_type',
        'pct_ownership', 'purchase_price', 'current_value',
        'citizenship', 'secondary_china', 'secondary_iran',
        'secondary_russia', 'secondary_nk',
        'is_adversarial', 'is_china',
    ]
    
    # Filter to columns that exist
    output_columns = [c for c in output_columns if any(c in r for r in processed[:1])]
    
    out_path = os.path.join(output_dir, 'afida_2024_holdings.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(processed)
    
    # Summary stats
    total = len(processed)
    china = sum(1 for r in processed if r.get('is_china') == 'Y')
    adversarial = sum(1 for r in processed if r.get('is_adversarial') == 'Y')
    countries = len(set(r.get('country_normalized', '') for r in processed))
    unique_fips = len(set(r.get('fips', '') for r in processed if r.get('is_china') == 'Y'))
    
    print(f"    Total holdings: {total:,}")
    print(f"    Unique countries: {countries}")
    print(f"    China (primary + secondary): {china}")
    print(f"    All adversarial nations: {adversarial}")
    print(f"    China unique FIPS counties: {unique_fips}")
    print(f"    -> {out_path}")
    
    # -- China county summary --
    china_counties = {}
    for r in processed:
        if r.get('is_china') != 'Y':
            continue
        fips = r.get('fips', '')
        if fips not in china_counties:
            china_counties[fips] = {
                'fips': fips,
                'state': r.get('state', ''),
                'county': r.get('county', ''),
                'holdings': 0,
                'total_acres': 0.0,
                'entities': set(),
            }
        china_counties[fips]['holdings'] += 1
        china_counties[fips]['total_acres'] += r.get('acres', 0)
        china_counties[fips]['entities'].add(r.get('owner_name', ''))
    
    summary_rows = []
    for fips, info in sorted(china_counties.items()):
        summary_rows.append({
            'fips': info['fips'],
            'state': info['state'],
            'county': info['county'],
            'holdings': info['holdings'],
            'total_acres': round(info['total_acres'], 2),
            'unique_entities': len(info['entities']),
        })
    
    summary_path = os.path.join(output_dir, 'china_county_summary.csv')
    with open(summary_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['fips','state','county','holdings','total_acres','unique_entities'])
        writer.writeheader()
        writer.writerows(summary_rows)
    
    print(f"    China county summary: {len(summary_rows)} counties -> {summary_path}")
    
    return processed


# ----------------------------------------------------------
# 3. County Centroids from NOAA or Census
# ----------------------------------------------------------

def generate_centroids_stub(output_dir):
    """
    Generate instructions for obtaining county centroid data.
    
    The spatial analysis requires a FIPS -> (lat, lon) lookup table.
    This function creates a stub CSV and instructions for populating it.
    """
    print("\n[3] County centroids...")
    
    stub_path = os.path.join(output_dir, 'county_centroids.csv')
    
    instructions = """# County Centroids Lookup Table
# 
# This file must be populated with FIPS -> (latitude, longitude) mappings.
# 
# RECOMMENDED SOURCES (in order of preference):
#
# 1. NOAA c_16ap26 county centroids (used in prior AFIDA research)
#    Download from: https://www.weather.gov/gis/Counties
#    File: c_16ap26.zip -> extract shapefile -> compute centroids
#
# 2. Census Bureau TIGER/Line county centroids
#    Download from: https://www2.census.gov/geo/docs/reference/cenpop2020/county/
#    File: CenPop2020_Mean_CO.txt (tab-delimited, has STATEFP+COUNTYFP+LATITUDE+LONGITUDE)
#    This is the easiest option -- download and rename columns.
#
# 3. Census Gazetteer Files
#    Download from: https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html
#    File: 2024_Gaz_counties_national.txt
#
# REQUIRED COLUMNS: fips (5-digit zero-padded), latitude, longitude
# EXAMPLE:
# fips,latitude,longitude
# 01001,32.5353,-86.6439
# 01003,30.6599,-87.7460
"""
    
    with open(stub_path, 'w', encoding='utf-8') as f:
        f.write(instructions)
    
    # Try to auto-generate from Census CenPop if available
    # For now, generate a download script
    download_script = os.path.join(output_dir, 'download_centroids.sh')
    with open(download_script, 'w', encoding='utf-8') as f:
        f.write("""#!/bin/bash
# Download Census 2020 county population centroids
# This is the simplest source for FIPS -> (lat, lon) mapping

URL="https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt"
OUTPUT="county_centroids_raw.txt"

echo "Downloading Census 2020 county centroids..."
curl -o "$OUTPUT" "$URL"

# Convert to the CSV format spatial_analysis_primary.py expects
python3 -c "
import csv
with open('$OUTPUT') as f:
    reader = csv.DictReader(f, delimiter=',')
    rows = []
    for row in reader:
        fips = row.get('STATEFP','').zfill(2) + row.get('COUNTYFP','').zfill(3)
        lat = row.get('LATITUDE','')
        lon = row.get('LONGITUDE','')
        if fips and lat and lon:
            rows.append({'fips': fips, 'latitude': lat, 'longitude': lon,
                         'state': row.get('STNAME',''), 'county': row.get('COUNAME','')})
    
    with open('county_centroids.csv', 'w', newline='') as out:
        w = csv.DictWriter(out, fieldnames=['fips','latitude','longitude','state','county'])
        w.writeheader()
        w.writerows(rows)
    print(f'  Written {len(rows)} county centroids to county_centroids.csv')
"
""")
    os.chmod(download_script, 0o755)
    
    print(f"    Stub -> {stub_path}")
    print(f"    Download script -> {download_script}")
    print(f"    Run: bash {download_script} to auto-generate centroids")


# ----------------------------------------------------------
# 4. Validation
# ----------------------------------------------------------

def validate_outputs(output_dir):
    """Check that all required output files exist and are well-formed."""
    print("\n[4] Validating outputs...")
    
    checks = {
        'cfius_appendix_a_geocoded.csv': ['site_name', 'latitude', 'longitude', 'conus', 'current_part'],
        'afida_2024_holdings.csv': ['fips', 'country_normalized', 'acres', 'is_china'],
        'china_county_summary.csv': ['fips', 'total_acres', 'unique_entities'],
    }
    
    all_pass = True
    for filename, required_cols in checks.items():
        filepath = os.path.join(output_dir, filename)
        if not os.path.exists(filepath):
            print(f"    [X] MISSING: {filename}")
            all_pass = False
            continue
        
        with open(filepath) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)
        
        missing = [c for c in required_cols if c not in headers]
        if missing:
            print(f"    [X] {filename}: missing columns {missing}")
            all_pass = False
        else:
            print(f"    [OK] {filename}: {len(rows)} rows, all required columns present")
    
    if all_pass:
        print("\n    All validations passed.")
    else:
        print("\n    [!] Some validations failed. Check output above.")
    
    return all_pass


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='MTS Research Programme -- Data Preparation Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python data_prep.py \\
      --mirta mirta-dod-sites-points-geojson.geojson \\
      --afida AFIDACurrentHoldingsYR2024.xlsx \\
      --output ./processed

  Then run the spatial analysis:
  python spatial_analysis_primary.py \\
      --appendix-a ./processed/cfius_appendix_a_geocoded.csv \\
      --afida ./processed/afida_2024_holdings.csv \\
      --centroids ./processed/county_centroids.csv \\
      --legacy installations_71.csv \\
      --output ./results
        """
    )
    
    parser.add_argument('--mirta', required=True,
                        help='Path to MIRTA DoD Sites Points GeoJSON')
    parser.add_argument('--afida', required=True,
                        help='Path to AFIDA 2024 holdings Excel (.xlsx)')
    parser.add_argument('--legacy', default=None,
                        help='Path to legacy installations CSV (71 sites, optional)')
    parser.add_argument('--output', default='./processed',
                        help='Output directory (default: ./processed)')
    
    args = parser.parse_args()
    
    os.makedirs(args.output, exist_ok=True)
    
    print("=" * 60)
    print("  MTS Data Preparation Pipeline")
    print("=" * 60)
    
    firrma_rows = convert_mirta(args.mirta, args.output)
    if args.legacy:
        merge_legacy(firrma_rows, args.legacy, args.output)
    convert_afida(args.afida, args.output)
    generate_centroids_stub(args.output)
    validate_outputs(args.output)
    
    print("\n" + "=" * 60)
    print("  NEXT STEPS")
    print("=" * 60)
    print("""
  1. Download county centroids:
     cd processed && bash download_centroids.sh

  2. (Optional) Add Part 1/Part 2 classification:
     Edit cfius_appendix_a_geocoded.csv -> update 'current_part'
     column based on Federal Register Appendix A text.

  3. (Optional) Add non-DoD Appendix A sites:
     DOE sites (Los Alamos, Sandia, Pantex, Y-12, etc.) and
     intelligence sites are NOT in MIRTA. Add rows manually
     with geocoded coordinates.

  4. Run spatial analysis:
     python spatial_analysis_primary.py \\
         --appendix-a processed/cfius_appendix_a_geocoded.csv \\
         --afida processed/afida_2024_holdings.csv \\
         --centroids processed/county_centroids.csv \\
         --legacy installations_71.csv \\
         --output results/
    """)


if __name__ == '__main__':
    main()
