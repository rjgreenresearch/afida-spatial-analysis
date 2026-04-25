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
        # 'tbd' = site is on Appendix A but classification is pending.
        # Treat as 'Yes' (included) — consistent with original MIRTA pipeline.
        # These sites ARE listed in the Federal Register Appendix A text;
        # the 'tbd' flag reflects MIRTA metadata lag, not genuine exclusion.
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

    # SAFETY: if an existing geocoded file already has Part 1/2/3 classification
    # (i.e., current_part has values other than just '2' and 'none'), preserve it.
    # Re-running data_prep.py with a different MIRTA source would otherwise lose
    # manually curated or merge_part_classification()-enriched part assignments.
    if os.path.exists(out_path_firrma):
        import csv as _csv
        with open(out_path_firrma, encoding='latin-1') as _f:
            _existing = list(_csv.DictReader(_f))
        _existing_parts = set(r.get('current_part','') for r in _existing
                              if r.get('conus','').upper() == 'Y')
        _existing_conus = sum(1 for r in _existing if r.get('conus','').upper()=='Y')
        if _existing_conus >= len(firrma_conus) and '1' in _existing_parts:
            print(f"    SAFETY: existing cfius_appendix_a_geocoded.csv has "
                  f"{_existing_conus} CONUS sites with Part 1/2/3 classification.")
            print(f"    Keeping existing file. Pass --force-mirta to regenerate.")
            return firrma_rows

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
# 1c. Merge Part 1/2/3 classification into geocoded file
# ----------------------------------------------------------

# Explicit name mappings for MIRTA abbreviations not caught by fuzzy matching
MIRTA_TO_ECFR = {
    # MIRTA name (lower)                : eCFR name (exact)
    'aberdeen pg':                        'Aberdeen Proving Ground',
    'army research lab - orlando simulations and training technology center':
                                          'Army Research Lab - Orlando',
    'army research lab - raleigh durham': 'Army Research Lab - Raleigh Durham Research Triangle Park',
    'avon park afr':                      'Avon Park Air Force Range',
    'bangor wa':                          'Naval Base Kitsap - Bangor',
    'bath iron works':                    'Bath Iron Works',
    'boeing st. louis':                   'Boeing St. Louis',
    'camp pendleton':                     'Marine Corps Base Camp Pendleton',
    'dugway pg':                          'Dugway Proving Ground',
    'ellsworth afb site 2':               'Ellsworth Air Force Base',
    'iowa ng jfhq':                       'Iowa National Guard Joint Force Headquarters',
    'keyport nuwc':                       'Naval Undersea Warfare Center Division Keyport',
    'lockheed martin marietta':           'Lockheed Martin Aeronautics Marietta',
    'luke air force auxiliary field no 1':'Luke Air Force Base',
    'luke waste annex':                   'Luke Air Force Base',
    'nas corpus christi':                 'Naval Air Station Corpus Christi',
    'nas jacksonville':                   'Naval Air Station Jacksonville',
    'nas kingsville':                     'Naval Air Station Kingsville',
    'naval base ventura county- port hueneme operating facility':
                                          'Naval Base Ventura County',
    'naval surface warfare center carderock division Ã¢â¬â acoustic research detachment':
                                          'Naval Surface Warfare Center Carderock Division',
    'ng camp dodge johnston ts':          'Iowa National Guard Joint Force Headquarters',
    'u.s. army natick soldier systems center': 'Army Natick Soldier Systems Center',
    # Additional mappings confirmed from classification file lookup
    'bangor wa':                          'Naval Base Kitsap Bangor',
    'keyport nuwc':                       'Naval Base Kitsap - Keyport',
    'naval base ventura county- port hueneme operating facility':
                                          'Naval Base Ventura County - Port Hueneme',
    'naval surface warfare center carderock division Ã¢â¬â acoustic research detachment':
                                          'Naval Surface Warfare Center Carderock Division - ARD',
    'ng camp dodge johnston ts':          'Camp Dodge',
    'white sands mr':                     'White Sands Missile Range',
}

# DOE/NNSA nuclear weapons complex sites: Part 2 (100-mile threshold)
# Source: 31 CFR Part 802 Appendix A -- these sites appear in Federal Register
# text but are absent from the MIRTA database used to build the geocoded CSV.
DOE_SITES_PART2 = {
    'hanford site',
    'idaho nl',
    'idaho national laboratory',
    'los alamos nl',
    'los alamos national laboratory',
    'pantex plant',
    'savannah river site',
    'y-12 nsc',
    'y-12 national security complex',
}

# Sites in MIRTA geocoded file but NOT in appendix_a_part_classification.csv.
# Part assigned from Federal Register text review.
# Part 1 = 1-mile threshold; Part 2 = 100-mile threshold
MIRTA_ONLY_PARTS = {
    # Part 2: submarine / strategic nuclear-adjacent
    'bangor wa':                 2,   # Naval Base Kitsap Bangor (SSBN homeport)
    # Part 1: all others
    'army research lab - raleigh durham': 1,
    'avon park afr':             1,
    'bath iron works':           1,
    'boeing st. louis':          1,
    'dugway pg':                 1,
    'iowa ng jfhq':              1,
    'lockheed martin marietta':  1,
    'nas jacksonville':          1,
    'nas kingsville':            1,
    'u.s. army natick soldier systems center': 1,
}


def merge_part_classification(geocoded_path, classification_path, output_path=None):
    """
    Merge Part 1/2/3 classification from appendix_a_part_classification.csv
    into cfius_appendix_a_geocoded.csv.

    Three-tier matching strategy:
      Tier 1 -- Explicit MIRTA_TO_ECFR name map (highest confidence)
      Tier 2 -- Fuzzy name match (SequenceMatcher >= 0.80 on normalised names)
      Tier 3 -- Coordinate proximity (<= 5 miles)

    Unmatched sites default to Part 1 (1-mile threshold) -- most conservative.
    Known DOE sites not in classification file are assigned Part 2 (100 miles).

    Updates current_part column in geocoded file.
    Writes to output_path (or overwrites geocoded_path if output_path is None).

    Returns (updated_df, match_report_df).
    """
    import re
    import csv
    import math
    from difflib import SequenceMatcher

    def _norm(s):
        """Normalise site name for fuzzy comparison."""
        s = s.lower().strip()
        s = re.sub(r"\bair force base\b", "afb", s)
        s = re.sub(r"\bnaval air station\b", "nas", s)
        s = re.sub(r"\bmarine corps base\b", "mcb", s)
        s = re.sub(r"\bfort\b", "fort", s)
        s = re.sub(r"\([^)]*\)", "", s)   # remove parentheticals
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _hav(lat1, lon1, lat2, lon2):
        R = 3958.8
        lat1,lon1,lat2,lon2 = map(math.radians, [lat1,lon1,lat2,lon2])
        dlat=lat2-lat1; dlon=lon2-lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
        return 2*R*math.asin(math.sqrt(a))

    def _fuzzy(a, b):
        return SequenceMatcher(None, a, b).ratio()

    # Load files
    geocoded = []
    with open(geocoded_path, encoding='latin-1') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            geocoded.append(dict(row))

    classif = []
    with open(classification_path, encoding='latin-1') as f:
        reader = csv.DictReader(f)
        for row in reader:
            classif.append(dict(row))

    # Pre-compute normalised names and coords for classification rows
    c_norm  = [_norm(c['ecfr_name']) for c in classif]
    c_lats  = [float(c['latitude'])  if c.get('latitude')  else None for c in classif]
    c_lons  = [float(c['longitude']) if c.get('longitude') else None for c in classif]

    match_report = []
    n_matched   = 0
    n_unmatched = 0

    for row in geocoded:
        if row.get('conus', '').upper() != 'Y':
            continue   # only CONUS sites need reclassification

        site = row['site_name']
        site_l = site.lower().strip()

        # Tier 0a: Sites in MIRTA but absent from classification file (hardcoded)
        if site_l in MIRTA_ONLY_PARTS:
            row['current_part'] = str(MIRTA_ONLY_PARTS[site_l])
            match_report.append({
                'site_name': site, 'matched_to': f"hardcoded Part {MIRTA_ONLY_PARTS[site_l]}",
                'part': MIRTA_ONLY_PARTS[site_l], 'method': 'mirta_only_hardcode'})
            n_matched += 1
            continue

        # Tier 0b: Known DOE sites not in classification file
        if site_l in DOE_SITES_PART2:
            row['current_part'] = '2'
            match_report.append({
                'site_name': site, 'matched_to': 'DOE Part 2 (hardcoded)',
                'part': 2, 'method': 'doe_hardcode'})
            n_matched += 1
            continue

        # Tier 1: explicit MIRTA→eCFR map
        matched_row = None
        method = None
        if site_l in MIRTA_TO_ECFR:
            ecfr_target = MIRTA_TO_ECFR[site_l]
            for i, c in enumerate(classif):
                if c['ecfr_name'].lower().strip() == ecfr_target.lower().strip():
                    matched_row = c
                    method = 'explicit_map'
                    break

        # Tier 2: fuzzy name match
        if matched_row is None:
            site_n = _norm(site)
            best_score, best_idx = 0.0, None
            for i, cn in enumerate(c_norm):
                s = _fuzzy(site_n, cn)
                if s > best_score:
                    best_score, best_idx = s, i
            if best_score >= 0.80:
                matched_row = classif[best_idx]
                method = f'fuzzy({best_score:.2f})'

        # Tier 3: coordinate proximity <= 5 miles
        if matched_row is None:
            try:
                g_lat = float(row['latitude'])
                g_lon = float(row['longitude'])
                best_dist, best_idx = float('inf'), None
                for i, (clat, clon) in enumerate(zip(c_lats, c_lons)):
                    if clat is not None and clon is not None:
                        d = _hav(g_lat, g_lon, clat, clon)
                        if d < best_dist:
                            best_dist, best_idx = d, i
                if best_dist <= 5.0:
                    matched_row = classif[best_idx]
                    method = f'coord({best_dist:.1f}mi)'
            except (ValueError, TypeError):
                pass

        if matched_row is not None:
            row['current_part'] = str(int(float(matched_row['part'])))
            if 'threshold_miles' not in row or not row['threshold_miles']:
                row['threshold_miles'] = str(matched_row['threshold_miles'])
            match_report.append({
                'site_name': site,
                'matched_to': matched_row['ecfr_name'],
                'part': int(float(matched_row['part'])),
                'method': method,
            })
            n_matched += 1
        else:
            # Default to Part 1 (most conservative)
            row['current_part'] = '1'
            match_report.append({
                'site_name': site, 'matched_to': None,
                'part': 1, 'method': 'default_part1'})
            n_unmatched += 1

    # Add threshold_miles to fieldnames if not present
    if 'threshold_miles' not in fieldnames:
        fieldnames = list(fieldnames) + ['threshold_miles']

    out = output_path or geocoded_path
    with open(out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(geocoded)

    print(f"  Part classification merged: {n_matched} matched, "
          f"{n_unmatched} defaulted to Part 1")
    print(f"  -> {out}")

    # Summary of part distribution
    part_counts = {1: 0, 2: 0, 3: 0}
    for row in geocoded:
        if row.get('conus', '').upper() == 'Y':
            try:
                part_counts[int(row['current_part'])] += 1
            except (ValueError, KeyError):
                pass
    print(f"  CONUS Part distribution: Part 1: {part_counts[1]}, "
          f"Part 2: {part_counts[2]}, Part 3: {part_counts[3]}")

    return geocoded, match_report



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

    # SAFETY: never overwrite an existing file that already has real data.
    # Re-running data_prep.py must not destroy a populated centroids file.
    if os.path.exists(stub_path):
        with open(stub_path, encoding='utf-8', errors='ignore') as _f:
            real_lines = [l for l in _f if l.strip() and not l.strip().startswith('#')]
        if real_lines:
            print(f"    county_centroids.csv already populated ({len(real_lines):,} data rows) — skipping stub generation.")
            return
    
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
    parser.add_argument('--force-mirta', action='store_true', dest='force_mirta',
                        help='Regenerate cfius_appendix_a_geocoded.csv even if '
                             'a classified version already exists (use carefully)')
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
    
    # Merge Part 1/2/3 classification into geocoded file
    geocoded_path = os.path.join(args.output, 'cfius_appendix_a_geocoded.csv')
    classif_path  = os.path.join(args.output, 'appendix_a_part_classification.csv')
    if os.path.exists(classif_path):
        print("\n[5] Merging Part 1/2/3 classification...")
        merge_part_classification(geocoded_path, classif_path)
    else:
        print("\n[5] appendix_a_part_classification.csv not found in output dir.")
        print("    Copy it there and re-run data_prep.py, or merge manually.")
        print("    Expected path:", classif_path)
    
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
