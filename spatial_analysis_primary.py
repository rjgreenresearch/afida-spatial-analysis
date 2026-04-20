#!/usr/bin/env python3
"""
===============================================================================
Spatial Clustering Analysis -- Dual-Database Framework
===============================================================================

Article:  "Spatial Clustering of Foreign Agricultural Acquisitions Near U.S.
           Military Installations: Comparative Evidence from USDA Primary Data"

Author:   Robert J. Green
Target:   Spatial Economic Analysis (Taylor & Francis)

Purpose:  Run Monte Carlo permutation testing against TWO installation databases:
          1. PRIMARY: 230 CFIUS Appendix A installations (192 CONUS)
             Source: Federal Register, geocoded via MIRTA DoD boundaries
          2. ROBUSTNESS: 71 hand-selected national security installations
             Source: Author-constructed from AFIDA analysis

The dual-database approach harmonizes the spatial analysis (Paper 1) with
the regulatory analysis (Paper 3), ensuring all papers in the research
programme use the same geographic reference frame.

Inputs:
  --afida       Path to AFIDA detailed holdings CSV (2024, or multi-year)
  --appendix-a  Path to cfius_appendix_a_geocoded.csv (230 sites)
  --legacy      Path to installations_71.csv (71 sites)
  --output      Output directory for results

Outputs:
  primary_china_multithreshold.csv     -- Table 2 (primary, Appendix A)
  comparative_50mi.csv                 -- Table 3 (multi-country, Appendix A)
  installation_subset_sensitivity.csv  -- Table 4 (subset analysis)
  database_comparison.csv              -- Table 4b (71 vs 230 robustness)
  panel_china_2022_2024.csv            -- Table 5 (temporal)
  ownership_concentration.csv          -- Table 6 (HHI / top entities)
  part3_icbm_analysis.csv              -- Table 7 (Part 3 missile field counties)
  part3_adversarial_detail.csv         -- Table 7b (adversarial holdings detail)

===============================================================================
"""

import numpy as np
import pandas as pd
from math import radians, sin, cos, sqrt, asin
import argparse
import os
import sys
import csv
from collections import Counter

# =============================================================================
# CONFIGURATION
# =============================================================================

RANDOM_SEED = 20260322       # Paper submission date
MC_PRIMARY = 10_000          # Iterations for primary China analysis
MC_COMPARATIVE = 5_000       # Iterations for multi-country battery
DISTANCE_THRESHOLDS = [10, 25, 50, 100]  # Miles
PRIMARY_THRESHOLD = 50       # Main reporting threshold
CONUS_LAT = (25.0, 49.0)
CONUS_LON = (-125.0, -66.0)
AG_LAT = (28.0, 48.0)       # Agricultural heartland restriction
AG_LON = (-120.0, -75.0)

# Countries for comparative analysis
COUNTRIES = {
    'CHINA':       {'label': 'China', 'type': 'adversarial'},
    'CANADA':      {'label': 'Canada', 'type': 'allied'},
    'UNITED KINGDOM': {'label': 'United Kingdom', 'type': 'allied'},
    'NETHERLANDS': {'label': 'Netherlands', 'type': 'allied'},
    'GERMANY':     {'label': 'Germany', 'type': 'allied'},
    'ITALY':       {'label': 'Italy', 'type': 'allied'},
    'JAPAN':       {'label': 'Japan', 'type': 'comparator'},
    'MEXICO':      {'label': 'Mexico', 'type': 'comparator'},
    'BRAZIL':      {'label': 'Brazil', 'type': 'comparator'},
    'IRAN':        {'label': 'Iran', 'type': 'adversarial'},
    'SAUDI ARABIA': {'label': 'Saudi Arabia', 'type': 'adversarial-adjacent'},
}

np.random.seed(RANDOM_SEED)

# Part 3 ICBM Missile Field Counties (31 CFR Part 802, Appendix A)
# These use county/township-range jurisdiction, NOT distance thresholds.
# Source: eCFR current as of January 2026 (85 FR 3166, as amended)
PART3_COUNTIES = {
    # 90th Missile Wing (F.E. Warren AFB) -- CO, NE, WY
    '08075': ('Logan', 'CO', '90th MW (Warren)'),
    '08087': ('Morgan', 'CO', '90th MW (Warren)'),
    '08115': ('Sedgwick', 'CO', '90th MW (Warren)'),
    '08121': ('Washington', 'CO', '90th MW (Warren)'),
    '08123': ('Weld', 'CO', '90th MW (Warren)'),
    '31007': ('Banner', 'NE', '90th MW (Warren)'),
    '31033': ('Cheyenne', 'NE', '90th MW (Warren)'),
    '31049': ('Deuel', 'NE', '90th MW (Warren)'),
    '31069': ('Garden', 'NE', '90th MW (Warren)'),
    '31105': ('Kimball', 'NE', '90th MW (Warren)'),
    '31123': ('Morrill', 'NE', '90th MW (Warren)'),
    '31157': ('Scotts Bluff', 'NE', '90th MW (Warren)'),
    '31165': ('Sioux', 'NE', '90th MW (Warren)'),
    '56015': ('Goshen', 'WY', '90th MW (Warren)'),
    '56021': ('Laramie', 'WY', '90th MW (Warren)'),
    '56031': ('Platte', 'WY', '90th MW (Warren)'),
    # 341st Missile Wing (Malmstrom AFB) -- MT
    '30005': ('Blaine', 'MT', '341st MW (Malmstrom)'),
    '30013': ('Cascade', 'MT', '341st MW (Malmstrom)'),
    '30015': ('Chouteau', 'MT', '341st MW (Malmstrom)'),
    '30027': ('Fergus', 'MT', '341st MW (Malmstrom)'),
    '30035': ('Glacier', 'MT', '341st MW (Malmstrom)'),
    '30037': ('Golden Valley', 'MT', '341st MW (Malmstrom)'),
    '30045': ('Judith Basin', 'MT', '341st MW (Malmstrom)'),
    '30049': ('Lewis and Clark', 'MT', '341st MW (Malmstrom)'),
    '30051': ('Liberty', 'MT', '341st MW (Malmstrom)'),
    '30059': ('Meagher', 'MT', '341st MW (Malmstrom)'),
    '30065': ('Musselshell', 'MT', '341st MW (Malmstrom)'),
    '30069': ('Petroleum', 'MT', '341st MW (Malmstrom)'),
    '30071': ('Phillips', 'MT', '341st MW (Malmstrom)'),
    '30073': ('Pondera', 'MT', '341st MW (Malmstrom)'),
    '30095': ('Stillwater', 'MT', '341st MW (Malmstrom)'),
    '30097': ('Sweet Grass', 'MT', '341st MW (Malmstrom)'),
    '30099': ('Teton', 'MT', '341st MW (Malmstrom)'),
    '30101': ('Toole', 'MT', '341st MW (Malmstrom)'),
    '30107': ('Wheatland', 'MT', '341st MW (Malmstrom)'),
    # 91st Missile Wing (Minot AFB) -- ND
    '38009': ('Bottineau', 'ND', '91st MW (Minot)'),
    '38013': ('Burke', 'ND', '91st MW (Minot)'),
    '38025': ('Dunn', 'ND', '91st MW (Minot)'),
    '38049': ('McHenry', 'ND', '91st MW (Minot)'),
    '38053': ('McKenzie', 'ND', '91st MW (Minot)'),
    '38055': ('McLean', 'ND', '91st MW (Minot)'),
    '38057': ('Mercer', 'ND', '91st MW (Minot)'),
    '38061': ('Mountrail', 'ND', '91st MW (Minot)'),
    '38069': ('Pierce', 'ND', '91st MW (Minot)'),
    '38075': ('Renville', 'ND', '91st MW (Minot)'),
    '38083': ('Sheridan', 'ND', '91st MW (Minot)'),
    '38101': ('Ward', 'ND', '91st MW (Minot)'),
    '38105': ('Williams', 'ND', '91st MW (Minot)'),
}


# =============================================================================
# HAVERSINE DISTANCE
# =============================================================================

def haversine(lat1, lon1, lat2, lon2):
    """Great-circle distance in miles using WGS-84 mean Earth radius."""
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    return 2 * R * asin(sqrt(a))


def min_distances(holdings_df, installations_df):
    """Compute minimum distance from each holding to nearest installation."""
    dists = []
    for _, h in holdings_df.iterrows():
        min_d = float('inf')
        for _, inst in installations_df.iterrows():
            d = haversine(h['latitude'], h['longitude'],
                          inst['latitude'], inst['longitude'])
            if d < min_d:
                min_d = d
        dists.append(min_d)
    return np.array(dists)


# =============================================================================
# MONTE CARLO PERMUTATION TEST
# =============================================================================

def monte_carlo_test(n_holdings, installations_df, threshold, n_iter,
                     lat_range=CONUS_LAT, lon_range=CONUS_LON):
    """
    Run Monte Carlo permutation test.
    
    Returns:
        dict with observed_placeholder (caller fills), expected, std, max, p_value
    """
    inst_lats = installations_df['latitude'].values
    inst_lons = installations_df['longitude'].values
    n_inst = len(inst_lats)
    
    mc_counts = np.zeros(n_iter)
    
    for i in range(n_iter):
        rand_lats = np.random.uniform(lat_range[0], lat_range[1], n_holdings)
        rand_lons = np.random.uniform(lon_range[0], lon_range[1], n_holdings)
        
        count = 0
        for k in range(n_holdings):
            for j in range(n_inst):
                d = haversine(rand_lats[k], rand_lons[k],
                              inst_lats[j], inst_lons[j])
                if d <= threshold:
                    count += 1
                    break
        mc_counts[i] = count
    
    return {
        'expected': mc_counts.mean(),
        'std': mc_counts.std(),
        'max': mc_counts.max(),
        'mc_counts': mc_counts
    }


def compute_enrichment(observed, mc_result, n_iter):
    """Compute enrichment ratio and empirical p-value."""
    p_value = (np.sum(mc_result['mc_counts'] >= observed) + 1) / (n_iter + 1)
    enrichment = observed / mc_result['expected'] if mc_result['expected'] > 0 else float('inf')
    return {
        'enrichment': enrichment,
        'p_value': p_value,
        'observed': observed,
        'expected': mc_result['expected'],
        'std': mc_result['std'],
        'max_under_h0': mc_result['max'],
    }


# =============================================================================
# DATA LOADING
# =============================================================================

def load_appendix_a(filepath):
    """Load Appendix A database, filter to CONUS."""
    df = pd.read_csv(filepath, encoding='latin-1')
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    # Filter to CONUS
    conus = df[df['conus'].astype(str).str.upper() == 'Y'].copy()
    print(f"  Appendix A: {len(df)} total, {len(conus)} CONUS")
    
    # Define Part 2 subset (100mi threshold)
    # Handle variations: '2', 2, 'Part 2', 'part2', etc.
    if 'current_part' in conus.columns:
        part_col = conus['current_part'].astype(str).str.strip()
        part2 = conus[part_col.isin(['2', 'Part 2', 'part2', 'Part2'])].copy()
    elif 'part' in conus.columns:
        part_col = conus['part'].astype(str).str.strip()
        part2 = conus[part_col.isin(['2', 'Part 2', 'part2', 'Part2'])].copy()
    else:
        print("  WARNING: No part classification column found. Using all CONUS as Part 2.")
        part2 = conus.copy()
    
    print(f"  Part 2 (100mi threshold): {len(part2)}")
    
    # If Part 2 is 0 but we have sites, all sites may have same part value
    if len(part2) == 0 and len(conus) > 0:
        unique_parts = conus['current_part'].astype(str).unique() if 'current_part' in conus.columns else ['?']
        print(f"  WARNING: Part 2 filter returned 0 sites.")
        print(f"  Unique current_part values: {list(unique_parts)[:10]}")
        print(f"  Using all CONUS sites for distance analysis (Part 1/2 distinction")
        print(f"  only affects the regulatory perimeter paper, not spatial clustering).")
        part2 = conus.copy()
    
    return conus, part2


def load_legacy_installations(filepath):
    """Load 71-site legacy database."""
    df = pd.read_csv(filepath, encoding='latin-1')
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    print(f"  Legacy installations: {len(df)}")
    return df


def load_afida_holdings(filepath, country_filter=None):
    """
    Load AFIDA detailed holdings, aggregate to county level.
    
    Expects columns: FIPS, country (or similar), acreage
    User must adapt column names to match their AFIDA spreadsheet format.
    """
    df = pd.read_csv(filepath, dtype={'fips': str}, encoding='latin-1')
    
    # Normalize column names — match first exact-ish hit per target only
    col_map = {}
    mapped_targets = set()
    # Priority order: exact matches first, then fuzzy
    target_patterns = [
        ('fips',        lambda c: c == 'fips'),
        ('country',     lambda c: c == 'country' or c == 'country_normalized'),
        ('acreage',     lambda c: c in ('acres', 'acreage', 'number of acres', 'number_of_acres')),
        ('entity_name', lambda c: c in ('owner_name', 'entity_name', 'owner name 1/')),
    ]
    for target, matcher in target_patterns:
        for col in df.columns:
            if target not in mapped_targets and matcher(col.lower().strip()):
                col_map[col] = target
                mapped_targets.add(target)
                break
    df = df.rename(columns=col_map)
    
    if country_filter:
        mask = df['country'].str.upper().str.contains(country_filter.upper(), na=False)
        df = df[mask]
    
    # Aggregate to county level
    if 'fips' in df.columns:
        county = df.groupby('fips').agg(
            acreage=('acreage', 'sum'),
            n_entities=('entity_name', 'nunique') if 'entity_name' in df.columns else ('acreage', 'count')
        ).reset_index()
    else:
        county = df
    
    return county, df


def geocode_counties(county_df, centroid_filepath):
    """
    Match FIPS codes to county centroids.
    
    centroid_filepath: NOAA county centroid file or pre-matched CSV
    with columns: fips, latitude, longitude
    """
    centroids = pd.read_csv(centroid_filepath, dtype={'fips': str}, encoding='latin-1', comment='#')
    centroids['latitude'] = pd.to_numeric(centroids['latitude'], errors='coerce')
    centroids['longitude'] = pd.to_numeric(centroids['longitude'], errors='coerce')
    
    merged = county_df.merge(centroids[['fips', 'latitude', 'longitude']],
                              on='fips', how='inner')
    print(f"  Geocoded: {len(merged)} of {len(county_df)} counties")
    return merged


# =============================================================================
# NUCLEAR-CAPABLE SUBSET
# =============================================================================

def get_nuclear_subset(appendix_a_df, legacy_df):
    """
    Build nuclear-capable installation subset from both databases.
    
    Uses Appendix A as primary, supplements with legacy DOE sites
    that are not on Appendix A.
    """
    # Known nuclear-capable installation name fragments
    nuclear_keywords = [
        'minot', 'malmstrom', 'warren', 'whiteman', 'barksdale',
        'dyess', 'ellsworth',  # Bomber bases
        'kings bay', 'kitsap', 'bangor',  # Submarine bases
        # DOE weapons complex
        'los alamos', 'sandia', 'pantex', 'y-12', 'savannah river',
        'idaho national', 'hanford', 'lawrence livermore', 'oak ridge'
    ]
    
    def is_nuclear(name):
        name_lower = name.lower()
        return any(kw in name_lower for kw in nuclear_keywords)
    
    # From Appendix A
    nuc_aa = appendix_a_df[appendix_a_df['site_name'].apply(is_nuclear)]
    
    # From legacy (DOE sites not on Appendix A)
    nuc_legacy = legacy_df[
        (legacy_df['category'] == 'DOE') | 
        (legacy_df['subcategory'].isin(['ICBM', 'Bomber', 'SSBN', 'Nuclear']))
    ]
    
    # Combine, deduplicate by proximity (within 5 miles = same site)
    combined_rows = []
    for _, r in nuc_aa.iterrows():
        combined_rows.append({
            'name': r['site_name'],
            'latitude': r['latitude'],
            'longitude': r['longitude'],
            'source': 'appendix_a'
        })
    
    for _, r in nuc_legacy.iterrows():
        # Check not already covered by Appendix A entry
        duplicate = False
        for cr in combined_rows:
            if haversine(float(r['latitude']), float(r['longitude']),
                        float(cr['latitude']), float(cr['longitude'])) < 5:
                duplicate = True
                break
        if not duplicate:
            combined_rows.append({
                'name': r['name'],
                'latitude': float(r['latitude']),
                'longitude': float(r['longitude']),
                'source': 'legacy_supplement'
            })
    
    result = pd.DataFrame(combined_rows)
    print(f"  Nuclear-capable subset: {len(result)} sites "
          f"({len(nuc_aa)} Appendix A + {len(result) - len(nuc_aa)} legacy supplement)")
    return result


# =============================================================================
# MAIN ANALYSIS PIPELINE
# =============================================================================

def run_full_analysis(afida_path, appendix_a_path, legacy_path,
                      centroid_path, output_dir):
    """
    Run complete dual-database spatial analysis.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 70)
    print("  SPATIAL CLUSTERING ANALYSIS -- DUAL-DATABASE FRAMEWORK")
    print("=" * 70)
    
    # -- Load installations --
    print("\n[1] Loading installation databases...")
    aa_conus, aa_part2 = load_appendix_a(appendix_a_path)
    legacy = load_legacy_installations(legacy_path)
    nuclear = get_nuclear_subset(aa_conus, legacy)
    
    installation_sets = {
        'Appendix A (192 CONUS)': aa_conus,
        'Appendix A Part 2 only': aa_part2,
        'Legacy 71-site': legacy,
        'Nuclear-capable': nuclear,
    }
    
    # -- Primary China Analysis (Table 2) --
    print("\n[2] Primary China multi-threshold analysis...")
    # NOTE: User must provide actual AFIDA data path and centroid path
    # Placeholder structure for the analysis:
    
    print("""
    +==================================================================+
    |  TO RUN THIS ANALYSIS:                                         |
    |                                                                |
    |  1. Download AFIDA 2024 detailed holdings from:                |
    |     fsa.usda.gov/resources/economic-policy-analysis/afida      |
    |                                                                |
    |  2. Download NOAA county centroids from:                       |
    |     weather.gov/gis/Counties                                   |
    |                                                                |
    |  3. Prepare a county centroid CSV with columns:                |
    |     fips, latitude, longitude                                  |
    |                                                                |
    |  4. Run:                                                       |
    |     python spatial_analysis_primary.py \\                       |
    |       --afida afida_2024_detailed.csv \\                        |
    |       --appendix-a cfius_appendix_a_geocoded.csv \\             |
    |       --legacy installations_71.csv \\                          |
    |       --centroids county_centroids.csv \\                       |
    |       --output results/                                        |
    +==================================================================+
    """)
    
    # -- Structure for results output --
    
    # Table 2: Multi-threshold China (primary = Appendix A 192 CONUS)
    print("  Table 2 structure: threshold | observed | expected | std | "
          "enrichment | p-value | significant")
    
    # Table 3: Multi-country comparative at 50mi (Appendix A)
    print("  Table 3 structure: country | n_counties | type | observed_50mi | "
          "expected | enrichment | p-value")
    
    # Table 4: Installation subset sensitivity
    print("  Table 4 structure: subset | n_installations | observed | "
          "expected | enrichment | p-value")
    print("    Subsets: All 192 CONUS Appendix A, Part 2 only, "
          "Nuclear-capable, Legacy 71")
    
    # Table 5: Panel 2022-2024
    print("  Table 5 structure: year | n_counties | mean_distance | "
          "pct_within_50mi | total_acreage")
    
    # Table 6: Ownership concentration
    print("  Table 6 structure: entity | acreage | pct_total | nearest_installation | distance")
    
    print("\n[COMPLETE] Framework ready. Run with actual AFIDA data.")
    
    return installation_sets


# =============================================================================
# ANALYSIS FUNCTIONS (called with actual data)
# =============================================================================

def run_china_multithreshold(china_holdings, installations_df, n_iter=MC_PRIMARY):
    """
    Run Table 2: Multi-threshold analysis for Chinese holdings.
    
    Parameters:
        china_holdings: DataFrame with latitude, longitude columns (county-level)
        installations_df: DataFrame with latitude, longitude columns
        n_iter: number of Monte Carlo iterations
    
    Returns:
        DataFrame with results for each threshold
    """
    distances = min_distances(china_holdings, installations_df)
    n = len(china_holdings)
    results = []
    
    for t in DISTANCE_THRESHOLDS:
        observed = int(np.sum(distances <= t))
        mc = monte_carlo_test(n, installations_df, t, n_iter)
        er = compute_enrichment(observed, mc, n_iter)
        
        results.append({
            'threshold_miles': t,
            'observed': observed,
            'expected': round(er['expected'], 2),
            'std': round(er['std'], 2),
            'enrichment': round(er['enrichment'], 1),
            'p_value': er['p_value'],
            'significant': 'Yes' if er['p_value'] < 0.001 else (
                'Yes' if er['p_value'] < 0.01 else (
                    'Yes' if er['p_value'] < 0.05 else 'No'
                )),
            'n_counties': n,
        })
    
    return pd.DataFrame(results)


def run_comparative_battery(all_holdings_by_country, installations_df,
                             threshold=PRIMARY_THRESHOLD, n_iter=MC_COMPARATIVE):
    """
    Run Table 3: Multi-country comparative analysis.
    
    Parameters:
        all_holdings_by_country: dict of {country_name: DataFrame}
        installations_df: DataFrame with latitude, longitude
        threshold: distance threshold in miles
        n_iter: Monte Carlo iterations per country
    
    Returns:
        DataFrame with results for each country
    """
    results = []
    
    for country, holdings in all_holdings_by_country.items():
        if len(holdings) < 2:
            print(f"  Skipping {country}: only {len(holdings)} counties")
            continue
        
        distances = min_distances(holdings, installations_df)
        observed = int(np.sum(distances <= threshold))
        mc = monte_carlo_test(len(holdings), installations_df, threshold, n_iter)
        er = compute_enrichment(observed, mc, n_iter)
        
        ctype = COUNTRIES.get(country.upper(), {}).get('type', 'other')
        
        results.append({
            'country': country,
            'country_type': ctype,
            'n_counties': len(holdings),
            'observed': observed,
            'expected': round(er['expected'], 2),
            'enrichment': round(er['enrichment'], 1),
            'p_value': er['p_value'],
        })
    
    return pd.DataFrame(results)


def run_installation_subset_sensitivity(china_holdings, installation_sets,
                                         threshold=PRIMARY_THRESHOLD,
                                         n_iter=MC_PRIMARY):
    """
    Run Table 4: Installation subset sensitivity + database comparison.
    
    Parameters:
        china_holdings: DataFrame with latitude, longitude
        installation_sets: dict of {label: DataFrame}
        threshold: distance in miles
        n_iter: Monte Carlo iterations
    
    Returns:
        DataFrame with results for each installation set
    """
    results = []
    
    for label, inst_df in installation_sets.items():
        distances = min_distances(china_holdings, inst_df)
        observed = int(np.sum(distances <= threshold))
        mc = monte_carlo_test(len(china_holdings), inst_df, threshold, n_iter)
        er = compute_enrichment(observed, mc, n_iter)
        
        results.append({
            'installation_set': label,
            'n_installations': len(inst_df),
            'observed': observed,
            'expected': round(er['expected'], 2),
            'enrichment': round(er['enrichment'], 1),
            'p_value': er['p_value'],
        })
    
    return pd.DataFrame(results)


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def run_part3_analysis(afida_path, centroids_path, output_dir):
    """
    Table 7: Part 3 ICBM Missile Field Analysis.
    
    Part 3 of CFIUS Appendix A defines jurisdiction over ICBM missile
    field counties using township/range boundaries, NOT distance thresholds.
    This analysis uses FIPS-code lookup rather than Haversine distance.
    
    IMPORTANT: Part 3 coverage within a county is defined by township/range,
    not whole-county. County-level AFIDA data cannot determine whether a
    specific holding falls within the Part 3 boundary within a county.
    Results are reported as "holdings in Part 3 counties" with this caveat.
    """
    print("\n[Table 7] Part 3 ICBM missile field analysis...")
    
    afida = pd.read_csv(afida_path, dtype={'fips': str}, encoding='latin-1', comment='#')
    
    # Normalize country column
    country_col = None
    for col in afida.columns:
        if 'country' in col.lower() and 'code' not in col.lower():
            country_col = col
            break
    
    if country_col is None:
        print("  WARNING: Cannot identify country column in AFIDA data")
        return pd.DataFrame()
    
    # Build Part 3 FIPS set
    p3_fips = set(PART3_COUNTIES.keys())
    
    # Flag holdings in Part 3 counties
    afida['in_part3'] = afida['fips'].isin(p3_fips)
    afida['part3_wing'] = afida['fips'].map(
        lambda f: PART3_COUNTIES[f][2] if f in PART3_COUNTIES else None
    )
    
    # All foreign holdings in Part 3 counties
    p3_all = afida[afida['in_part3']].copy()
    
    # Adversarial-nation holdings in Part 3 counties
    adversarial_keywords = ['CHINA', 'RUSSIA', 'IRAN', 'NORTH KOREA', 'KOREA, DEM']
    
    def is_adversarial(country_val):
        if pd.isna(country_val):
            return False
        c = str(country_val).upper()
        return any(k in c for k in adversarial_keywords)
    
    # Also check secondary interest columns if present
    sec_cols = [c for c in afida.columns if 'secondary' in c.lower()]
    
    def has_adversarial_secondary(row):
        for col in sec_cols:
            if pd.notna(row.get(col)) and row.get(col) not in (0, '0', '', None):
                return True
        return False
    
    p3_adversarial = p3_all[
        p3_all[country_col].apply(is_adversarial) | 
        p3_all.apply(has_adversarial_secondary, axis=1)
    ]
    
    # Summary by missile wing
    wing_summary = []
    for wing in ['90th MW (Warren)', '341st MW (Malmstrom)', '91st MW (Minot)']:
        wing_fips = {f for f, v in PART3_COUNTIES.items() if v[2] == wing}
        wing_all = p3_all[p3_all['fips'].isin(wing_fips)]
        wing_adv = p3_adversarial[p3_adversarial['fips'].isin(wing_fips)]
        
        # Count unique counties with any foreign holdings
        counties_with_holdings = wing_all['fips'].nunique()
        total_counties = len(wing_fips)
        
        # Acreage
        acres_col = None
        for col in afida.columns:
            if 'acre' in col.lower():
                acres_col = col
                break
        
        total_acres = wing_all[acres_col].sum() if acres_col else 0
        adv_acres = wing_adv[acres_col].sum() if acres_col else 0
        
        # Country breakdown
        country_counts = wing_all[country_col].value_counts().to_dict()
        top_countries = ', '.join(f"{c} ({n})" for c, n in 
                                  sorted(country_counts.items(), key=lambda x: -x[1])[:5])
        
        wing_summary.append({
            'missile_wing': wing,
            'part3_counties': total_counties,
            'counties_with_foreign_holdings': counties_with_holdings,
            'total_foreign_holdings': len(wing_all),
            'total_foreign_acres': round(total_acres, 1),
            'adversarial_holdings': len(wing_adv),
            'adversarial_acres': round(adv_acres, 1),
            'unique_countries': wing_all[country_col].nunique(),
            'top_countries': top_countries,
        })
    
    # Add totals row
    wing_summary.append({
        'missile_wing': 'ALL PART 3',
        'part3_counties': len(p3_fips),
        'counties_with_foreign_holdings': p3_all['fips'].nunique(),
        'total_foreign_holdings': len(p3_all),
        'total_foreign_acres': round(p3_all[acres_col].sum() if acres_col else 0, 1),
        'adversarial_holdings': len(p3_adversarial),
        'adversarial_acres': round(p3_adversarial[acres_col].sum() if acres_col else 0, 1),
        'unique_countries': p3_all[country_col].nunique(),
        'top_countries': '',
    })
    
    results = pd.DataFrame(wing_summary)
    
    # Print results
    print(f"\n  Part 3 counties: {len(p3_fips)}")
    print(f"  Counties with any foreign holdings: {p3_all['fips'].nunique()}")
    print(f"  Total foreign holdings in Part 3 counties: {len(p3_all)}")
    print(f"  Adversarial-nation holdings in Part 3 counties: {len(p3_adversarial)}")
    
    if len(p3_adversarial) > 0:
        print(f"\n  *** ADVERSARIAL-NATION HOLDINGS IN ICBM MISSILE FIELD COUNTIES ***")
        for _, row in p3_adversarial.iterrows():
            fips = row['fips']
            county_info = PART3_COUNTIES.get(fips, ('?', '?', '?'))
            entity = row.get('owner_name', row.get('Owner Name 1/', '?'))
            country = row[country_col]
            acres = row.get(acres_col, '?') if acres_col else '?'
            print(f"    FIPS {fips}: {county_info[0]}, {county_info[1]} "
                  f"({county_info[2]}) -- {entity} ({country}) {acres} acres")
    
    print(f"\n  CAVEAT: Part 3 jurisdiction within each county is defined by")
    print(f"  township/range boundaries. County-level AFIDA data cannot")
    print(f"  determine whether specific holdings fall within or outside")
    print(f"  the Part 3 boundary within a county.")
    
    # Save
    out_path = os.path.join(output_dir, 'part3_icbm_analysis.csv')
    results.to_csv(out_path, index=False)
    print(f"\n  -> {out_path}")
    
    # Also save adversarial detail if any
    if len(p3_adversarial) > 0:
        detail_path = os.path.join(output_dir, 'part3_adversarial_detail.csv')
        keep_cols = ['fips', country_col]
        if acres_col:
            keep_cols.append(acres_col)
        for col in ['owner_name', 'Owner Name 1/', 'part3_wing',
                     'acquisition_year', 'Acquisition Year']:
            if col in p3_adversarial.columns:
                keep_cols.append(col)
        keep_cols = [c for c in keep_cols if c in p3_adversarial.columns]
        p3_adversarial[keep_cols].to_csv(detail_path, index=False)
        print(f"  -> {detail_path}")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Spatial Clustering Analysis -- Dual-Database Framework')
    parser.add_argument('--afida', required=True, help='Path to AFIDA holdings CSV')
    parser.add_argument('--appendix-a', required=True,
                        help='Path to cfius_appendix_a_geocoded.csv')
    parser.add_argument('--legacy', required=True,
                        help='Path to installations_71.csv')
    parser.add_argument('--centroids', required=True,
                        help='Path to county centroids CSV (fips,lat,lon)')
    parser.add_argument('--output', default='results/',
                        help='Output directory')
    parser.add_argument('--demo', action='store_true',
                        help='Show framework structure without running analysis')
    
    args = parser.parse_args()
    
    if args.demo:
        run_full_analysis(args.afida, args.appendix_a, args.legacy,
                          args.centroids, args.output)
        return
    
    # Load installation databases
    print("Loading installations...")
    aa_conus, aa_part2 = load_appendix_a(args.appendix_a)
    legacy = load_legacy_installations(args.legacy)
    nuclear = get_nuclear_subset(aa_conus, legacy)
    
    # Load and geocode AFIDA data for each country
    print("\nLoading AFIDA data...")
    centroids = pd.read_csv(args.centroids, dtype={'fips': str}, encoding='latin-1', comment='#')
    if len(centroids) == 0:
        print("ERROR: County centroids file is empty or contains only comments.")
        print("       You need the NOAA c_16ap26 extraction, not the stub file.")
        print("       Run: python -c ")
        print("         import shapefile, csv")
        print("         sf = shapefile.Reader('path/to/c_16ap26')")
        print("         ...extract fips,latitude,longitude...")
        sys.exit(1)
    if 'fips' not in centroids.columns:
        # Try to detect the FIPS column
        for col in centroids.columns:
            if 'fips' in col.lower() or 'stcnty' in col.lower():
                centroids = centroids.rename(columns={col: 'fips'})
                break
    if 'latitude' not in centroids.columns:
        for col in centroids.columns:
            if col.upper() in ('LAT', 'LATITUDE', 'INTPTLAT'):
                centroids = centroids.rename(columns={col: 'latitude'})
            if col.upper() in ('LON', 'LONG', 'LONGITUDE', 'INTPTLONG'):
                centroids = centroids.rename(columns={col: 'longitude'})
    centroids['fips'] = centroids['fips'].astype(str).str.zfill(5)
    print(f"  County centroids: {len(centroids)} counties loaded")
    centroids['latitude'] = pd.to_numeric(centroids['latitude'], errors='coerce')
    centroids['longitude'] = pd.to_numeric(centroids['longitude'], errors='coerce')
    
    holdings_by_country = {}
    for country_key in COUNTRIES:
        county_df, _ = load_afida_holdings(args.afida, country_filter=country_key)
        if len(county_df) > 0 and 'fips' in county_df.columns:
            geocoded = county_df.merge(
                centroids[['fips', 'latitude', 'longitude']],
                on='fips', how='inner'
            )
            if len(geocoded) >= 2:
                holdings_by_country[COUNTRIES[country_key]['label']] = geocoded
                print(f"  {COUNTRIES[country_key]['label']}: {len(geocoded)} counties")
    
    china = holdings_by_country.get('China')
    if china is None:
        print("ERROR: No Chinese holdings found. Check AFIDA data format.")
        sys.exit(1)
    
    os.makedirs(args.output, exist_ok=True)
    
    # -- Table 2: Primary China multi-threshold (Appendix A) --
    print("\n[Table 2] China multi-threshold (Appendix A, N=10,000)...")
    t2 = run_china_multithreshold(china, aa_conus, MC_PRIMARY)
    t2.to_csv(os.path.join(args.output, 'primary_china_multithreshold.csv'), index=False)
    print(t2.to_string(index=False))
    
    # -- Table 3: Multi-country comparative (Appendix A, 50mi) --
    print("\n[Table 3] Comparative battery (Appendix A, 50mi, N=5,000)...")
    t3 = run_comparative_battery(holdings_by_country, aa_conus)
    t3.to_csv(os.path.join(args.output, 'comparative_50mi.csv'), index=False)
    print(t3.to_string(index=False))
    
    # -- Table 4: Installation subset sensitivity --
    print("\n[Table 4] Installation subset sensitivity...")
    inst_sets = {
        'Appendix A (192 CONUS)': aa_conus,
        'Appendix A Part 2 only': aa_part2,
        'Nuclear-capable': nuclear,
        'Legacy 71-site': legacy,
    }
    t4 = run_installation_subset_sensitivity(china, inst_sets)
    t4.to_csv(os.path.join(args.output, 'installation_subset_sensitivity.csv'), index=False)
    print(t4.to_string(index=False))
    
    # -- Table 7: Part 3 ICBM missile field analysis --
    t7 = run_part3_analysis(args.afida, args.centroids, args.output)
    
    # -- Summary --
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)
    aa_50 = t2[t2['threshold_miles'] == 50].iloc[0]
    print(f"  Primary (Appendix A, 50mi): {aa_50['enrichment']}x enrichment, "
          f"p={aa_50['p_value']:.5f}")
    
    legacy_row = t4[t4['installation_set'] == 'Legacy 71-site'].iloc[0]
    print(f"  Robustness (Legacy 71, 50mi): {legacy_row['enrichment']}x enrichment, "
          f"p={legacy_row['p_value']:.5f}")
    
    nuc_row = t4[t4['installation_set'] == 'Nuclear-capable'].iloc[0]
    print(f"  Nuclear-capable (50mi): {nuc_row['enrichment']}x enrichment, "
          f"p={nuc_row['p_value']:.5f}")
    
    print(f"\n  All results saved to: {args.output}")
    
    if t7 is not None and len(t7) > 0:
        totals = t7[t7['missile_wing'] == 'ALL PART 3']
        if len(totals) > 0:
            totals = totals.iloc[0]
            print(f"\n  Part 3 ICBM missile fields:")
            print(f"    {int(totals['total_foreign_holdings'])} foreign holdings "
                  f"in {int(totals['counties_with_foreign_holdings'])} of "
                  f"{int(totals['part3_counties'])} missile field counties")
            print(f"    {int(totals['adversarial_holdings'])} adversarial-nation "
                  f"holdings ({totals['adversarial_acres']} acres)")
    
    print("=" * 70)


if __name__ == '__main__':
    main()
