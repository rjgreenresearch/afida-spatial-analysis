#!/usr/bin/env python3
"""
===============================================================================
WEBB ANALYSIS — Reproducible Spatial Clustering of Foreign Agricultural
Holdings Near U.S. Military Installations

Companion code to:
  Green, R.J. (2026). "Spatial Clustering of Foreign Agricultural
  Acquisitions Near U.S. Military Installations: Comparative Evidence
  from USDA Primary Data." Working Paper.

Data Requirements:
  1. USDA AFIDA detailed holdings spreadsheet (any year 2016-2024)
     Download: fsa.usda.gov/resources/economic-policy-analysis/afida/
               annual-reports-underlying-data
  2. NOAA county centroids shapefile (c_16ap26.zip or similar)
     Download: weather.gov/gis/Counties

Usage:
  python webb_analysis.py --afida AFIDACurrentHoldingsYR2024.xlsx \
                          --centroids c_16ap26.zip \
                          [--mc-iterations 10000] \
                          [--threshold 50]

Author: Robert J. Green
github.com/rjgreenresearch/afida-spatial-analysis
===============================================================================
"""

import numpy as np
import pandas as pd
import argparse
import os
import sys
import zipfile
import warnings

warnings.filterwarnings('ignore')

# ============================================================================
# INSTALLATION DATABASE — 71 CONUS Sites
# ============================================================================

INSTALLATIONS = [
    # ICBM Fields
    ("Minot AFB", 48.4159, -101.3304, "ICBM"),
    ("Malmstrom AFB", 47.505, -111.183, "ICBM"),
    ("F.E. Warren AFB", 41.1453, -104.8618, "ICBM"),
    # Bomber Bases
    ("Whiteman AFB", 38.7268, -93.5479, "Bomber"),
    ("Barksdale AFB", 32.5013, -93.6627, "Bomber"),
    ("Dyess AFB", 32.4208, -99.8546, "Bomber"),
    ("Ellsworth AFB", 44.1453, -103.1006, "Bomber"),
    # Submarine Bases
    ("Kings Bay", 30.7986, -81.563, "SSBN"),
    ("Kitsap-Bangor", 47.7249, -122.7144, "SSBN"),
    # Combatant Commands
    ("MacDill AFB", 27.8491, -82.5212, "COCOM"),
    ("Offutt AFB", 41.1185, -95.9124, "COCOM"),
    ("Peterson SFB", 38.8091, -104.7145, "COCOM"),
    # Army
    ("Fort Liberty", 35.1391, -79.0064, "Army"),
    ("Fort Cavazos", 31.1349, -97.7753, "Army"),
    ("Fort Stewart", 31.8691, -81.6095, "Army"),
    ("Fort Campbell", 36.6627, -87.4714, "Army"),
    ("Fort Drum", 44.0554, -75.7588, "Army"),
    ("Fort Riley", 39.055, -96.7645, "Army"),
    ("Fort Leonard Wood", 37.7464, -92.1467, "Army"),
    ("Fort Sill", 34.65, -98.39, "Army"),
    ("Redstone Arsenal", 34.6849, -86.6471, "Army"),
    # Space Force
    ("Schriever SFB", 38.8058, -104.5266, "Space"),
    ("Vandenberg SFB", 34.7332, -120.5681, "Space"),
    ("Patrick SFB", 28.2346, -80.6101, "Space"),
    ("Cape Canaveral", 28.4889, -80.5778, "Space"),
    # ISR
    ("Grand Forks AFB", 47.9547, -97.3811, "ISR"),
    ("Beale AFB", 39.1362, -121.4367, "ISR"),
    ("Creech AFB", 36.5822, -115.6711, "ISR"),
    # Fighter
    ("Nellis AFB", 36.2361, -115.0343, "Fighter"),
    ("Mountain Home AFB", 43.0436, -115.8664, "Fighter"),
    ("Luke AFB", 33.5354, -112.3838, "Fighter"),
    ("Hill AFB", 41.1241, -111.9732, "Fighter"),
    ("Eglin AFB", 30.4832, -86.5254, "Fighter"),
    ("Tyndall AFB", 30.0696, -85.5782, "Fighter"),
    ("JB Langley-Eustis", 37.0832, -76.3605, "Fighter"),
    # Airlift
    ("JB McGuire-Dix", 40.0157, -74.5936, "Airlift"),
    # Naval
    ("NS Norfolk", 36.9461, -76.3014, "Naval"),
    ("NAS Jacksonville", 30.2358, -81.6806, "Naval"),
    ("NAS Corpus Christi", 27.6934, -97.2901, "Naval"),
    ("NAS Kingsville", 27.5069, -97.8093, "Naval"),
    ("NB San Diego", 32.6839, -117.1291, "Naval"),
    ("Camp Pendleton", 33.3853, -117.5653, "Naval"),
    # Training
    ("Laughlin AFB", 29.3596, -100.782, "Training"),
    ("Columbus AFB", 33.6395, -88.4427, "Training"),
    ("Vance AFB", 36.3394, -97.9172, "Training"),
    # DOE Nuclear Weapons Complex
    ("Los Alamos NL", 35.8443, -106.2874, "Nuclear"),
    ("Sandia NL", 35.0585, -106.5493, "Nuclear"),
    ("Pantex Plant", 35.3167, -101.5564, "Nuclear"),
    ("Y-12 NSC", 35.9843, -84.2537, "Nuclear"),
    ("Savannah River Site", 33.3417, -81.7353, "Nuclear"),
    ("Idaho NL", 43.5157, -112.9477, "Nuclear"),
    ("Hanford Site", 46.5506, -119.4881, "Nuclear"),
    # Testing
    ("White Sands MR", 32.3894, -106.4786, "Testing"),
    ("Dugway PG", 40.1914, -112.9374, "Testing"),
    ("Aberdeen PG", 39.4684, -76.131, "Testing"),
    ("Camp Grayling", 44.6617, -84.7281, "Testing"),
    # Depots
    ("Camp Navajo", 35.2369, -111.8339, "Depot"),
    ("NSA Crane", 38.8614, -86.8383, "Depot"),
    ("Avon Park AFR", 27.6461, -81.3172, "Range"),
    # Guard
    ("Iowa NG JFHQ", 41.5868, -93.625, "Guard"),
    # Intelligence
    ("NSA Fort Meade", 39.1086, -76.7717, "Intel"),
    ("NGA Springfield", 38.751, -77.1564, "Intel"),
    ("CIA Langley", 38.9516, -77.1467, "Intel"),
    ("DIA Bolling", 38.8426, -77.0137, "Intel"),
    # Defense Industry
    ("Lockheed Martin Marietta", 33.9553, -84.5194, "Industry"),
    ("Boeing St. Louis", 38.7503, -90.3701, "Industry"),
    ("Bath Iron Works", 43.9076, -69.8195, "Industry"),
    ("HII Newport News", 36.978, -76.436, "Industry"),
    ("GD Electric Boat", 41.349, -72.0759, "Industry"),
    # Missile Defense
    ("Fort Greely", 63.9789, -145.7322, "Defense"),
]


# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def haversine_matrix(lats1, lons1, lats2, lons2):
    """Vectorized pairwise Haversine distance in miles."""
    R = 3958.8
    la1 = np.radians(lats1[:, None]); lo1 = np.radians(lons1[:, None])
    la2 = np.radians(lats2[None, :]); lo2 = np.radians(lons2[None, :])
    a = np.sin((la2 - la1) / 2)**2 + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def load_afida(filepath):
    """Load an AFIDA spreadsheet, auto-detecting the header row."""
    raw = pd.read_excel(filepath, header=None)
    for i in range(min(10, len(raw))):
        vals = [str(v).strip() for v in raw.iloc[i] if pd.notna(v)]
        if any('Country' == v for v in vals) and any('Acre' in v for v in vals):
            df = pd.read_excel(filepath, header=i)
            df.columns = [str(c).strip() for c in df.columns]
            return df
    raise ValueError(f"Could not find header row in {filepath}")


def load_centroids(zip_path):
    """Load county centroids from NOAA shapefile zip."""
    try:
        import shapefile
    except ImportError:
        print("ERROR: pyshp required. Install with: pip install pyshp")
        sys.exit(1)

    extract_dir = os.path.join(os.path.dirname(zip_path), '_centroids_tmp')
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_dir)

    import glob
    shp = glob.glob(os.path.join(extract_dir, '**/*.shp'), recursive=True)
    if not shp:
        raise FileNotFoundError("No .shp file found in zip")

    sf = shapefile.Reader(shp[0].replace('.shp', ''))
    records = []
    for rec in sf.iterRecords():
        d = dict(zip([f[0] for f in sf.fields[1:]], rec))
        records.append(d)
    centroids = pd.DataFrame(records)
    centroids['FIPS'] = centroids['FIPS'].str.strip()
    return centroids


def filter_by_country(df, patterns):
    """Filter AFIDA data by country name patterns."""
    country_col = next((c for c in df.columns if c == 'Country'), None)
    if country_col is None:
        raise ValueError("No 'Country' column found in AFIDA data")
    mask = pd.Series(False, index=df.index)
    for p in patterns:
        mask |= df[country_col].astype(str).str.contains(p, case=False, na=False)
    # Also check secondary interest flags
    for label, search in [('CHINA', 'China'), ('IRAN', 'Iran'), ('RUSSIA', 'Russia')]:
        if any(label in p.upper() for p in patterns):
            sec_col = next((c for c in df.columns if 'Secondary' in c and search in c), None)
            if sec_col:
                mask |= (pd.to_numeric(df[sec_col], errors='coerce') == 1)
    return df[mask].copy()


def run_monte_carlo(n_counties, base_lats, base_lons, threshold,
                    n_iter=10000, lat_range=(25.0, 49.0), lon_range=(-125.0, -66.0)):
    """Monte Carlo permutation test for spatial clustering."""
    mc = np.zeros(n_iter)
    for i in range(n_iter):
        rl = np.random.uniform(lat_range[0], lat_range[1], n_counties)
        rn = np.random.uniform(lon_range[0], lon_range[1], n_counties)
        dm = haversine_matrix(rl, rn, base_lats, base_lons)
        mc[i] = np.sum(dm.min(axis=1) <= threshold)
    return mc


# ============================================================================
# MAIN ANALYSIS
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Webb Analysis — Spatial Clustering')
    parser.add_argument('--afida', required=True, help='Path to AFIDA xlsx file')
    parser.add_argument('--centroids', required=True, help='Path to NOAA county zip')
    parser.add_argument('--mc-iterations', type=int, default=10000, help='Monte Carlo iterations')
    parser.add_argument('--threshold', type=int, default=50, help='Primary distance threshold (miles)')
    parser.add_argument('--seed', type=int, default=20260318, help='Random seed')
    parser.add_argument('--all-countries', action='store_true', help='Run multi-country comparison')
    args = parser.parse_args()

    np.random.seed(args.seed)

    print("=" * 78)
    print("  WEBB ANALYSIS — Spatial Clustering of Foreign Agricultural Holdings")
    print("  Robert J. Green | github.com/rjgreenresearch/afida-spatial-analysis")
    print("=" * 78)

    # Load data
    print("\nLoading AFIDA data...", end=' ')
    df = load_afida(args.afida)
    print(f"{len(df):,} holdings")

    print("Loading county centroids...", end=' ')
    centroids = load_centroids(args.centroids)
    print(f"{len(centroids):,} counties")

    # Setup installations
    bases = pd.DataFrame(INSTALLATIONS, columns=['name', 'lat', 'lon', 'category'])
    blats = bases['lat'].values.astype(float)
    blons = bases['lon'].values.astype(float)
    print(f"Installation database: {len(bases)} sites")

    # Prepare FIPS
    fips_col = next((c for c in df.columns if c == 'FIPS'), None)
    acres_col = next((c for c in df.columns if 'Acre' in c), None)
    df['fips_5'] = df[fips_col].astype(str).str.strip().str.zfill(5)
    df['acres_num'] = pd.to_numeric(df[acres_col], errors='coerce')

    # Country groups
    country_groups = {'CHINA': ['CHINA', 'CHINESE']}
    if args.all_countries:
        country_groups.update({
            'CANADA': ['CANADA'], 'UNITED KINGDOM': ['UNITED KINGDOM', 'GREAT BRITAIN'],
            'NETHERLANDS': ['NETHERLANDS'], 'GERMANY': ['GERMANY'], 'ITALY': ['ITALY'],
            'JAPAN': ['JAPAN'], 'BRAZIL': ['BRAZIL'], 'MEXICO': ['MEXICO'],
            'SAUDI ARABIA': ['SAUDI ARABIA'], 'IRAN': ['IRAN'], 'RUSSIA': ['RUSSIA'],
        })

    results = {}
    for group_name, patterns in country_groups.items():
        subset = filter_by_country(df, patterns)
        if len(subset) < 3:
            print(f"\n  {group_name}: {len(subset)} holdings — skipped (too few)")
            continue

        # Geocode
        merged = subset.merge(centroids[['FIPS', 'LAT', 'LON']],
                              left_on='fips_5', right_on='FIPS', how='left')
        geo = merged.dropna(subset=['LAT', 'LON'])
        if len(geo) < 3:
            continue

        # County aggregation
        dm = haversine_matrix(geo['LAT'].values.astype(float),
                              geo['LON'].values.astype(float), blats, blons)
        geo_c = geo.copy()
        geo_c['min_dist'] = dm.min(axis=1)
        county = geo_c.groupby('fips_5').agg(
            d=('min_dist', 'min'), ac=('acres_num', 'sum')).reset_index()

        nc = len(county)
        dists = county['d'].values
        obs = int(np.sum(dists <= args.threshold))

        # Monte Carlo
        iters = args.mc_iterations if group_name == 'CHINA' else min(args.mc_iterations, 5000)
        mc = run_monte_carlo(nc, blats, blons, args.threshold, n_iter=iters)
        exp = mc.mean()
        enr = obs / max(exp, 0.01)
        p = (np.sum(mc >= obs) + 1) / (iters + 1)

        results[group_name] = {
            'holdings': len(subset), 'counties': nc,
            'acres': county['ac'].sum(), 'mean_dist': dists.mean(),
            'obs': obs, 'exp': exp, 'enrichment': enr, 'p': p,
        }

        sig = 'NON-RANDOM' if p < 0.001 else 'SIG' if p < 0.05 else 'ns'
        print(f"\n  {group_name:<20} Counties={nc:>5}  "
              f"Within {args.threshold}mi: {obs}/{nc} ({obs/nc*100:.0f}%)  "
              f"E(H0)={exp:.1f}  Enrichment={enr:.1f}x  p={p:.5f}  [{sig}]")

    # Summary
    print(f"\n{'=' * 78}")
    print(f"  RESULTS SUMMARY ({args.threshold}-mile threshold)")
    print(f"{'=' * 78}")
    print(f"\n  {'Country':<20} {'Counties':>9} {'Obs':>5} {'E(H0)':>7} "
          f"{'Enrich':>8} {'p-value':>9} {'Result':>12}")
    print(f"  {'-'*20} {'-'*9} {'-'*5} {'-'*7} {'-'*8} {'-'*9} {'-'*12}")
    for name, r in sorted(results.items(), key=lambda x: x[1]['enrichment'], reverse=True):
        sig = 'NON-RANDOM' if r['p'] < 0.001 else 'SIGNIFICANT' if r['p'] < 0.05 else 'Not sig.'
        print(f"  {name:<20} {r['counties']:>9} {r['obs']:>5} {r['exp']:>7.1f} "
              f"{r['enrichment']:>7.1f}x {r['p']:>9.5f} {sig:>12}")

    print(f"\n{'=' * 78}")
    print(f"  Analysis complete. All data from USDA AFIDA primary sources.")
    print(f"  Robert J. Green | github.com/rjgreenresearch/afida-spatial-analysis")
    print(f"{'=' * 78}")


if __name__ == '__main__':
    main()
