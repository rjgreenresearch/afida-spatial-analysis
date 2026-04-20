#!/usr/bin/env python3
"""
===============================================================================
THE WEBB ANALYSIS — Applied to Real-World CFIUS/AFIDA Data
===============================================================================

Applying the spatial correlation methodology from Digital Harvest (2026)
to publicly reported Chinese agricultural land acquisitions in the
continental United States.

Data Sources:
    - USDA AFIDA annual reports (2021-2024)
    - CFIUS transaction reviews and public filings
    - Congressional testimony and GAO reports (GAO-24-106337)
    - New York Post 19-base investigation (June 2024)
    - Modern Diplomacy military-linked purchase reporting (Jan 2026)
    - NBC News AFIDA document review (Aug 2023)
    - American Farm Bureau Federation AFIDA analysis (2025)

Methodology (from Digital Harvest, Chapter 1):
    - Haversine geodesic distance computation
    - Monte Carlo permutation testing (n=10,000)
    - Multi-threshold spatial correlation (10, 25, 50, 100 miles)
    - CFIUS jurisdiction analysis (1-mile and 100-mile tiers)
    - Concentration analysis (Herfindahl-Hirschman Index)

IMPORTANT DISCLAIMER:
    This analysis uses publicly available data compiled from government
    reports and credible journalism. It is presented as a demonstration
    of spatial econometric methods and as companion content to the novel
    Digital Harvest by Robert J. Green (www.digitalharvestbook.com).
    Proximity alone does not establish intent. The data quality limitations
    noted by GAO (2024) and USDA apply to all findings herein.

===============================================================================
"""

import numpy as np
import pandas as pd
from scipy import stats
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os
import warnings

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

RANDOM_SEED = 20260118
MC_ITERATIONS = 10_000
CFIUS_TIER1 = 1        # miles — CFIUS Appendix A (direct adjacency)
CFIUS_TIER2 = 100      # miles — CFIUS Appendix A (extended range)
THRESHOLDS = [10, 25, 50, 100]  # Real-world analysis thresholds (miles)

US_LAT_RANGE = (25.0, 49.0)
US_LON_RANGE = (-125.0, -66.0)

np.random.seed(RANDOM_SEED)

DIVIDER = "=" * 78
THIN_DIV = "-" * 78

def header(title):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)

def subheader(title):
    print(f"\n{THIN_DIV}")
    print(f"  {title}")
    print(THIN_DIV)

# ============================================================================
# MAJOR CONUS MILITARY INSTALLATIONS + CRITICAL INFRASTRUCTURE
# ============================================================================

def build_installation_database():
    """
    Comprehensive database of CONUS military installations and critical
    infrastructure sites. Includes CFIUS Appendix A sites, nuclear
    weapons infrastructure, major training installations, and
    combatant command headquarters.
    
    72 installations across all service branches and DOE.
    """
    data = [
        # ICBM Fields
        ("Minot AFB", 48.4159, -101.3304, "ND", "USAF", "ICBM / Bomber"),
        ("Malmstrom AFB", 47.5050, -111.1830, "MT", "USAF", "ICBM"),
        ("F.E. Warren AFB", 41.1453, -104.8618, "WY", "USAF", "ICBM"),
        # Bomber Bases
        ("Whiteman AFB", 38.7268, -93.5479, "MO", "USAF", "Bomber (B-2)"),
        ("Barksdale AFB", 32.5013, -93.6627, "LA", "USAF", "Bomber (B-52) / AFGSC"),
        ("Dyess AFB", 32.4208, -99.8546, "TX", "USAF", "Bomber (B-1B)"),
        ("Ellsworth AFB", 44.1453, -103.1006, "SD", "USAF", "Bomber (B-1B)"),
        # Submarine Bases
        ("Naval Sub Base Kings Bay", 30.7986, -81.5630, "GA", "USN", "SSBN (Trident)"),
        ("Naval Base Kitsap-Bangor", 47.7249, -122.7144, "WA", "USN", "SSBN (Trident)"),
        # Combatant Command HQs
        ("MacDill AFB", 27.8491, -82.5212, "FL", "USAF", "CENTCOM / SOCOM HQ"),
        ("Offutt AFB", 41.1185, -95.9124, "NE", "USAF", "STRATCOM HQ"),
        ("Peterson SFB", 38.8091, -104.7145, "CO", "USSF", "NORTHCOM / NORAD"),
        # Major Army Installations
        ("Fort Liberty", 35.1391, -79.0064, "NC", "USA", "Airborne / SOF"),
        ("Fort Cavazos", 31.1349, -97.7753, "TX", "USA", "Armor / Corps HQ"),
        ("Fort Stewart", 31.8691, -81.6095, "GA", "USA", "Infantry Division"),
        ("Fort Campbell", 36.6627, -87.4714, "TN", "USA", "Air Assault / SOF"),
        ("Fort Drum", 44.0554, -75.7588, "NY", "USA", "Mountain Infantry"),
        ("Fort Riley", 39.0550, -96.7645, "KS", "USA", "Infantry Division"),
        ("Fort Leonard Wood", 37.7464, -92.1467, "MO", "USA", "Maneuver / CBRN"),
        ("Fort Sill", 34.6500, -98.3900, "OK", "USA", "Fires / ADA"),
        ("Redstone Arsenal", 34.6849, -86.6471, "AL", "USA", "Missile / Space / PEO"),
        # Space Force
        ("Schriever SFB", 38.8058, -104.5266, "CO", "USSF", "Space Ops / GPS"),
        ("Vandenberg SFB", 34.7332, -120.5681, "CA", "USSF", "ICBM Test / Space Launch"),
        ("Patrick SFB", 28.2346, -80.6101, "FL", "USSF", "Space Launch"),
        ("Cape Canaveral SFS", 28.4889, -80.5778, "FL", "USSF", "Space Launch"),
        # ISR / Special
        ("Grand Forks AFB", 47.9547, -97.3811, "ND", "USAF", "ISR / Space Networking"),
        ("Beale AFB", 39.1362, -121.4367, "CA", "USAF", "ISR / U-2 / Global Hawk"),
        ("Creech AFB", 36.5822, -115.6711, "NV", "USAF", "RPA / UAS Operations"),
        ("Nellis AFB", 36.2361, -115.0343, "NV", "USAF", "Advanced Weapons / NTTR"),
        ("Mountain Home AFB", 43.0436, -115.8664, "ID", "USAF", "Fighter / Strike"),
        # Fighter Bases
        ("Luke AFB", 33.5354, -112.3838, "AZ", "USAF", "Fighter Training (F-35)"),
        ("Hill AFB", 41.1241, -111.9732, "UT", "USAF", "Fighter / Depot"),
        ("Eglin AFB", 30.4832, -86.5254, "FL", "USAF", "Weapons / Testing"),
        ("Tyndall AFB", 30.0696, -85.5782, "FL", "USAF", "Fighter / Air Dominance"),
        ("Joint Base Langley-Eustis", 37.0832, -76.3605, "VA", "USAF", "Fighter / ACC HQ"),
        ("Joint Base McGuire-Dix-Lakehurst", 40.0157, -74.5936, "NJ", "USAF", "Airlift / Joint"),
        # Naval Stations
        ("Naval Station Norfolk", 36.9461, -76.3014, "VA", "USN", "Fleet HQ"),
        ("NAS Jacksonville", 30.2358, -81.6806, "FL", "USN", "ASW / Maritime Patrol"),
        ("NAS Corpus Christi", 27.6934, -97.2901, "TX", "USN", "Aviation Training"),
        ("NAS Kingsville", 27.5069, -97.8093, "TX", "USN", "Jet Training"),
        ("Naval Base San Diego", 32.6839, -117.1291, "CA", "USN", "Pacific Fleet"),
        ("Camp Pendleton", 33.3853, -117.5653, "CA", "USMC", "I MEF / Expeditionary"),
        ("JBPHH", 21.3469, -157.9740, "HI", "USN/USAF", "INDOPACOM HQ"),
        # Pilot Training
        ("Laughlin AFB", 29.3596, -100.7820, "TX", "USAF", "Pilot Training"),
        ("Columbus AFB", 33.6395, -88.4427, "MS", "USAF", "Pilot Training"),
        ("Vance AFB", 36.3394, -97.9172, "OK", "USAF", "Pilot Training"),
        # DOE Nuclear Weapons Complex
        ("Los Alamos National Lab", 35.8443, -106.2874, "NM", "DOE", "Nuclear Weapons Design"),
        ("Sandia National Lab", 35.0585, -106.5493, "NM", "DOE", "Nuclear Weapons Engineering"),
        ("Pantex Plant", 35.3167, -101.5564, "TX", "DOE", "Nuclear Weapons Assembly"),
        ("Y-12 NSC", 35.9843, -84.2537, "TN", "DOE", "Nuclear Components"),
        ("Savannah River Site", 33.3417, -81.7353, "SC", "DOE", "Nuclear Materials"),
        ("Idaho National Lab", 43.5157, -112.9477, "ID", "DOE", "Nuclear Research"),
        ("Hanford Site", 46.5506, -119.4881, "WA", "DOE", "Nuclear Cleanup / Research"),
        # Missile Defense
        ("Fort Greely", 63.9789, -145.7322, "AK", "USA", "GMD / Missile Defense"),
        # Testing / Proving Grounds
        ("White Sands MR", 32.3894, -106.4786, "NM", "USA", "Weapons Testing"),
        ("Dugway PG", 40.1914, -112.9374, "UT", "USA", "CBRN Defense"),
        ("Aberdeen PG", 39.4684, -76.1310, "MD", "USA", "Weapons Testing / Research"),
        ("Camp Grayling", 44.6617, -84.7281, "MI", "ARNG", "Largest ARNG Training Center"),
        # Depots / Supply
        ("Camp Navajo", 35.2369, -111.8339, "AZ", "USA", "Munitions Storage"),
        ("NSA Crane", 38.8614, -86.8383, "IN", "USN", "Ordnance / Expeditionary"),
        ("Avon Park AFR", 27.6461, -81.3172, "FL", "USAF", "Weapons Range"),
        # Guard / Reserve Key Sites
        ("Iowa NG JFHQ", 41.5868, -93.6250, "IA", "ARNG", "Joint Force HQ"),
        # Key Intelligence
        ("NSA Fort Meade", 39.1086, -76.7717, "MD", "NSA", "SIGINT HQ"),
        ("NGA Springfield", 38.7510, -77.1564, "VA", "NGA", "GEOINT HQ"),
        ("CIA Langley", 38.9516, -77.1467, "VA", "CIA", "Intelligence HQ"),
        ("DIA Bolling", 38.8426, -77.0137, "DC", "DIA", "Defense Intelligence HQ"),
        # Critical Defense Industry
        ("Lockheed Martin Marietta", 33.9553, -84.5194, "GA", "Industry", "F-35 Production"),
        ("Boeing St. Louis", 38.7503, -90.3701, "MO", "Industry", "Fighter / Defense HQ"),
        ("Bath Iron Works", 43.9076, -69.8195, "ME", "Industry", "DDG Shipbuilding"),
        ("Huntington Ingalls Newport News", 36.9780, -76.4360, "VA", "Industry", "CVN / SSN Shipbuilding"),
        ("General Dynamics EB Groton", 41.3490, -72.0759, "CT", "Industry", "SSN / SSBN Shipbuilding"),
    ]

    df = pd.DataFrame(data, columns=['name', 'latitude', 'longitude', 'state',
                                      'branch', 'category'])
    return df


# ============================================================================
# VECTORIZED HAVERSINE
# ============================================================================

def haversine_matrix(lats1, lons1, lats2, lons2):
    R = 3958.8
    lat1 = np.radians(lats1[:, np.newaxis])
    lon1 = np.radians(lons1[:, np.newaxis])
    lat2 = np.radians(lats2[np.newaxis, :])
    lon2 = np.radians(lons2[np.newaxis, :])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def proximity_analysis(acquisitions, installations, thresholds):
    """Analyze what fraction of acquisitions fall within each distance threshold."""
    # Use reported distances from the dataset
    results = {}
    distances = acquisitions['distance_to_nearest_base_mi'].values

    for t in thresholds:
        within = np.sum(distances <= t)
        pct = within / len(distances) * 100
        results[t] = {'count': within, 'total': len(distances), 'pct': pct}

    return results, distances


def cfius_jurisdiction_analysis(acquisitions):
    """Analyze how many acquisitions fall within CFIUS review tiers."""
    distances = acquisitions['distance_to_nearest_base_mi'].values

    tier1 = np.sum(distances <= CFIUS_TIER1)
    tier2 = np.sum(distances <= CFIUS_TIER2)
    outside = np.sum(distances > CFIUS_TIER2)

    return {
        'tier1_1mi': tier1,
        'tier2_100mi': tier2,
        'outside_100mi': outside,
        'total': len(distances),
        'pct_within_100mi': tier2 / len(distances) * 100,
    }


def monte_carlo_proximity(n_sites, installations, threshold_mi, n_iter=MC_ITERATIONS):
    """
    Monte Carlo test: if n_sites were placed randomly in CONUS,
    how many would fall within threshold_mi of any installation?
    """
    base_lats = installations['latitude'].values.astype(float)
    base_lons = installations['longitude'].values.astype(float)
    mc_counts = np.zeros(n_iter)

    for i in range(n_iter):
        rand_lats = np.random.uniform(US_LAT_RANGE[0], US_LAT_RANGE[1], n_sites)
        rand_lons = np.random.uniform(US_LON_RANGE[0], US_LON_RANGE[1], n_sites)
        dm = haversine_matrix(rand_lats, rand_lons, base_lats, base_lons)
        mc_counts[i] = np.sum(dm.min(axis=1) <= threshold_mi)

    return mc_counts


def acreage_concentration(acquisitions):
    """Herfindahl-Hirschman Index for ownership concentration."""
    entity_acreage = acquisitions.groupby('entity_name')['acreage_approx'].sum()
    total = entity_acreage.sum()
    shares = entity_acreage / total * 100
    hhi = (shares**2).sum()
    return hhi, entity_acreage.sort_values(ascending=False)


def generate_real_world_figures(acquisitions, installations, distances,
                                 prox_results, mc_25, mc_50, output_dir):
    """Generate publication-quality figures for real-world analysis."""
    sns.set_theme(style='whitegrid', font_scale=1.1)
    fig_paths = []

    # --- Figure 1: Distance Distribution ---
    fig, ax = plt.subplots(figsize=(11, 6))
    valid = distances[~np.isnan(distances)]
    ax.hist(valid, bins=20, density=False, alpha=0.7, color='#2c5f8a',
            edgecolor='white', linewidth=0.8, label='Documented acquisitions')
    ax.axvline(x=CFIUS_TIER1, color='#c0392b', linewidth=2.5, linestyle='-',
               label=f'{CFIUS_TIER1}-mile CFIUS Tier 1')
    ax.axvline(x=CFIUS_TIER2, color='#e67e22', linewidth=2, linestyle='--',
               label=f'{CFIUS_TIER2}-mile CFIUS Tier 2')
    ax.axvline(x=25, color='#27ae60', linewidth=1.5, linestyle=':',
               label='25-mile reference')
    ax.set_xlabel('Reported Distance to Nearest Installation (miles)', fontsize=12)
    ax.set_ylabel('Number of Acquisitions', fontsize=12)
    ax.set_title('Real-World Chinese Farmland Acquisitions:\n'
                 'Distance to Nearest U.S. Military Installation or Critical Infrastructure',
                 fontsize=13, fontweight='bold')
    ax.legend(frameon=True, fancybox=True, shadow=True)
    plt.tight_layout()
    path1 = os.path.join(output_dir, 'rw_fig1_distance_distribution.png')
    fig.savefig(path1, dpi=200, bbox_inches='tight')
    fig_paths.append(path1)
    plt.close()

    # --- Figure 2: Monte Carlo (25-mile threshold) ---
    fig, ax = plt.subplots(figsize=(10, 6))
    observed_25 = prox_results[25]['count']
    ax.hist(mc_25, bins=range(int(mc_25.max()) + 3), density=True,
            alpha=0.6, color='#95a5a6', edgecolor='white',
            label=f'H₀ distribution (n={MC_ITERATIONS:,})')
    ax.axvline(x=observed_25, color='#c0392b', linewidth=3,
               label=f'Observed: {observed_25} within 25 mi')
    ax.axvline(x=mc_25.mean(), color='#2c5f8a', linewidth=2, linestyle='--',
               label=f'Expected: {mc_25.mean():.1f}')
    p_val = (np.sum(mc_25 >= observed_25) + 1) / (MC_ITERATIONS + 1)
    ax.set_title(f'Monte Carlo Permutation Test (25-Mile Threshold)\n'
                 f'p-value = {p_val:.5f}  |  Enrichment: {observed_25/max(mc_25.mean(),0.01):.1f}×',
                 fontsize=13, fontweight='bold')
    ax.set_xlabel('Sites Within 25 Miles of Installation', fontsize=12)
    ax.set_ylabel('Probability Density', fontsize=12)
    ax.legend(frameon=True, fancybox=True, shadow=True)
    plt.tight_layout()
    path2 = os.path.join(output_dir, 'rw_fig2_mc_25mi.png')
    fig.savefig(path2, dpi=200, bbox_inches='tight')
    fig_paths.append(path2)
    plt.close()

    # --- Figure 3: Acreage by Entity ---
    fig, ax = plt.subplots(figsize=(12, 6))
    entity_acreage = acquisitions.groupby('entity_name')['acreage_approx'].sum().sort_values()
    entity_acreage = entity_acreage[entity_acreage > 0].tail(10)
    short_names = [n[:35] for n in entity_acreage.index]
    colors = sns.color_palette('RdYlBu_r', len(entity_acreage))
    bars = ax.barh(short_names, entity_acreage.values, color=colors,
                    edgecolor='white', linewidth=0.8)
    ax.set_xlabel('Reported Acreage', fontsize=12)
    ax.set_title('Chinese-Linked U.S. Agricultural Land Holdings by Entity\n'
                 'Source: USDA AFIDA, Congressional Reports, CFIUS Filings',
                 fontsize=13, fontweight='bold')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    path3 = os.path.join(output_dir, 'rw_fig3_acreage_by_entity.png')
    fig.savefig(path3, dpi=200, bbox_inches='tight')
    fig_paths.append(path3)
    plt.close()

    # --- Figure 4: Installation Category Exposure ---
    fig, ax = plt.subplots(figsize=(10, 6))
    cat_counts = acquisitions.groupby('installation_type').size().sort_values()
    colors = sns.color_palette('viridis', len(cat_counts))
    ax.barh([c[:30] for c in cat_counts.index], cat_counts.values,
            color=colors, edgecolor='white')
    ax.set_xlabel('Number of Nearby Acquisitions', fontsize=12)
    ax.set_title('Acquisitions by Nearest Installation Category',
                 fontsize=13, fontweight='bold')
    plt.tight_layout()
    path4 = os.path.join(output_dir, 'rw_fig4_installation_categories.png')
    fig.savefig(path4, dpi=200, bbox_inches='tight')
    fig_paths.append(path4)
    plt.close()

    return fig_paths


# ============================================================================
# MAIN
# ============================================================================

def main():
    print(DIVIDER)
    print("  THE WEBB ANALYSIS — Real-World Application")
    print("  Spatial Correlation of Chinese Agricultural Acquisitions")
    print("  with U.S. Military Installations & Critical Infrastructure")
    print(f"  Method: Digital Harvest, Ch. 1 (Robert J. Green, 2026)")
    print(f"  www.digitalharvestbook.com")
    print(DIVIDER)

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Load data
    header("1. DATA INGESTION")
    acq = pd.read_csv(os.path.join(script_dir, 'real_world_chinese_acquisitions.csv'))
    bases = build_installation_database()

    # Clean acreage
    acq['acreage_approx'] = pd.to_numeric(acq['acreage_approx'], errors='coerce').fillna(0)
    acq['distance_to_nearest_base_mi'] = pd.to_numeric(acq['distance_to_nearest_base_mi'], errors='coerce')

    print(f"\n  Documented acquisitions loaded:  {len(acq)}")
    print(f"  Military/CI installations:       {len(bases)}")
    print(f"  Total reported acreage:          {acq['acreage_approx'].sum():,.0f}")
    print(f"  States with acquisitions:        {acq['state'].nunique()}")
    print(f"  Unique acquiring entities:       {acq['entity_name'].nunique()}")

    # Proximity analysis
    header("2. PROXIMITY ANALYSIS")
    prox, distances = proximity_analysis(acq, bases, THRESHOLDS)

    print(f"\n  {'Threshold':>12}  {'Count':>8}  {'Pct':>8}")
    print(f"  {'-'*12}  {'-'*8}  {'-'*8}")
    for t in THRESHOLDS:
        r = prox[t]
        print(f"  {t:>10} mi  {r['count']:>8}  {r['pct']:>7.1f}%")

    # CFIUS jurisdiction
    header("3. CFIUS JURISDICTION ANALYSIS")
    cfius = cfius_jurisdiction_analysis(acq)

    print(f"""
  CFIUS Tier 1 (≤ 1 mile):       {cfius['tier1_1mi']} acquisitions
  CFIUS Tier 2 (≤ 100 miles):    {cfius['tier2_100mi']} acquisitions
  Outside CFIUS jurisdiction:     {cfius['outside_100mi']} acquisitions
  Pct within 100-mile review:    {cfius['pct_within_100mi']:.1f}%

  NOTE: Prior to November 2024, CFIUS jurisdiction covered far fewer
  installations. The Fufeng Group purchase (12 mi from Grand Forks AFB)
  was explicitly found "not a covered transaction" by CFIUS in Dec 2022.
  The regulatory gap identified in Digital Harvest is the real gap.
    """)

    # Monte Carlo tests
    header("4. MONTE CARLO PERMUTATION TESTS")

    valid_distances = distances[~np.isnan(distances)]
    n_valid = len(valid_distances)

    for t in [25, 50]:
        print(f"\n  Running {MC_ITERATIONS:,} iterations at {t}-mile threshold...")
        mc = monte_carlo_proximity(n_valid, bases, t)
        observed = prox[t]['count']
        p_val = (np.sum(mc >= observed) + 1) / (MC_ITERATIONS + 1)
        enrichment = observed / max(mc.mean(), 0.01)

        subheader(f"{t}-MILE THRESHOLD RESULT")
        print(f"""
  Observed within {t} mi:    {observed} / {n_valid}
  Expected under H₀:        {mc.mean():.2f} (σ = {mc.std():.2f})
  Maximum under H₀:         {int(mc.max())}
  Enrichment ratio:          {enrichment:.1f}×
  p-value:                   {p_val:.5f}
  Confidence:                {(1-p_val)*100:.3f}%
  Interpretation:            {'NON-RANDOM' if p_val < 0.001 else 'SIGNIFICANT' if p_val < 0.05 else 'NOT SIGNIFICANT'}
        """)

        if t == 25:
            mc_25 = mc
        else:
            mc_50 = mc

    # Concentration analysis
    header("5. OWNERSHIP CONCENTRATION (HHI)")
    hhi, entity_ranking = acreage_concentration(acq)

    print(f"\n  Herfindahl-Hirschman Index: {hhi:.0f}")
    print(f"  (HHI > 2500 = highly concentrated market)")
    print(f"\n  Top entities by acreage:")
    for name, acres in entity_ranking.head(8).items():
        if acres > 0:
            print(f"    {name[:45]:<48} {acres:>10,.0f} acres")

    # The Digital Harvest comparison
    header("6. FICTION vs. REALITY — THE WEBB COMPARISON")

    print(f"""
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                DIGITAL HARVEST vs. REAL-WORLD DATA                     │
  ├──────────────────────┬──────────────────────┬────────────────────────── ┤
  │  Parameter           │  Digital Harvest     │  Real World (AFIDA)      │
  ├──────────────────────┼──────────────────────┼──────────────────────────┤
  │  Total acquisitions  │  31                  │  23 (documented)         │
  │  Total acreage       │  ~10,600             │  ~277,000+               │
  │  Distance clustering │  3.6-4.6 mi (100%)   │  1-100 mi (variable)     │
  │  Radius tested       │  5 miles             │  25 / 50 / 100 miles     │
  │  Shell companies     │  4 LLCs → 1 parent   │  12+ entities tracked    │
  │  CFIUS gap exploited │  5-10 mi gap         │  12 mi (Fufeng = real)   │
  │  Overpayment         │  15-30% above market │  Not systematically rptd │
  │  p-value (5-mi)      │  0.00003             │  N/A (different radius)  │
  │  Core finding        │  NON-RANDOM          │  Elevated proximity      │
  ├──────────────────────┴──────────────────────┴──────────────────────────┤
  │                                                                        │
  │  KEY INSIGHT: Marcus Webb's fictional methodology — spatial            │
  │  correlation of PE farmland acquisitions against military              │
  │  installations — maps directly onto a real analytical gap.             │
  │                                                                        │
  │  The USDA's own GAO audit (GAO-24-106337) found the agency            │
  │  cannot reliably determine where foreign-owned land is                 │
  │  relative to military sites. Marcus's Python script does               │
  │  in 18 seconds what USDA cannot do with paper forms.                   │
  │                                                                        │
  │  The regulatory gap Digital Harvest identifies — acquisitions          │
  │  positioned just outside CFIUS review thresholds — was real.           │
  │  Fufeng's 12-mile position was explicitly outside CFIUS                │
  │  jurisdiction until November 2024 rule expansion.                      │
  │                                                                        │
  └────────────────────────────────────────────────────────────────────────┘
    """)

    # Generate figures
    header("7. GENERATING FIGURES")
    fig_paths = generate_real_world_figures(acq, bases, distances, prox,
                                             mc_25, mc_50, script_dir)
    for p in fig_paths:
        print(f"  [SAVED] {os.path.basename(p)}")

    print(f"\n{DIVIDER}")
    print("  Analysis complete.")
    print(f"  All data sources are publicly available government records.")
    print(f"  www.digitalharvestbook.com")
    print(DIVIDER)


if __name__ == '__main__':
    main()
