#!/usr/bin/env python3
"""
===============================================================================
PATTERN DETECTION SYSTEM (PDS) — Spatial Correlation Analysis
===============================================================================

Author:     Marcus J. Webb, ABD
            Department of Economics, MIT
            Dissertation: "Anomalous Capital Flows in Agricultural Sector
                           Acquisitions, 2020–2024"

Advisor:    Dr. Heinrich Voss, University of Chicago (PhD)
            MIT Sloan School of Management

Date:       January 15, 2025, 03:00 EST
Location:   Building E52, MIT, Cambridge, Massachusetts

Purpose:    Evaluate whether 31 private-equity agricultural land acquisitions
            exhibit statistically significant spatial clustering relative to
            U.S. strategic military installations.

Methods:    - Haversine geodesic distance computation
            - Nearest-neighbor spatial correlation (multi-threshold)
            - Moran's I spatial autocorrelation statistic
            - Monte Carlo permutation testing (n=10,000 iterations)
            - Temporal clustering analysis (Kolmogorov-Smirnov test)
            - Kernel Density Estimation for distance distribution

Data:       - USDA FIRREA foreign agricultural land ownership reports
            - SEC beneficial ownership filings
            - CFIUS pre-filing transaction records
            - GPS-verified parcel surveys (±3m accuracy)
            - DoD installation geographic reference data (DISDI)

Output:     Regression summary tables, p-values, confidence intervals,
            and publication-quality visualizations.

License:    Academic research — all methods reproducible per AEA guidelines.
===============================================================================

    FICTIONAL RESEARCH ARTIFACT — Digital Harvest by Robert J. Green
    www.digitalharvestbook.com

    This script accompanies Chapter 1 of Digital Harvest, the first book
    in The Silent Conquest series. The datasets, entities, and findings
    are fictional. The statistical methods are real and correctly applied.

===============================================================================
"""

import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import cdist
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import sys
import os

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

RANDOM_SEED = 20250115          # January 15, 2025 — the night Marcus found it
MC_ITERATIONS = 10_000          # Monte Carlo permutation count
DISTANCE_THRESHOLDS = [3, 5, 10]  # Miles — tested per manuscript methodology
PRIMARY_RADIUS = 5              # Primary analysis radius (miles)
CFIUS_TRIGGER = 10              # CFIUS mandatory review distance (miles)
ALPHA = 0.05                    # Significance level

# U.S. continental bounding box for random-site generation
US_LAT_RANGE = (25.0, 49.0)
US_LON_RANGE = (-125.0, -66.0)

np.random.seed(RANDOM_SEED)

# ============================================================================
# OUTPUT STYLING
# ============================================================================

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
# HAVERSINE DISTANCE (Miles)
# ============================================================================

def haversine(lat1, lon1, lat2, lon2):
    """
    Compute great-circle distance in miles between two GPS coordinates.
    Uses the Haversine formula — standard geodesic approximation for
    distances under 1,000 miles where ellipsoidal correction is negligible.
    """
    R = 3958.8  # Earth radius in miles (WGS-84 mean)
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def compute_distance_matrix(sites_df, bases_df):
    """
    Compute pairwise distance matrix between all acquisition sites
    and all military installations. Returns (n_sites × n_bases) matrix.
    """
    return haversine_matrix_vectorized(
        sites_df['latitude'].values.astype(float),
        sites_df['longitude'].values.astype(float),
        bases_df['latitude'].values.astype(float),
        bases_df['longitude'].values.astype(float)
    )

# ============================================================================
# SPATIAL CORRELATION ANALYSIS
# ============================================================================

def haversine_vectorized(lat1, lon1, lat2_arr, lon2_arr):
    """
    Vectorized haversine: single point (lat1, lon1) against arrays of points.
    Returns array of distances in miles.
    """
    R = 3958.8
    lat1_r = np.radians(lat1)
    lon1_r = np.radians(lon1)
    lat2_r = np.radians(lat2_arr)
    lon2_r = np.radians(lon2_arr)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = np.sin(dlat / 2)**2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def haversine_matrix_vectorized(lats1, lons1, lats2, lons2):
    """
    Fully vectorized pairwise haversine: (n_sites,) vs (n_bases,).
    Returns (n_sites × n_bases) distance matrix in miles.
    """
    R = 3958.8
    lat1 = np.radians(lats1[:, np.newaxis])
    lon1 = np.radians(lons1[:, np.newaxis])
    lat2 = np.radians(lats2[np.newaxis, :])
    lon2 = np.radians(lons2[np.newaxis, :])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def spatial_correlation(sites, military_bases, radius_miles=5):
    """
    Core spatial correlation function.
    Tests whether observed acquisition sites cluster near military
    installations at rates exceeding random spatial distribution.

    Methodology:
        1. Compute nearest-installation distance for each acquisition
        2. Count sites within specified radius
        3. Monte Carlo permutation test: generate n=10,000 random site
           distributions within CONUS, compute expected count under H₀
        4. Derive empirical p-value and confidence level

    Returns dict with p_value, confidence, interpretation, and diagnostics.
    """
    # Step 1: Compute distance matrix
    dist_matrix = compute_distance_matrix(sites, military_bases)
    min_distances = dist_matrix.min(axis=1)

    # Step 2: Observed count within radius
    observed_count = np.sum(min_distances <= radius_miles)
    n_sites = len(sites)

    # Step 3: Vectorized Monte Carlo permutation test
    base_lats = military_bases['latitude'].values.astype(float)
    base_lons = military_bases['longitude'].values.astype(float)
    mc_counts = np.zeros(MC_ITERATIONS)

    print(f"  Monte Carlo progress: ", end='', flush=True)
    for i in range(MC_ITERATIONS):
        if i % 2000 == 0:
            print(f"{i:,}...", end='', flush=True)
        rand_lats = np.random.uniform(US_LAT_RANGE[0], US_LAT_RANGE[1], n_sites)
        rand_lons = np.random.uniform(US_LON_RANGE[0], US_LON_RANGE[1], n_sites)

        # Vectorized: compute full distance matrix at once
        dm = haversine_matrix_vectorized(rand_lats, rand_lons, base_lats, base_lons)
        mc_counts[i] = np.sum(dm.min(axis=1) <= radius_miles)

    print(f"{MC_ITERATIONS:,}. Done.")

    # Step 4: Empirical p-value
    p_value = (np.sum(mc_counts >= observed_count) + 1) / (MC_ITERATIONS + 1)

    # Confidence level
    confidence = (1.0 - p_value) * 100.0

    # Interpretation
    if p_value < 0.001:
        interpretation = "NON-RANDOM"
    elif p_value < 0.01:
        interpretation = "HIGHLY SIGNIFICANT"
    elif p_value < 0.05:
        interpretation = "SIGNIFICANT"
    else:
        interpretation = "NOT SIGNIFICANT"

    return {
        'observed_count': observed_count,
        'n_sites': n_sites,
        'radius_miles': radius_miles,
        'p_value': p_value,
        'confidence': confidence,
        'interpretation': interpretation,
        'min_distances': min_distances,
        'mc_counts': mc_counts,
        'mc_mean': mc_counts.mean(),
        'mc_std': mc_counts.std(),
        'mc_max': mc_counts.max(),
    }

# ============================================================================
# MORAN'S I SPATIAL AUTOCORRELATION
# ============================================================================

def compute_morans_i(sites, military_bases, bandwidth=50.0):
    """
    Compute Moran's I statistic for spatial autocorrelation of
    acquisition proximity to military installations.

    Uses inverse-distance spatial weight matrix with specified bandwidth.
    Variable of interest: nearest-installation distance for each site.

    Moran's I ∈ [-1, 1]:
        I > 0 → positive spatial autocorrelation (clustering)
        I ≈ 0 → random spatial pattern
        I < 0 → spatial dispersion

    Statistical significance via z-score under normality assumption.
    """
    dist_matrix = compute_distance_matrix(sites, military_bases)
    min_distances = dist_matrix.min(axis=1)
    n = len(sites)

    # Spatial weight matrix (inverse distance between sites) — vectorized
    site_lats = sites['latitude'].values.astype(float)
    site_lons = sites['longitude'].values.astype(float)
    site_dists = haversine_matrix_vectorized(site_lats, site_lons, site_lats, site_lons)

    W = np.zeros((n, n))
    mask = (site_dists > 0) & (site_dists <= bandwidth)
    W[mask] = 1.0 / np.maximum(site_dists[mask], 0.1)

    # Row-standardize
    row_sums = W.sum(axis=1)
    row_sums[row_sums == 0] = 1.0
    W = W / row_sums[:, np.newaxis]

    # Moran's I
    z = min_distances - min_distances.mean()
    numerator = n * np.dot(z, np.dot(W, z))
    denominator = W.sum() * np.dot(z, z)
    I = numerator / denominator if denominator != 0 else 0

    # Expected value under H₀
    E_I = -1.0 / (n - 1)

    # Variance (normality assumption)
    S0 = W.sum()
    S1 = 0.5 * np.sum((W + W.T)**2)
    S2 = np.sum((W.sum(axis=1) + W.sum(axis=0))**2)
    D = n * np.sum(z**4) / (np.sum(z**2)**2)

    var_I_num = (n * ((n**2 - 3*n + 3) * S1 - n * S2 + 3 * S0**2) -
                 D * (n * (n - 1) * S1 - 2 * n * S2 + 6 * S0**2))
    var_I_den = (n - 1) * (n - 2) * (n - 3) * S0**2
    var_I = var_I_num / var_I_den - E_I**2 if var_I_den != 0 else 1.0
    var_I = max(var_I, 1e-10)

    z_score = (I - E_I) / np.sqrt(var_I)
    p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))

    return {
        'morans_i': I,
        'expected_i': E_I,
        'variance': var_I,
        'z_score': z_score,
        'p_value': p_value,
        'significant': p_value < ALPHA,
    }

# ============================================================================
# TEMPORAL CLUSTERING ANALYSIS
# ============================================================================

def temporal_clustering(sites):
    """
    Test whether acquisition dates exhibit temporal clustering
    beyond what would be expected from a uniform distribution over
    the observed time window (April 2022 – March 2024).

    Uses Kolmogorov-Smirnov test against uniform CDF.
    """
    dates = pd.to_datetime(sites['acquisition_date'])
    t_min = dates.min()
    t_max = dates.max()
    total_days = (t_max - t_min).days

    # Normalize to [0, 1]
    normalized = np.array([(d - t_min).days / total_days for d in dates])
    normalized.sort()

    # KS test against uniform
    ks_stat, ks_p = stats.kstest(normalized, 'uniform')

    # Quarterly binning for chi-square
    quarters = dates.dt.to_period('Q')
    quarter_counts = quarters.value_counts().sort_index()
    n_quarters = len(quarter_counts)
    expected_per_q = len(sites) / n_quarters
    chi2, chi2_p = stats.chisquare(quarter_counts.values,
                                     f_exp=[expected_per_q] * n_quarters)

    # Inter-arrival times
    sorted_dates = np.sort(dates.values)
    inter_arrival = np.diff(sorted_dates).astype('timedelta64[D]').astype(int)

    return {
        'ks_statistic': ks_stat,
        'ks_p_value': ks_p,
        'chi2_statistic': chi2,
        'chi2_p_value': chi2_p,
        'n_quarters': n_quarters,
        'quarter_counts': quarter_counts,
        'mean_inter_arrival_days': inter_arrival.mean(),
        'std_inter_arrival_days': inter_arrival.std(),
        'acquisition_window': f"{t_min.strftime('%B %Y')} – {t_max.strftime('%B %Y')}",
    }

# ============================================================================
# FINANCIAL ANOMALY SUMMARY
# ============================================================================

def financial_summary(sites):
    """
    Compute aggregate financial statistics for the acquisition portfolio.
    """
    total_investment = sites['purchase_price_usd'].sum()
    total_market = sites['est_market_value_usd'].sum()
    total_overpayment = total_investment - total_market
    mean_overpayment_pct = sites['overpayment_pct'].mean()
    total_acreage = sites['acreage'].sum()

    entities = sites['acquiring_entity'].nunique()
    shells_1 = sites['parent_shell_1'].nunique()
    shells_2 = sites['parent_shell_2'].nunique()
    ultimate = sites['ultimate_parent'].nunique()

    return {
        'total_investment': total_investment,
        'total_market_value': total_market,
        'total_overpayment': total_overpayment,
        'mean_overpayment_pct': mean_overpayment_pct,
        'total_acreage': total_acreage,
        'n_acquiring_entities': entities,
        'n_cayman_shells': shells_1,
        'n_intermediate_shells': shells_2,
        'n_ultimate_parents': ultimate,
    }

# ============================================================================
# MULTI-THRESHOLD ANALYSIS
# ============================================================================

def multi_threshold_analysis(sites, bases, thresholds=[3, 5, 10]):
    """
    Run spatial correlation at multiple distance thresholds.
    Tests whether clustering strengthens at specific distances,
    which may indicate deliberate positioning relative to CFIUS
    review triggers.
    """
    results = {}
    dist_matrix = compute_distance_matrix(sites, bases)
    min_distances = dist_matrix.min(axis=1)
    base_lats = bases['latitude'].values.astype(float)
    base_lons = bases['longitude'].values.astype(float)
    n_sites = len(sites)

    # Single MC run, evaluate all thresholds
    max_t = max(thresholds)
    mc_min_dists = np.zeros((1000, n_sites))
    for i in range(1000):
        rand_lats = np.random.uniform(US_LAT_RANGE[0], US_LAT_RANGE[1], n_sites)
        rand_lons = np.random.uniform(US_LON_RANGE[0], US_LON_RANGE[1], n_sites)
        dm = haversine_matrix_vectorized(rand_lats, rand_lons, base_lats, base_lons)
        mc_min_dists[i] = dm.min(axis=1)

    for r in thresholds:
        count = np.sum(min_distances <= r)
        pct = count / n_sites * 100
        mc_counts = np.sum(mc_min_dists <= r, axis=1)
        expected = mc_counts.mean()
        ratio = count / expected if expected > 0 else float('inf')

        results[r] = {
            'observed': count,
            'expected_random': expected,
            'enrichment_ratio': ratio,
            'pct_within': pct,
        }

    return results

# ============================================================================
# VISUALIZATION
# ============================================================================

def generate_figures(sites, bases, sc_result, morans, temporal, mt_results,
                     output_dir='/home/claude'):
    """
    Generate publication-quality figures for the analysis.
    """
    sns.set_theme(style='whitegrid', font_scale=1.1)
    fig_paths = []

    # --- Figure 1: Distance Distribution with KDE ---
    fig, ax = plt.subplots(figsize=(10, 6))
    distances = sc_result['min_distances']
    ax.hist(distances, bins=15, density=True, alpha=0.6, color='#2c5f8a',
            edgecolor='white', linewidth=0.8, label='Observed acquisitions')
    kde_x = np.linspace(0, max(distances) * 1.2, 300)
    kde = stats.gaussian_kde(distances)
    ax.plot(kde_x, kde(kde_x), color='#c0392b', linewidth=2.5,
            label='Kernel density estimate')
    ax.axvline(x=PRIMARY_RADIUS, color='#e67e22', linewidth=2, linestyle='--',
               label=f'{PRIMARY_RADIUS}-mile threshold')
    ax.axvline(x=CFIUS_TRIGGER, color='#7f8c8d', linewidth=1.5, linestyle=':',
               label=f'{CFIUS_TRIGGER}-mile CFIUS trigger')
    ax.set_xlabel('Distance to Nearest Military Installation (miles)', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title('Distribution of Acquisition-to-Installation Distances\n'
                 'Foreign PE Farmland Acquisitions, April 2022 – March 2024',
                 fontsize=13, fontweight='bold')
    ax.legend(frameon=True, fancybox=True, shadow=True)
    ax.set_xlim(0, max(distances) * 1.3)
    plt.tight_layout()
    path1 = os.path.join(output_dir, 'fig1_distance_distribution.png')
    fig.savefig(path1, dpi=200, bbox_inches='tight')
    fig_paths.append(path1)
    plt.close()

    # --- Figure 2: Monte Carlo Null Distribution vs Observed ---
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(sc_result['mc_counts'], bins=range(int(sc_result['mc_counts'].max()) + 3),
            density=True, alpha=0.6, color='#95a5a6', edgecolor='white',
            linewidth=0.8, label=f'H₀ distribution (n={MC_ITERATIONS:,} simulations)')
    ax.axvline(x=sc_result['observed_count'], color='#c0392b', linewidth=3,
               linestyle='-', label=f'Observed: {sc_result["observed_count"]} sites')
    ax.axvline(x=sc_result['mc_mean'], color='#2c5f8a', linewidth=2,
               linestyle='--', label=f'Expected under H₀: {sc_result["mc_mean"]:.1f}')

    # Shade rejection region
    x_crit = sc_result['observed_count']
    bins_vals, bins_edges, patches = ax.hist(
        sc_result['mc_counts'],
        bins=range(int(sc_result['mc_counts'].max()) + 3),
        density=True, alpha=0)
    for patch, left_edge in zip(patches, bins_edges[:-1]):
        if left_edge >= x_crit:
            patch.set_facecolor('#c0392b')
            patch.set_alpha(0.3)
            patch.set_visible(True)

    ax.set_xlabel(f'Number of Sites Within {PRIMARY_RADIUS} Miles of Installation', fontsize=12)
    ax.set_ylabel('Probability Density', fontsize=12)
    ax.set_title(f'Monte Carlo Permutation Test: Observed vs. Random Clustering\n'
                 f'p-value = {sc_result["p_value"]:.5f}  |  '
                 f'Confidence: {sc_result["confidence"]:.3f}%',
                 fontsize=13, fontweight='bold')
    ax.legend(frameon=True, fancybox=True, shadow=True, loc='upper right')
    plt.tight_layout()
    path2 = os.path.join(output_dir, 'fig2_monte_carlo_null.png')
    fig.savefig(path2, dpi=200, bbox_inches='tight')
    fig_paths.append(path2)
    plt.close()

    # --- Figure 3: Multi-Threshold Enrichment ---
    fig, ax = plt.subplots(figsize=(9, 6))
    thresholds = sorted(mt_results.keys())
    enrichment = [mt_results[t]['enrichment_ratio'] for t in thresholds]
    observed = [mt_results[t]['observed'] for t in thresholds]
    expected = [mt_results[t]['expected_random'] for t in thresholds]

    x = np.arange(len(thresholds))
    width = 0.3
    bars1 = ax.bar(x - width/2, observed, width, label='Observed', color='#c0392b',
                    edgecolor='white', linewidth=0.8)
    bars2 = ax.bar(x + width/2, expected, width, label='Expected (random)',
                    color='#95a5a6', edgecolor='white', linewidth=0.8)

    # Add enrichment ratio annotations
    for i, (t, e) in enumerate(zip(thresholds, enrichment)):
        ax.annotate(f'{e:.0f}×', xy=(x[i], max(observed[i], expected[i])),
                    xytext=(0, 12), textcoords='offset points',
                    ha='center', fontsize=12, fontweight='bold', color='#2c3e50')

    ax.set_xlabel('Distance Threshold (miles)', fontsize=12)
    ax.set_ylabel('Number of Sites', fontsize=12)
    ax.set_title('Spatial Enrichment Across Distance Thresholds\n'
                 'Observed Acquisitions vs. Random Expectation',
                 fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{t} mi' for t in thresholds])
    ax.legend(frameon=True, fancybox=True, shadow=True)
    ax.axhline(y=0, color='black', linewidth=0.5)
    plt.tight_layout()
    path3 = os.path.join(output_dir, 'fig3_multi_threshold.png')
    fig.savefig(path3, dpi=200, bbox_inches='tight')
    fig_paths.append(path3)
    plt.close()

    # --- Figure 4: Temporal Distribution ---
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    dates = pd.to_datetime(sites['acquisition_date'])
    ax1.hist(dates, bins=24, color='#2c5f8a', edgecolor='white',
             linewidth=0.8, alpha=0.8)
    ax1.set_xlabel('Acquisition Date', fontsize=11)
    ax1.set_ylabel('Count', fontsize=11)
    ax1.set_title('Temporal Distribution of Acquisitions', fontsize=12, fontweight='bold')
    ax1.tick_params(axis='x', rotation=45)

    # Overpayment by entity
    entity_means = sites.groupby('acquiring_entity')['overpayment_pct'].mean().sort_values()
    colors = ['#2c5f8a', '#c0392b', '#e67e22', '#27ae60', '#8e44ad']
    short_names = [name.replace(' LLC', '').replace(' Group', '')[:25] for name in entity_means.index]
    bars = ax2.barh(short_names, entity_means.values, color=colors[:len(entity_means)],
                     edgecolor='white', linewidth=0.8)
    ax2.set_xlabel('Mean Overpayment (%)', fontsize=11)
    ax2.set_title('Overpayment by Acquiring Entity', fontsize=12, fontweight='bold')
    ax2.axvline(x=sites['overpayment_pct'].mean(), color='#c0392b',
                linestyle='--', linewidth=1.5, label='Portfolio mean')
    ax2.legend(frameon=True)

    plt.tight_layout()
    path4 = os.path.join(output_dir, 'fig4_temporal_financial.png')
    fig.savefig(path4, dpi=200, bbox_inches='tight')
    fig_paths.append(path4)
    plt.close()

    return fig_paths


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print(DIVIDER)
    print("  PATTERN DETECTION SYSTEM (PDS) v2.4")
    print("  Spatial Correlation Analysis — Agricultural Sector Acquisitions")
    print(f"  Executed: January 15, 2025, 03:00 EST")
    print(f"  Analyst:  Marcus J. Webb, ABD — MIT Department of Economics")
    print(DIVIDER)

    # ------------------------------------------------------------------
    # LOAD DATA
    # ------------------------------------------------------------------
    header("1. DATA INGESTION")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    sites_path = os.path.join(script_dir, 'pe_farmsite_acquisitions.csv')
    bases_path = os.path.join(script_dir, 'military_installations.csv')

    sites = pd.read_csv(sites_path)
    bases = pd.read_csv(bases_path)

    print(f"\n  Acquisition sites loaded:       {len(sites)}")
    print(f"  Military installations loaded:  {len(bases)}")
    print(f"  Acquisition window:             {sites['acquisition_date'].min()} to "
          f"{sites['acquisition_date'].max()}")
    print(f"  States represented:             {sites['state'].nunique()}")

    # ------------------------------------------------------------------
    # FINANCIAL SUMMARY
    # ------------------------------------------------------------------
    header("2. FINANCIAL ANOMALY SUMMARY")

    fin = financial_summary(sites)
    print(f"""
  Total portfolio investment:       ${fin['total_investment']:>15,.0f}
  Estimated aggregate market value: ${fin['total_market_value']:>15,.0f}
  Total overpayment (loss):         ${fin['total_overpayment']:>15,.0f}
  Mean overpayment:                 {fin['mean_overpayment_pct']:>14.1f}%
  Total acreage acquired:           {fin['total_acreage']:>15,} acres

  Acquiring entities (U.S. LLCs):   {fin['n_acquiring_entities']}
  Cayman Islands shell entities:    {fin['n_cayman_shells']}
  Intermediate jurisdiction shells: {fin['n_intermediate_shells']}
  Ultimate parent entities:         {fin['n_ultimate_parents']}
    """)

    # ------------------------------------------------------------------
    # PRIMARY SPATIAL CORRELATION
    # ------------------------------------------------------------------
    header("3. SPATIAL CORRELATION ANALYSIS")
    print(f"\n  Running primary analysis at {PRIMARY_RADIUS}-mile radius...")
    print(f"  Monte Carlo iterations: {MC_ITERATIONS:,}")
    print(f"  Random seed: {RANDOM_SEED}")
    print(f"  Generating CONUS-wide random distributions...")

    sc = spatial_correlation(sites, bases, radius_miles=PRIMARY_RADIUS)

    # ---- THE OUTPUT FROM THE MANUSCRIPT ----
    subheader("PRIMARY RESULT")
    print(f"""
  >>> spatial_correlation(sites, military_bases, radius_miles={PRIMARY_RADIUS})

  >>> p_value:        {sc['p_value']:.5f}
  >>> confidence:     {sc['confidence']:.3f}%
  >>> interpretation: {sc['interpretation']}

  Sites within {PRIMARY_RADIUS} miles:  {sc['observed_count']} / {sc['n_sites']}  ({sc['observed_count']/sc['n_sites']*100:.1f}%)
  Expected under H₀:    {sc['mc_mean']:.2f}  (σ = {sc['mc_std']:.2f})
  Maximum under H₀:     {int(sc['mc_max'])}

  Enrichment ratio:      {sc['observed_count']/sc['mc_mean']:.1f}× expected
    """)

    # ------------------------------------------------------------------
    # MORAN'S I
    # ------------------------------------------------------------------
    header("4. MORAN'S I SPATIAL AUTOCORRELATION")
    print(f"\n  Computing spatial weight matrix (inverse-distance, 50-mi bandwidth)...")

    mi = compute_morans_i(sites, bases, bandwidth=50.0)

    print(f"""
  Moran's I statistic:   {mi['morans_i']:>10.4f}
  Expected I (H₀):       {mi['expected_i']:>10.4f}
  Variance:              {mi['variance']:>10.6f}
  Z-score:               {mi['z_score']:>10.4f}
  P-value (two-tailed):  {mi['p_value']:>10.6f}
  Significant (α=0.05):  {'YES — positive spatial autocorrelation' if mi['significant'] else 'NO'}
    """)

    # ------------------------------------------------------------------
    # MULTI-THRESHOLD ANALYSIS
    # ------------------------------------------------------------------
    header("5. MULTI-THRESHOLD DISTANCE ANALYSIS")
    print(f"\n  Testing thresholds: {DISTANCE_THRESHOLDS} miles")
    print(f"  CFIUS mandatory review trigger: {CFIUS_TRIGGER} miles")
    print(f"  Running 1,000-iteration MC per threshold...")

    mt = multi_threshold_analysis(sites, bases, DISTANCE_THRESHOLDS)

    print(f"\n  {'Threshold':>12}  {'Observed':>10}  {'Expected':>10}  {'Enrichment':>12}  {'% Within':>10}")
    print(f"  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*12}  {'-'*10}")
    for t in sorted(mt.keys()):
        r = mt[t]
        print(f"  {t:>10} mi  {r['observed']:>10}  {r['expected_random']:>10.1f}  "
              f"{r['enrichment_ratio']:>11.1f}×  {r['pct_within']:>9.1f}%")

    print(f"""
  NOTE: Correlation strengthens at {PRIMARY_RADIUS} miles, suggesting deliberate
  positioning to stay below the {CFIUS_TRIGGER}-mile CFIUS review trigger while
  maximizing proximity to strategic installations.
    """)

    # ------------------------------------------------------------------
    # TEMPORAL CLUSTERING
    # ------------------------------------------------------------------
    header("6. TEMPORAL CLUSTERING ANALYSIS")

    tc = temporal_clustering(sites)

    print(f"""
  Acquisition window:               {tc['acquisition_window']}
  Mean inter-arrival time:          {tc['mean_inter_arrival_days']:.1f} days
  Std dev inter-arrival:            {tc['std_inter_arrival_days']:.1f} days

  Kolmogorov-Smirnov test (H₀: uniform temporal distribution):
    KS statistic:                   {tc['ks_statistic']:.4f}
    P-value:                        {tc['ks_p_value']:.4f}
    Result:                         {'Reject H₀ — non-uniform' if tc['ks_p_value'] < ALPHA else 'Fail to reject H₀'}

  Chi-square test (quarterly bins):
    χ² statistic:                   {tc['chi2_statistic']:.4f}
    P-value:                        {tc['chi2_p_value']:.4f}
    Quarters observed:              {tc['n_quarters']}
    """)

    # ------------------------------------------------------------------
    # DISTANCE STATISTICS TABLE
    # ------------------------------------------------------------------
    header("7. INDIVIDUAL SITE DISTANCE AUDIT")

    dist_matrix = compute_distance_matrix(sites, bases)
    sites_audit = sites.copy()
    sites_audit['nearest_distance_mi'] = dist_matrix.min(axis=1)
    nearest_idx = dist_matrix.argmin(axis=1)
    sites_audit['nearest_base'] = [bases.iloc[j]['installation_name'] for j in nearest_idx]

    print(f"\n  {'ID':>8}  {'State':>5}  {'Dist (mi)':>10}  {'Nearest Installation':<40}  {'Entity':<30}")
    print(f"  {'-'*8}  {'-'*5}  {'-'*10}  {'-'*40}  {'-'*30}")
    for _, row in sites_audit.iterrows():
        entity_short = row['acquiring_entity'].replace(' LLC', '')[:28]
        print(f"  {row['acquisition_id']:>8}  {row['state']:>5}  "
              f"{row['nearest_distance_mi']:>9.2f}   {row['nearest_base']:<40}  {entity_short:<30}")

    summary_stats = sites_audit['nearest_distance_mi'].describe()
    print(f"\n  Distance summary statistics:")
    print(f"    Mean:    {summary_stats['mean']:.2f} miles")
    print(f"    Median:  {summary_stats['50%']:.2f} miles")
    print(f"    Std dev: {summary_stats['std']:.2f} miles")
    print(f"    Min:     {summary_stats['min']:.2f} miles")
    print(f"    Max:     {summary_stats['max']:.2f} miles")
    print(f"    Range:   {summary_stats['max'] - summary_stats['min']:.2f} miles")

    # ------------------------------------------------------------------
    # GENERATE FIGURES
    # ------------------------------------------------------------------
    header("8. GENERATING FIGURES")
    print(f"\n  Output directory: {script_dir}")

    fig_paths = generate_figures(sites, bases, sc, mi, tc, mt,
                                 output_dir=script_dir)
    for p in fig_paths:
        print(f"  [SAVED] {os.path.basename(p)}")

    # ------------------------------------------------------------------
    # FINAL ASSESSMENT
    # ------------------------------------------------------------------
    header("9. CONSOLIDATED ASSESSMENT")

    print(f"""
  ┌──────────────────────────────────────────────────────────────────────┐
  │                      PATTERN DETECTION SYSTEM                       │
  │                       CONSOLIDATED FINDINGS                         │
  ├──────────────────────────────────────────────────────────────────────┤
  │                                                                      │
  │  Spatial Correlation ({PRIMARY_RADIUS}-mi):                                       │
  │    p-value:      {sc['p_value']:<12.5f}                                    │
  │    Confidence:   {sc['confidence']:<12.3f}%                                   │
  │    Result:       {sc['interpretation']:<12}                                    │
  │                                                                      │
  │  Moran's I:                                                          │
  │    I-statistic:  {mi['morans_i']:<12.4f}   (positive autocorrelation)      │
  │    Z-score:      {mi['z_score']:<12.4f}                                    │
  │    p-value:      {mi['p_value']:<12.6f}                                    │
  │                                                                      │
  │  Financial Anomaly:                                                  │
  │    Overpayment:  ${fin['total_overpayment']/1e6:<10.1f}M  ({fin['mean_overpayment_pct']:.1f}% avg above market)    │
  │    Entities:     {fin['n_acquiring_entities']} U.S. LLCs → {fin['n_cayman_shells']} Cayman shells → {fin['n_ultimate_parents']} parent       │
  │                                                                      │
  │  CFIUS Positioning:                                                  │
  │    100% of acquisitions fall BELOW {CFIUS_TRIGGER}-mile review trigger       │
  │    Enrichment peaks at {PRIMARY_RADIUS} miles — consistent with deliberate   │
  │    proximity maximization under regulatory avoidance                  │
  │                                                                      │
  ├──────────────────────────────────────────────────────────────────────┤
  │                                                                      │
  │  CONCLUSION: This pattern is statistically impossible unless          │
  │              it is intentional.                                       │
  │                                                                      │
  └──────────────────────────────────────────────────────────────────────┘
    """)

    print(DIVIDER)
    print("  Analysis complete.")
    print(f"  Runtime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Figures saved to: {script_dir}")
    print(DIVIDER)


if __name__ == '__main__':
    main()
