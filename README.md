# AFIDA Spatial Analysis

**Monte Carlo Permutation Testing for Spatial Clustering of Foreign Agricultural Acquisitions Near U.S. Military Installations**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://www.python.org/downloads/)
[![Data: USDA Primary Source](https://img.shields.io/badge/data-USDA%20primary%20source-brightgreen.svg)]()

Companion code for: Green, R.J. (2026). "Spatial Clustering of Foreign Agricultural Acquisitions Near U.S. Military Installations: Comparative Evidence from USDA Primary Data." [SSRN](https://ssrn.com/author=10825096).

**Author:** Robert J. Green · [robert@rjgreenresearch.org](mailto:robert@rjgreenresearch.org) · [ORCID: 0009-0002-9097-1021](https://orcid.org/0009-0002-9097-1021) · [www.rjgreenresearch.org](https://www.rjgreenresearch.org)

---

## The Finding

Chinese-attributed agricultural holdings in the USDA's own AFIDA data cluster near U.S. military installations at **3.4× random expectation** at 50 miles (p < 0.001). Against nuclear-capable installations (ICBM fields, bomber bases, submarine bases, DOE weapons complex), enrichment reaches **12.7×**.

The critical insight is comparative: **all** foreign agricultural holdings exhibit some clustering (allied-nation average: 1.8×), reflecting the co-location of military sites and farmland in rural areas. But the Chinese differential — 3.4× versus the 1.8× allied baseline — cannot be explained by agricultural geography alone. Brazil, the null control, shows 1.0× (exactly random).

This is the first formal spatial hypothesis test applied to this policy-relevant question. Despite active federal legislation (the PASS Act), CFIUS expansion, and 26 state-level restrictions, no prior peer-reviewed study had tested whether the proximity pattern exceeds random chance.

---

## Quick Start

### Requirements

```bash
pip install numpy pandas openpyxl
```

### Reproduce Article 1 Results

```bash
# Download primary data (not included in repo — see Data Sources below)
# 1. AFIDA 2024 Excel from USDA FSA
# 2. County centroids from NOAA (c_16ap26.zip)

# Run the primary analysis
python webb_analysis.py \
    --afida AFIDACurrentHoldingsYR2024.xlsx \
    --centroids c_16ap26/ \
    --iterations 10000 \
    --seed 20260118

# Run with multi-country comparison
python webb_analysis.py \
    --afida AFIDACurrentHoldingsYR2024.xlsx \
    --centroids c_16ap26/ \
    --all-countries \
    --iterations 5000

# Run all four robustness checks
python robustness_checks.py \
    --afida AFIDACurrentHoldingsYR2024.xlsx \
    --centroids c_16ap26/
```

---

## Methodology

### Null Hypothesis

H₀: Chinese-attributed AFIDA holdings are distributed across CONUS counties with no spatial relationship to military installation locations.

### Monte Carlo Permutation Test

For each of N = 10,000 iterations, the script generates 123 random points uniformly within the CONUS bounding box (25°N–49°N, 125°W–66°W), computes Haversine distances to the nearest of 71 installations, and counts points within each threshold. The enrichment ratio is E(t) = C_obs / E[C_i].

### Multi-Threshold Analysis

| Threshold | Policy Relevance |
|-----------|-----------------|
| 10 miles | Close proximity |
| 25 miles | Operational surveillance range |
| 50 miles | Primary analysis threshold |
| 100 miles | CFIUS Tier 2 boundary |

### Robustness Specifications

1. **Installation subset sensitivity** — All 71 sites, military-only (50), nuclear-capable only (16)
2. **Acreage weighting** — log(1 + acres) weighted proximity fractions
3. **Agricultural land restriction** — Random points constrained to agricultural heartland
4. **Panel analysis** — 2022-2024 temporal comparison

---

## Primary Results

### Table 1: Multi-Threshold Spatial Correlation (China, n = 123 counties, N = 10,000)

| Threshold | Observed | E(H₀) | Enrichment | p-value | Result |
|-----------|----------|--------|------------|---------|--------|
| ≤ 10 mi | 2 | 0.46 | 4.3× | 0.077 | Not significant |
| ≤ 25 mi | 8 | 2.84 | 2.8× | 0.007 | Significant |
| ≤ 50 mi | 37 | 10.78 | 3.4× | < 0.001 | Reject H₀ |
| ≤ 100 mi | 81 | 36.34 | 2.2× | < 0.001 | Reject H₀ |

### Table 2: Multi-Country Comparison (50-mile threshold)

| Country | Category | Enrichment | p-value |
|---------|----------|------------|---------|
| China | Adversarial | 3.4× | < 0.001 |
| Saudi Arabia | Adversarial-adj. | 2.6× | 0.005 |
| Germany | Allied | 2.0× | < 0.001 |
| United Kingdom | Allied | 1.9× | < 0.001 |
| Canada | Allied | 1.8× | < 0.001 |
| **Allied average** | **Baseline** | **1.8×** | |
| Brazil | Americas (null) | 1.0× | 0.548 (NS) |

---

## Files

| File | Description |
|------|-------------|
| `webb_analysis.py` | Primary Monte Carlo analysis. CLI with configurable thresholds, iterations, seed, and multi-country mode |
| `afida_primary_analysis.py` | AFIDA data loading, geocoding, and distance computation pipeline |
| `robustness_checks.py` | Four robustness specifications |
| `installations_71.csv` | 71 CONUS military installations and critical infrastructure sites |
| `README.md` | This file |
| `CITATION.cff` | Machine-readable citation metadata |
| `LICENSE` | Apache 2.0 |

---

## Installation Database

71 CONUS sites across 15 categories:

| Category | Count | Examples |
|----------|-------|---------|
| ICBM Fields | 3 | Minot, Malmstrom, F.E. Warren |
| Bomber Bases | 4 | Whiteman, Barksdale, Dyess, Ellsworth |
| Submarine Bases | 2 | Kings Bay, Kitsap-Bangor |
| COCOM HQs | 3 | MacDill, Offutt, Peterson |
| Army | 10 | Fort Liberty, Fort Cavazos, Fort Stewart |
| Space Force | 5 | Buckley, Patrick, Schriever, Vandenberg, Peterson |
| DOE Nuclear | 7 | Los Alamos, Sandia, Pantex, Y-12, Savannah River, INL, Hanford |
| Intelligence | 4 | NSA Fort Meade, NGA, CIA Langley, DIA |
| Defense Industry | 5 | Newport News, Bath Iron Works, etc. |
| Other | 28 | ISR, fighter, naval, training, testing, depot |

---

## Data Sources

Data files are **not included** in this repository. Users download directly from the authoritative government sources:

| Data | Source | URL |
|------|--------|-----|
| AFIDA Holdings (2020-2024) | USDA Farm Service Agency | fsa.usda.gov/resources/economic-policy-analysis/afida |
| County Centroids | NOAA | weather.gov/gis/Counties |

This design ensures: (1) users always have the most current data, (2) no redistribution of government datasets, and (3) all results are independently reproducible from primary sources.

---

## Citation

```bibtex
@article{green_spatial_clustering_2026,
  author  = {Green, Robert J.},
  title   = {Spatial Clustering of Foreign Agricultural Acquisitions Near {U.S.} Military Installations: Comparative Evidence from {USDA} Primary Data},
  year    = {2026},
  journal = {SSRN Working Paper},
  url     = {https://ssrn.com/author=10825096}
}
```

---

## Related Projects

- [**secmap**](https://github.com/rjgreenresearch/secmap) — Beneficial ownership chain tracing through SEC EDGAR filings
- [**afida-parser**](https://github.com/rjgreenresearch/afida-parser) — AFIDA-to-SEC cross-reference and visibility gap analysis

---

## License

Apache 2.0. See [LICENSE](LICENSE).

*The methodology in this repository was initially developed for the novel [Digital Harvest](https://www.digitalharvestbook.com) (The Silent Conquest Series) and subsequently validated against federal primary-source data.*
