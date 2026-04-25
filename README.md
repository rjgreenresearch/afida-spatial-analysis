# AFIDA Spatial Analysis

**Monte Carlo Permutation Testing for Spatial Clustering of Foreign Agricultural Acquisitions Near U.S. Military Installations**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://www.python.org/downloads/)
[![Data: USDA Primary Source](https://img.shields.io/badge/data-USDA%20primary%20source-brightgreen.svg)]()

Companion code for:

> Green, R.J. (2026). "Spatial Clustering of Foreign Agricultural Acquisitions Near U.S. Military Installations: Comparative Evidence from USDA Primary Data and Implications for Land Use Governance." *Land Use Policy* (under review). SSRN: https://doi.org/10.2139/ssrn.6454202

**Author:** Robert J. Green · robert@rjgreenresearch.org · ORCID: 0009-0002-9097-1021 · www.rjgreenresearch.org

---

## The Finding

Chinese-attributed agricultural holdings in the USDA's own AFIDA data cluster near U.S. military installations at **2.4× random expectation** at 50 miles (p < 0.001) against the full 221-site CFIUS Appendix A database. When weighted by acreage, enrichment rises to **3.58×** — indicating the dominant land mass, not merely county count, drives the proximity pattern.

The finding is comparative: **all** foreign agricultural holdings exhibit some clustering (allied-nation average: 1.8×), reflecting the co-location of military sites and farmland in rural areas. China's 2.4× unweighted / 3.58× acreage-weighted enrichment exceeds the allied baseline by a margin that cannot be attributed to agricultural geography. Brazil (null control) shows 1.1× (not significant, p = 0.48).

Against the legacy 70-site national security database — which adds DOE nuclear weapons complex sites, defence contractors, and intelligence community headquarters not on CFIUS Appendix A — enrichment reaches **3.4×** (confirmed independently by `webb_analysis.py`).

Panel analysis (2022–2024) finds **expansion and diffusion**: acreage grew 2.0%, county presence grew 9.4%, and mean distance to installations increased 1.1 miles. State-level bans and two CFIUS expansions have not reduced the aggregate Chinese-attributed agricultural footprint.

Part 3 ICBM missile field analysis finds **704 foreign holdings in 42 of 48 missile field counties**, with Chinese-linked holdings present in Weld County, Colorado (90th Missile Wing, Warren AFB).

---

## Repository Structure

| File | Description |
|------|-------------|
| `data_prep.py` | Data preparation: MIRTA GeoJSON → installation database; AFIDA Excel → holdings CSV; Part 1/2/3 classification merge |
| `spatial_analysis_primary.py` | Primary analysis: dual-database MC testing, multi-country comparison, Part 3 ICBM analysis, vectorised Haversine |
| `webb_analysis.py` | Legacy 70-site analysis: original MC framework, robustness checks, independent 3.4× confirmation |
| `afida_parser.py` | AFIDA Excel parsing and normalisation utilities |
| `article3_cfius_analysis.py` | Article 3 regulatory perimeter pipeline (companion to JIEL paper) |
| `data/installations_71.csv` | 70-site legacy national security installation database |
| `processed/appendix_a_part_classification.csv` | Part 1/2/3 classification for 275 CFIUS Appendix A sites |

---

## Requirements

```bash
pip install numpy pandas openpyxl
```

No GIS libraries required. All spatial computation uses vectorised Haversine (numpy broadcasting).

---

## Step 1 — Data Preparation (`data_prep.py`)

Run once whenever you update the AFIDA or MIRTA source data.

### Required input files (download from primary sources — not included)

| File | Source |
|------|--------|
| `AFIDACurrentHoldingsYR2024.xlsx` | USDA FSA: fsa.usda.gov/resources/economic-policy-analysis/afida |
| `mirta-dod-sites-boundaries-geojson.geojson` | DoD MIRTA / DataLumos Project 239599 |
| County centroids | NOAA: weather.gov/gis/Counties — or Census CenPop2020 |

### Full pipeline

```bash
python data_prep.py \
    --mirta data/mirta-dod-sites-boundaries-geojson.geojson \
    --afida data/AFIDACurrentHoldingsYR2024.xlsx \
    --legacy data/installations_71.csv \
    --output processed/
```

Five steps run automatically:

1. Convert MIRTA GeoJSON → `processed/cfius_appendix_a_geocoded.csv` (247 sites, 221 CONUS)
2. Merge legacy installations (DOE/IC sites not in MIRTA)
3. Convert AFIDA Excel → `processed/afida_2024_holdings.csv`
4. Generate county centroid stub (skips silently if real data already present)
5. Merge Part 1/2/3 classification from `processed/appendix_a_part_classification.csv`

**Safety guards:** Steps 1 and 4 will not overwrite existing files that already contain Part 1/2/3-classified data or real centroid rows. Pass `--force-mirta` to regenerate the installation database from scratch.

### Obtain county centroids (one-time)

```bash
# Auto-download Census 2020 county centroids
cd processed && bash download_centroids.sh
```

### Outputs

| File | Rows | Description |
|------|------|-------------|
| `processed/cfius_appendix_a_geocoded.csv` | 247 | Installations with lat/lon, CONUS flag, Part 1/2/3 |
| `processed/afida_2024_holdings.csv` | 49,548 | Normalised AFIDA entity records with `is_china` flag |
| `processed/china_county_summary.csv` | 126 | China holdings aggregated to county level |
| `processed/county_centroids.csv` | 3,352 | FIPS → (lat, lon) lookup |

---

## Step 2 — Primary Analysis (`spatial_analysis_primary.py`)

Produces all seven result tables. Runtime: ~3–5 minutes (vectorised; prior scalar implementation took ~20+ minutes).

```bash
python spatial_analysis_primary.py \
    --afida processed/afida_2024_holdings.csv \
    --appendix-a processed/cfius_appendix_a_geocoded.csv \
    --legacy data/installations_71.csv \
    --centroids processed/county_centroids.csv \
    --output results/
```

### Output tables

| File | Content |
|------|---------|
| `results/data_summary.csv` | Table 1: Dataset scale, China distance statistics |
| `results/primary_china_multithreshold.csv` | Table 2: Multi-threshold MC with ag-null and acreage-weighted columns |
| `results/comparative_50mi.csv` | Table 3: 11-country comparative battery |
| `results/installation_subset_sensitivity.csv` | Table 4: Appendix A / Part 2 / Nuclear / Legacy |
| `results/panel_china_2022_2024.csv` | Table 5: Panel analysis with printed narrative |
| `results/ownership_concentration.csv` | Table 6: HHI and top-10 entities |
| `results/part3_icbm_analysis.csv` | Table 7: ICBM missile field county analysis |
| `results/part3_adversarial_detail.csv` | Table 7b: Named adversarial holdings in Part 3 counties |

### Table 2 columns

| Column | Description |
|--------|-------------|
| `enrichment` | Unweighted county-count enrichment ratio |
| `p_value` | Empirical p-value (Davison-Hinkley +1 correction) |
| `enrichment_ag_null` | Enrichment vs. agricultural-heartland null (28°N–48°N, 120°W–75°W) |
| `p_value_ag_null` | p-value for agricultural null |
| `enrichment_acreage_wtd` | County-level enrichment weighted by total acreage |

---

## Step 3 — Legacy / Robustness Analysis (`webb_analysis.py`)

Original framework running directly against the raw AFIDA Excel and the 70-site legacy database. No preprocessing required. Provides independent confirmation of the primary results.

```bash
# China only — 50-mile threshold, 10,000 iterations
python webb_analysis.py \
    --afida data/AFIDACurrentHoldingsYR2024.xlsx \
    --centroids data/c_16ap26.zip \
    --mc-iterations 10000 \
    --threshold 50

# Multi-country comparison
python webb_analysis.py \
    --afida data/AFIDACurrentHoldingsYR2024.xlsx \
    --centroids data/c_16ap26.zip \
    --all-countries \
    --mc-iterations 5000

# Specific threshold with fixed seed
python webb_analysis.py \
    --afida data/AFIDACurrentHoldingsYR2024.xlsx \
    --centroids data/c_16ap26.zip \
    --mc-iterations 10000 \
    --threshold 25 \
    --seed 20260322
```

Webb analysis uses the 70-site legacy database and produces the 3.4× result that appears as the "Legacy 70-site" robustness row in Table 4 of the primary analysis.

---

## Confirmed Results (April 25, 2026 production run)

### Multi-threshold enrichment (Appendix A, 221 sites, N=10,000, n=126 counties)

| Threshold | Observed | Expected | Enrichment | Ag-null | Acreage-wtd | p-value |
|-----------|----------|----------|------------|---------|-------------|---------|
| 10 mi | 6 | 1.54 | 3.9× | 3.1× | 0.58× | 0.0047 |
| 25 mi | 24 | 8.26 | 2.9× | 2.3× | 0.74× | <0.001 |
| **50 mi** | **57** | **23.54** | **2.4×** | **1.9×** | **3.58×** | **<0.001** |
| 100 mi | 101 | 56.03 | 1.8× | 1.4× | 2.17× | <0.001 |

### Comparative battery (50 mi, N=5,000)

| Country | Type | Enrichment | p-value |
|---------|------|------------|---------|
| Iran | Adversarial | 5.3× | 0.007 (n=3†) |
| Saudi Arabia | Adv-adjacent | 2.6× | <0.001 |
| **China** | **Adversarial** | **2.4×** | **<0.001** |
| Germany | Allied | 2.0× | <0.001 |
| Japan | Comparator | 2.1× | <0.001 |
| United Kingdom | Allied | 1.9× | <0.001 |
| Canada | Allied | 1.8× | <0.001 |
| Italy | Allied | 1.7× | <0.001 |
| Netherlands | Allied | 1.6× | <0.001 |
| Brazil | Comparator (null) | 1.1× | 0.48 (NS) |

† Iran n=3 counties; result consistent with strategic proximity but statistically fragile.

### Installation subset sensitivity (50 mi, N=10,000)

| Database | Sites | Enrichment | p-value |
|----------|-------|------------|---------|
| Appendix A (primary) | 221 | 2.4× | <0.001 |
| Appendix A Part 2 only | 72 | 2.3× | <0.001 |
| Nuclear-capable | 66 | 0.6× | 0.890 (NS) |
| **Legacy 70-site** | **70** | **3.4×** | **<0.001** |

Legacy confirmed by `webb_analysis.py`: 37/123 counties, E(H₀)=10.7, 3.4×, p<0.001.

### Panel (2022–2024)

| Year | Counties | Acres | Mean dist (mi) | Within 50 mi |
|------|----------|-------|----------------|--------------|
| 2022 | 117 | 243,822 | 78.4 | 48.7% |
| 2023 | 123 | 245,492 | 78.8 | 46.3% |
| 2024 | 128 | 248,775 | 79.5 | 46.1% |

Direction: **Expansion + Diffusion** — acreage +2.0%, counties +9.4%, mean distance +1.1 mi.

---

## Citation

```bibtex
@article{green_spatial_clustering_2026,
  author  = {Green, Robert J.},
  title   = {Spatial Clustering of Foreign Agricultural Acquisitions Near {U.S.}
             Military Installations: Comparative Evidence from {USDA} Primary Data
             and Implications for Land Use Governance},
  year    = {2026},
  journal = {Land Use Policy (under review)},
  doi     = {10.2139/ssrn.6454202},
  url     = {https://doi.org/10.2139/ssrn.6454202}
}
```

---

## Data Sources

See [DATA_SOURCES.md](data_sources.md) for complete citation information, download instructions, and licence terms for all required data files. Note: the MIRTA DoD Sites Boundaries dataset (DataLumos 239599, DOI: 10.3886/E239599V1) requires acceptance of DataLumos Terms of Use and citation notification to bibliography@icpsr.umich.edu upon publication.

---

## Related Projects

- [**secmap**](https://github.com/rjgreenresearch/secmap) — Beneficial ownership chain tracing through SEC EDGAR (Article 2)
- [**afida-parser**](https://github.com/rjgreenresearch/afida-parser) — AFIDA-to-SEC cross-reference and visibility gap analysis

---

## License

Apache 2.0. See [LICENSE](LICENSE).
