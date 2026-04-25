# Data Sources

This repository contains no data files. All analysis data must be downloaded
directly from the authoritative primary sources listed below. This design
ensures reproducibility from original sources, avoids redistribution of
government datasets, and ensures users always have the most current data.

---

## 1. USDA AFIDA Annual Holdings

**Full name:** Agricultural Foreign Investment Disclosure Act — Detailed Holdings Data

**Provider:** U.S. Department of Agriculture, Farm Service Agency (USDA FSA)

**URL:** https://www.fsa.usda.gov/resources/economic-policy-analysis/afida/annual-reports-underlying-data

**File used:** `AFIDACurrentHoldingsYR2024.xlsx` (and prior years for panel analysis)

**License:** U.S. government work, public domain.

**Citation:**
> U.S. Department of Agriculture, Farm Service Agency (2024). *AFIDA Detailed Holdings Data: Foreign Holdings of U.S. Agricultural Land through December 31, 2024*. Washington, DC: USDA FSA. https://www.fsa.usda.gov/resources/economic-policy-analysis/afida/annual-reports-underlying-data

**Used by:** `data_prep.py`, `webb_analysis.py`

---

## 2. MIRTA DoD Sites — Boundaries (GeoJSON)

**Full name:** HIFLD OPEN Military Installations, Ranges, and Training Areas (MIRTA) DoD Sites — Boundaries

**Provider:** United States Department of Defense; United States Department of Homeland Security

**Distributor:** Inter-university Consortium for Political and Social Research (ICPSR), DataLumos

**DataLumos project:** 239599, Version V1

**DOI:** https://doi.org/10.3886/E239599V1

**Published:** 2025-11-03

**Download URL:** https://www.datalumos.org/datalumos/project/239599/version/V1/view

**File used:** `mirta-dod-sites-boundaries-geojson.zip`

**License:** Attribution 4.0 International (CC BY 4.0). Requires attribution in any publication using these data. See DataLumos Terms of Use.

**Country-of-concern restriction:** The DataLumos Terms of Use prohibit sharing these data with researchers in China, Hong Kong, Macau, Russia, Iran, North Korea, Venezuela, or Cuba without ICPSR written approval, pursuant to Executive Order 14117. See terms at https://www.datalumos.org/datalumos/project/239599/version/V1/download/terms

**Required citation:**
> United States Department of Defense, and United States Department of Homeland Security. *HIFLD OPEN Military Installations, Ranges, and Training Areas (MIRTA) DoD Sites — Boundaries*. Ann Arbor, MI: Inter-university Consortium for Political and Social Research [distributor], 2025-11-03. https://doi.org/10.3886/E239599V1

**Citation notification obligation:** If you publish work using these data, you are required to send a citation of your published work to ICPSR at bibliography@icpsr.umich.edu for inclusion in the DataLumos project description.

**Dataset description:** Depicts the authoritative locations of the most commonly known Department of Defense sites, installations, ranges, and training areas worldwide. Sites encompass land that is federally owned or otherwise managed. Created from source data provided by the four Military Service Component headquarters and compiled by the Defense Installation Spatial Data Infrastructure (DISDI) Program, Office of the Assistant Secretary of Defense for Energy, Installations, and Environment.

**Important caveat (from DISDI):** "The point and boundary location data in MIRTA is intended for planning purposes only, and does not represent the legal or surveyed land parcel boundaries."

**Used by:** `data_prep.py` (to generate `processed/cfius_appendix_a_geocoded.csv`)

---

## 3. MIRTA DoD Sites — Points (GeoJSON) [alternative/prior version]

**Full name:** HIFLD OPEN Military Installations, Ranges, and Training Areas (MIRTA) DoD Sites — Points

**DataLumos project:** 239602

**DOI:** https://doi.org/10.3886/E239602V1

**URL:** https://www.datalumos.org/datalumos/project/239602/version/V1/view

**Note:** This is the point-feature version of the MIRTA dataset (one point per site). The analysis primarily uses the boundaries version (Project 239599) for polygon-based proximity measurement. The points version was used in earlier development runs of `webb_analysis.py`. The same citation and terms of use apply.

---

## 4. County Centroids

**Option A — NOAA County Shapefile (c_16ap26)**

**Provider:** NOAA National Weather Service

**URL:** https://www.weather.gov/gis/Counties

**File used:** `c_16ap26.zip`

**License:** U.S. government work, public domain.

**Citation:**
> NOAA National Weather Service (2026). *U.S. County Boundaries Shapefile (c_16ap26)*. Silver Spring, MD: NOAA NWS. https://www.weather.gov/gis/Counties

**Used by:** `webb_analysis.py`

---

**Option B — Census Bureau CenPop2020 County Population Centroids**

**Provider:** U.S. Census Bureau

**URL:** https://www2.census.gov/geo/docs/reference/cenpop2020/county/

**File used:** `CenPop2020_Mean_CO.txt`

**License:** U.S. government work, public domain.

**Citation:**
> U.S. Census Bureau (2020). *2020 Census Centers of Population by County*. Washington, DC: U.S. Census Bureau. https://www2.census.gov/geo/docs/reference/cenpop2020/county/

**Note:** Census population centroids weight the centroid by population distribution within the county, which differs from the NOAA geometric centroid. For large, sparsely populated western counties, the population centroid may diverge substantially from the geometric centroid. The NOAA centroid is recommended for spatial analysis of agricultural land.

**Used by:** `data_prep.py`

---

## 5. CFIUS Appendix A Part Classification

**Source:** 31 CFR Part 802, Appendix A (current regime: December 22, 2024 update)

**Provider:** U.S. Department of the Treasury, Committee on Foreign Investment in the United States (CFIUS)

**URL:** https://www.ecfr.gov/current/title-31/subtitle-B/chapter-VIII/part-802/appendix-Appendix A to Part 802

**File:** `processed/appendix_a_part_classification.csv` (author-compiled from eCFR text)

**License:** U.S. government work, public domain.

**Citation:**
> U.S. Department of the Treasury (2024). *31 CFR Part 802, Appendix A — Military Installations, Ranges, and Training Areas.* Washington, DC: U.S. Treasury / CFIUS. https://www.ecfr.gov/current/title-31/subtitle-B/chapter-VIII/part-802/appendix-Appendix A to Part 802

**Note:** `appendix_a_part_classification.csv` was compiled manually from the eCFR text listing and the December 2024 Federal Register update (89 Fed. Reg. 103,028). It classifies each listed installation as Part 1 (1-mile threshold), Part 2 (100-mile threshold), or Part 3 (ICBM missile field counties, township/range boundary). This file is included in the repository as a derived analytical product, not primary government data.

---

## Citation Requirements Summary

| Dataset | Citation required in publications | Notification required |
|---------|-----------------------------------|-----------------------|
| USDA AFIDA | Yes (standard academic citation) | No |
| MIRTA Boundaries (239599) | Yes — see required citation above | Yes — email bibliography@icpsr.umich.edu |
| MIRTA Points (239602) | Yes | Yes — email bibliography@icpsr.umich.edu |
| NOAA County Centroids | Yes (standard) | No |
| Census CenPop2020 | Yes (standard) | No |
| 31 CFR Part 802 Appendix A | Yes (regulatory citation) | No |

---

## How to Download

```bash
# Create data directory
mkdir -p data/

# 1. AFIDA 2024 — download manually from USDA FSA website
#    https://www.fsa.usda.gov/resources/economic-policy-analysis/afida/annual-reports-underlying-data
#    Save as: data/AFIDACurrentHoldingsYR2024.xlsx

# 2. MIRTA Boundaries — download from DataLumos (requires account, terms agreement)
#    https://www.datalumos.org/datalumos/project/239599/version/V1/view
#    Accept terms of use, then download mirta-dod-sites-boundaries-geojson.zip
#    Extract and save GeoJSON as: data/mirta-dod-sites-boundaries-geojson.geojson

# 3. NOAA county centroids — direct download
curl -L "https://www.weather.gov/source/gis/Shapefiles/County/c_16ap26.zip" \
     -o data/c_16ap26.zip

# 4. Census centroids (alternative)
curl -L "https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt" \
     -o processed/county_centroids_raw.txt
# Then reformat columns to fips, latitude, longitude
```
