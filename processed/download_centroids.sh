#!/bin/bash
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
