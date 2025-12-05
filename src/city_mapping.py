#!/usr/bin/env python3
"""
Map US Geonames cities -> nearest county -> CBSA (metro/micro) region.

Adjust file paths at the top before running.
"""

import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from scipy.spatial import cKDTree

# ---------------------------
# USER CONFIG: update paths
# ---------------------------
CITIES_FILE = "../data/resources/cities15000.txt"
COUNTY_SHAPEFILE = "../data/resources/cb_2020_us_county_500k/cb_2020_us_county_500k.shp"
CBSA_XLS = "../data/resources/list1_2020.xls"
OUTPUT_CSV = "../data/city_to_cbsa.csv"

# ---------------------------
# Load Geonames cities
# ---------------------------
def load_cities(path):
    cols = [
        "geonameid", "name", "asciiname", "alternatenames",
        "latitude", "longitude", "feature_class", "feature_code",
        "country_code", "cc2", "admin1", "admin2", "admin3", "admin4",
        "population", "elevation", "dem", "timezone", "modification_date"
    ]
    df = pd.read_csv(path, sep="\t", header=None, names=cols, low_memory=False, dtype={"country_code": str})
    df_us = df[df["country_code"] == "US"].copy()
    df_us["latitude"] = df_us["latitude"].astype(float)
    df_us["longitude"] = df_us["longitude"].astype(float)
    gdf = gpd.GeoDataFrame(df_us, geometry=[Point(xy) for xy in zip(df_us["longitude"], df_us["latitude"])], crs="EPSG:4326")
    return gdf

# ---------------------------
# Load US counties
# ---------------------------
def load_counties(path):
    counties = gpd.read_file(path)
    if "GEOID" not in counties.columns:
        if "STATEFP" in counties.columns and "COUNTYFP" in counties.columns:
            counties["GEOID"] = counties["STATEFP"].astype(str).str.zfill(2) + counties["COUNTYFP"].astype(str).str.zfill(3)
        else:
            raise ValueError("County shapefile missing GEOID or STATEFP/COUNTYFP")
    counties = counties.to_crs("EPSG:4326")
    counties = counties[~counties.geometry.is_empty].copy()
    return counties

# ---------------------------
# Compute centroids
# ---------------------------
def county_centroids(counties, crs_proj="EPSG:5070"):
    counties_proj = counties.to_crs(crs_proj)
    counties_proj["centroid"] = counties_proj.geometry.centroid
    coords = np.array([[pt.x, pt.y] for pt in counties_proj["centroid"]])
    centroids_df = pd.DataFrame({
        "GEOID": counties_proj["GEOID"].astype(str).values,
        "centroid_x": coords[:, 0],
        "centroid_y": coords[:, 1]
    })
    return centroids_df, counties_proj

# ---------------------------
# KDTree for fast lookup
# ---------------------------
def build_kdtree(centroids_df):
    pts = np.vstack([centroids_df["centroid_x"].values, centroids_df["centroid_y"].values]).T
    return cKDTree(pts)

# ---------------------------
# Load CBSA crosswalk
# ---------------------------
def load_cbsa_crosswalk(xls_path):
    # Your file has headers in the 3rd row → skip first 2 rows
    df = pd.read_excel(xls_path, sheet_name=0, skiprows=2, dtype=str)

    # Standardize column names
    df = df.rename(columns={
        "FIPS State Code": "state_fips",
        "FIPS County Code": "county_fips",
        "CBSA Code": "cbsa_code",
        "CBSA Title": "cbsa_title",
        "Metropolitan/Micropolitan Statistical Area": "cbsa_type"
    })

    # Create 5-digit GEOID by combining state + county codes
    df["GEOID"] = df["state_fips"].str.zfill(2) + df["county_fips"].str.zfill(3)

    return df[["GEOID", "cbsa_code", "cbsa_title"]].drop_duplicates(subset=["GEOID"]).set_index("GEOID")

# ---------------------------
# Main mapping
# ---------------------------
def map_cities_to_cbsa(cities_file, county_shapefile, cbsa_xls, output_csv):
    print("Loading cities...")
    cities_gdf = load_cities(cities_file)
    print(f"US cities loaded: {len(cities_gdf)}")

    print("Loading counties...")
    counties = load_counties(county_shapefile)
    print(f"Counties loaded: {len(counties)}")

    print("Computing county centroids...")
    centroids_df, counties_proj = county_centroids(counties)

    print("Building KDTree...")
    tree = build_kdtree(centroids_df)

    print("Projecting cities and querying nearest county...")
    cities_proj = cities_gdf.to_crs("EPSG:5070")
    city_coords = np.vstack([cities_proj.geometry.x.values, cities_proj.geometry.y.values]).T
    dists, idxs = tree.query(city_coords, k=1)
    cities_gdf["nearest_county_geoid"] = centroids_df.loc[idxs, "GEOID"].values
    cities_gdf["dist_to_county_m"] = dists

    print("Loading CBSA crosswalk...")
    cbsa_df = load_cbsa_crosswalk(cbsa_xls)

    print("Joining city -> county -> CBSA...")
    cities_gdf["nearest_county_geoid"] = cities_gdf["nearest_county_geoid"].astype(str).str.zfill(5)
    city_cbsa = cities_gdf.join(cbsa_df, on="nearest_county_geoid", how="left")

    # Fill non-metro
    city_cbsa["cbsa_code"] = city_cbsa["cbsa_code"].fillna("NON_CBSA")
    city_cbsa["cbsa_title"] = city_cbsa["cbsa_title"].fillna("Non-metro / not in CBSA crosswalk")

    # Save final mapping
    outcols = ["geonameid", "name", "asciiname", "latitude", "longitude",
               "population", "admin1", "admin2", "nearest_county_geoid",
               "dist_to_county_m", "cbsa_code", "cbsa_title"]
    city_cbsa[outcols].to_csv(output_csv, index=False)
    print(f"✅ City -> CBSA mapping saved to {output_csv}")

if __name__ == "__main__":
    map_cities_to_cbsa(CITIES_FILE, COUNTY_SHAPEFILE, CBSA_XLS, OUTPUT_CSV)
