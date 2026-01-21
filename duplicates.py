#!/usr/bin/env python3
"""
Compare Working Files Script
Compares 3Q25 (master) against 4Q25 (new data) using the same matching logic
as merge_master_database.py to find duplicates and new rows.
"""

import pandas as pd
import re


def clean_model_name(model_name):
    """
    Clean and normalize model name for matching.

    Steps:
    1. Convert to lowercase
    2. Strip whitespace
    3. Remove punctuation (.-_/()[]{}:)
    4. Collapse spaces
    5. Keep sub-brand words (Aspire, Nitro, Swift, Vostro, Latitude, Inspiron)
    6. Remove only noise terms and brand names
    """
    if pd.isna(model_name) or model_name == "":
        return ""

    # Step 1: lowercase
    cleaned = str(model_name).lower()

    # Step 2: strip whitespace
    cleaned = cleaned.strip()

    # Step 3: remove punctuation
    punctuation_to_remove = ".-_/()[]{}:"
    for char in punctuation_to_remove:
        cleaned = cleaned.replace(char, " ")

    # Step 4: collapse multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # Step 6: remove noise terms (but NOT sub-brand words)
    noise_terms = [
        "notebook",
        "laptop",
        "gamer",
        "gaming",
        "kit",
        "bundle",
        "ordenador",
        "ultrabook",
        # Brand names
        "acer",
        "dell",
        "hp",
        "lenovo",
        "asus",
        "msi",
        "apple",
        "microsoft",
        "samsung",
        "lg",
        "huawei",
        "razer",
        "alienware",
    ]

    # Split into words and filter out noise terms
    words = cleaned.split()
    filtered_words = [w for w in words if w not in noise_terms]

    # Step 4 again: collapse spaces after filtering
    cleaned = " ".join(filtered_words)

    return cleaned


def load_data():
    """Load the two working files."""
    print("Loading working files...")

    # Load 3Q25 (treat as MASTER / existing data)
    master_df = pd.read_excel(
        "Working File Mobile PC Specifications Price Database 3Q25.xlsb",
        engine="pyxlsb",
        sheet_name="Notebook PC spec data",
        skiprows=5,
    )
    print(f"  Loaded {len(master_df)} rows from 3Q25 (MASTER)")

    # Load 4Q25 (treat as NEW / scraped data)
    new_df = pd.read_excel(
        "Working File Mobile PC Specifications Price Database 4Q25.xlsb",
        engine="pyxlsb",
        sheet_name="Notebook PC spec data",
        skiprows=5,
    )
    print(f"  Loaded {len(new_df)} rows from 4Q25 (NEW)")

    # Validate columns match
    master_cols = set(master_df.columns)
    new_cols = set(new_df.columns)

    if master_cols != new_cols:
        print("\n⚠️  WARNING: Column mismatch detected!")
        missing_in_new = master_cols - new_cols
        missing_in_master = new_cols - master_cols

        if missing_in_new:
            print(f"  Columns in 3Q25 but NOT in 4Q25: {missing_in_new}")
        if missing_in_master:
            print(f"  Columns in 4Q25 but NOT in 3Q25: {missing_in_master}")
        print()

    return new_df, master_df


def normalize_for_comparison(df, is_master=False):
    """Add normalized columns for comparison."""
    # Create copies of comparison columns
    df = df.copy()

    # Normalize Region
    df["region_norm"] = df["Region"].fillna("").astype(str).str.strip().str.lower()

    # Normalize Country - handle different column names
    country_col = "Country/territory" if is_master else "Country"
    df["country_norm"] = df[country_col].fillna("").astype(str).str.strip().str.lower()

    # Normalize Brand
    df["brand_norm"] = df["Brand"].fillna("").astype(str).str.strip().str.lower()

    # Clean Model Name
    df["model_clean"] = df["Model Name"].apply(clean_model_name)

    # Normalize CPU Model
    df["cpu_norm"] = df["CPU Model"].fillna("").astype(str).str.strip().str.lower()

    # Normalize GPU Model
    df["gpu_norm"] = df["GPU Model"].fillna("").astype(str).str.strip().str.lower()

    # Normalize Resolution
    df["resolution_norm"] = (
        df["Resolution"].fillna("").astype(str).str.strip().str.lower()
    )

    return df


def find_matches(scraped_df, master_df):
    """
    Find matches between scraped and master data.

    Matching rules:
    1. Region, Country, Brand must match
    2. Cleaned Model Name must match
    3. CPU Model, GPU Model, and Resolution must all match

    Returns:
        new_rows: DataFrame of rows to add
        duplicate_rows: DataFrame of duplicate rows
    """
    print("\nProcessing matches...")

    # Normalize both dataframes
    scraped_norm = normalize_for_comparison(scraped_df, is_master=False)
    master_norm = normalize_for_comparison(master_df, is_master=False)

    new_rows_list = []
    duplicate_rows_list = []

    for idx, scraped_row in scraped_norm.iterrows():
        # Step 1: Filter master rows by Region, Country, Brand
        mask = (
            (master_norm["region_norm"] == scraped_row["region_norm"])
            & (master_norm["country_norm"] == scraped_row["country_norm"])
            & (master_norm["brand_norm"] == scraped_row["brand_norm"])
        )
        filtered_master = master_norm[mask]

        # If no match on Region/Country/Brand -> NEW
        if len(filtered_master) == 0:
            new_rows_list.append(idx)
            continue

        # Step 2: Check model name match
        model_matches = filtered_master[
            filtered_master["model_clean"] == scraped_row["model_clean"]
        ]

        # If no model name match -> NEW
        if len(model_matches) == 0:
            new_rows_list.append(idx)
            continue

        # Step 3: Check CPU, GPU, and Resolution match
        cpu_gpu_resolution_matches = model_matches[
            (model_matches["cpu_norm"] == scraped_row["cpu_norm"])
            & (model_matches["gpu_norm"] == scraped_row["gpu_norm"])
            & (model_matches["resolution_norm"] == scraped_row["resolution_norm"])
        ]

        # If exact CPU+GPU+Resolution match exists -> DUPLICATE
        if len(cpu_gpu_resolution_matches) > 0:
            duplicate_rows_list.append(idx)
        else:
            # CPU, GPU, or Resolution differs -> NEW variant
            new_rows_list.append(idx)

    # Create output dataframes
    new_rows = scraped_df.iloc[new_rows_list].copy()
    duplicate_rows = scraped_df.iloc[duplicate_rows_list].copy()

    print(f"  Found {len(new_rows)} NEW rows to add")
    print(f"  Found {len(duplicate_rows)} DUPLICATE rows to skip")

    return new_rows, duplicate_rows


def main():
    """Main execution function."""
    print("=" * 60)
    print("Compare Working Files: 3Q25 vs 4Q25")
    print("=" * 60)

    # Load data
    new_df, master_df = load_data()

    # Find matches
    new_rows, duplicate_rows = find_matches(new_df, master_df)

    # Save outputs
    print("\nSaving output files...")
    new_rows.to_excel("new_rows_to_add.xlsx", index=False)
    print(f"  ✓ Saved new_rows_to_add.xlsx ({len(new_rows)} rows)")

    duplicate_rows.to_excel("duplicate_rows_found.xlsx", index=False)
    print(f"  ✓ Saved duplicate_rows_found.xlsx ({len(duplicate_rows)} rows)")

    print("\n" + "=" * 60)
    print("Processing complete!")
    print("=" * 60)

    # Summary
    print("\nSUMMARY:")
    print(f"  3Q25 (MASTER) rows: {len(master_df)}")
    print(f"  4Q25 (NEW) rows processed: {len(new_df)}")
    print(f"  New rows (unique to 4Q25): {len(new_rows)}")
    print(f"  Duplicates (already in 3Q25): {len(duplicate_rows)}")


if __name__ == "__main__":
    main()
