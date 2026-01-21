import pandas as pd
import glob
import os

folder = "/home/jat/PC"

# --- Detect files by brand ---
acer_files = glob.glob(f"{folder}/Acer/NBPC_SPECS_ACER_*.xlsx")
dell_files = glob.glob(f"{folder}/Dell/NBPC_SPECS_DELL_*.xlsx")


def combine(files, output_name):
    if not files:
        print(f"No files found for {output_name}")
        return

    dfs = []
    for f in files:
        df = pd.read_excel(f, dtype=str)
        df["source_file"] = os.path.basename(f)  # optional tracking
        dfs.append(df)

    final = pd.concat(dfs, ignore_index=True)
    final.to_excel(output_name, index=False)
    print(f"✅ Created {output_name} with {len(final)} rows")


# --- Create brand outputs ---
combine(acer_files, "Acer_combined.xlsx")
combine(dell_files, "Dell_combined.xlsx")
