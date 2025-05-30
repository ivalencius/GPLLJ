# Description of all codes

## /data-download/

All codes in this folder download and process the associated dataset. Large files are saved to `SCRATCH_DIR` (inside `../setup.py`) while smaller files (such as metadata need to download data) is stored inside `../data/`.

## GHCNh_cna.ipynb

This file looks at near surface (10 m) wind speed trends for the Global Historical Climatology Network hourly (GHCNh) dataset. It is designed as the replacement for the Integrated Surface Database (ISD).

## cna_grid.ipynb (TBD)
Creates the CNA grid used to harmonize all model data.