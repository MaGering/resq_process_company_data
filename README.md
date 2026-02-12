Introduction
============

This repo creates:

1. A `csv`-file with all companies in Adlershof:: `crawl_enterprizes_Adlershof.py`
2. It allocates geodata to all companies (Adresses): `get_company_geo_data.py`
3. It assignes to every company a cluster by Branchenzweig: `assign_company_to_cluster.py`
4. It preprocesses the data to be used for nominatim geocoder: `preprocess_companies.py`
5. And finally gives area and type of use for nPro from processed and geocoded data: `get_area_per_type_of_use.py`

raw_data
========
Find raw data here: `https://rlinstitutde.sharepoint.com/:f:/s/427_ResQEnergy-427_internal_Team/IgBrRnJIDRASTrrFOauHZw_pATnH9rJFoh6hthIq46IUDBI?e=vVWIUP`

To skip step 1 and 2
====================
Step 1 and 2 take a lot of time. You can start right away with steps 3. to 5.
To do this use data `adlershof_companies.csv` and `adlershof_companies_geodata.csv` from here: `https://rlinstitutde.sharepoint.com/:f:/s/427_ResQEnergy-427_internal_Team/IgCTT3B0WA2pRIoc4zZY0s-SASHBtR4BINgFFCASqVY0xcE?e=0rnqKF`
