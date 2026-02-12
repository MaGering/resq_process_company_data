Introduction
============

This repo creates:

    1. A `csv`-file with all companies in Adlershof:: `crawl_enterprizes_Adlershof.py`
    2. It allocates geodata to all companies (Adresses): `get_company_geo_data.py`
    3. It assignes to every company a cluster by Branchenzweig: `assign_company_to_cluster.py`
    4. It preprocesses the data to be used for nominatim geocoder: `preprocess_companies.py`
    5. And finally gives area and type of use for nPro from processed and geocoded data: `get_area_per_type_of_use.py`
