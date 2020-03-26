# Gallatin County Tax per Parcel

Project to calculate per-parcel tax payments, based on Montana Cadastral data.

## Data

The property tax data for 2019 / 2020 is downloaded from [ftp://ftp.geoinfo.msl.mt.gov/Data/Spatial/MSDI/Cadastral/](ftp://ftp.geoinfo.msl.mt.gov/Data/Spatial/MSDI/Cadastral/)

## Data Processing

The data processing in `orion.R` attempts to create relationships between the various properties listed and the underlying parcels.