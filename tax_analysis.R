library(data.table)
library(sf)

parcels <- st_read("out/bozeman_parcel_tax/parcels.gpkg", stringsAsFactors = FALSE)
parcels <- st_transform(parcels, 26912)

zoning <- st_read("../shapefiles/Zoning/Zoning.shp", stringsAsFactors = FALSE)
stopifnot(st_crs(parcels) == st_crs(zoning))
dput(colnames(zoning))
zoning <- zoning[, c("ZONING", "geometry")]

neighborhoods <- st_read("../shapefiles/Neighborhoods/neighborhoods.shp", stringsAsFactors = FALSE)
neighborhoods <- st_transform(neighborhoods, 26912)

parcels <- st_join(parcels, zoning)

parcels <- st_join(parcels, neighborhoods)

parcels <- parcels[!is.na(parcels$ZONING), ]


DT <- as.data.table(st_drop_geometry(parcels))

DT <- DT[!is.na(TaxAmount)]

tax <- DT[!(ParcelPropType %in% c("EP - Exempt Property",
                                  "EP_PART - Partial Exempt Property")), 
          .(acres = sum(TotalAcres), # BldgValue > 0
       tax = sum(TaxAmount), 
       parcel_count = .N),
   by = NAME]
tax[, tax_per_acre := round(tax / acres, 2)]
tax[, parcels_per_acre := round(parcel_count / acres, 2)]

fwrite(tax, "out/zoning_tax_summary.csv", row.names = FALSE)
#-----
# Unioned parcels - assign the zoning by the zoning of the largest list
zoned_parcels <- st_read("out/cob_parcels_w_zoning_rough/parcels.shp", stringsAsFactors = FALSE)
DT <- as.data.table(st_drop_geometry(zoned_parcels))

DT <- DT[!is.na(Assessment)]
setorder(DT, Assessment, -new_area)
parcel_zoning <- DT[, .(AssesmntId = first(AssesmntId),
                        zoning = first(ZONING)), by = Assessment]

res <- merge(parcels, parcel_zoning)

st_write(res, "out/ParcelsZoningCOB/parcels.gpkg", delete_layer = TRUE)

# ----
DT <- as.data.table(st_drop_geometry(res))

DT <- DT[!is.na(mt_tax_pmt)]

tax <- DT[TotalBuild > 0, .(acres = sum(TotalAcres),
                                                                         tax = sum(mt_tax_pmt),
                                                                         parcel_count = .N),
          by = PropType]

tax <- DT[PropType %in% c("IMP_U - Improved Property - Urban",
                                           "TU - Townhouse Urban",
                                           "IMP_R - Improved Property - Rural",
                                           "APT_U - Apartment Urban"), .(acres = sum(TotalAcres),
              tax = sum(mt_tax_pmt),
              parcel_count = .N),
          by = zoning]

tax[, tax_per_acre := round(tax / acres, 2)]
