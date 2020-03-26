# process the orion database data to aggregate all of the tax paying entties on
# a parcel to the parcel itself
library(data.table)
library(sf)

property <- fread("data/orion/Property.csv")
property <- property[PropCategory %in% c("RP", "RP   ")]
for(col in c("Addr_PreDirectional", "Addr_RoadSuffix", "Addr_PostDirectional",
    "Addr_UnitNumber")) {
  set(property, which(property[[col]] == "NULL"), col, "")
}

property[property == "NULL"] <- NA
property <- property[, c("PropertyID", "TaxYear", "County", "GeoCode", "GeoCodeSearch", 
                         "AssessmentCode", 
                         "LevyDistrict", "SubSection", "SubBlock", "SubLot", "SubLotRange", 
                         "SubdivisionCode", "Subdivision", "Section", "Township", "Range", 
                         "NBHD", "Addr_Number", "Addr_PreDirectional", "Addr_Street", 
                         "Addr_RoadSuffix", "Addr_PostDirectional", "Addr_City", "Addr_State", 
                         "Addr_Zip", "Addr_UnitNumber", "Addr_UnitType", "PropType", "LegalDescription", 
                         "Condo_GenPct", "Condo_LtdPct", "LivUnits",
                         "Location_Desc", "Dwel_Val", 
                         "MOB_Val", "Com_Val", "ObyFlat_Val", "TotImpt_Val", "TotMarket_Val", 
                         "TotMarket_Acres", 
                         "Levy_District", "PropCategory", 
                         "PropSubCategory", "PropSubCategory_Desc"), with = FALSE]
# parcels <- st_read("out/GallatinOwnerParcelWithTax/parcels.shp", stringsAsFactors = FALSE)
# parcels <- st_transform(parcels, 26912)
# property[, IsParcel := GeoCodeSearch %in% parcels$PARCELID]
# saveRDS(parcels$PARCELID, file = "data/parcel_ids.rds")
property[, IsParcel := GeoCodeSearch %in% readRDS("data/parcel_ids.rds")]
table(property$IsParcel)
setkey(property, PropertyID)


# Join Condos
condo_master <- fread("orion/CondoMaster.csv")
setkey(condo_master, PropertyID)
# Keep only condo masters that are parcels
condo_master <- condo_master[CondoMasterId %in% property[IsParcel == TRUE]$PropertyID]


property <- condo_master[, .(PropertyID, CondoMasterID = CondoMasterId)][property] # merge on PropertyID

# MOBILE HOMES --------------------------------
# Join Mobile Homes to their Parcel ... by GeoCode

# when a property appears in the condo master list, join to the parcel with the same address.
# All condo masters end in 7000
# 06-0338-02-1-60-03-7
# Condos end in 7xxxx
# if the property id is in condo_master$PropertyID then its parent is the parcel that's in 
# IsParcel == TRUE with a GeoCode ending the in same Xxxx
cm <- property[grepl("7\\d{3}$", GeoCode, perl = TRUE) & 
                 is.na(CondoMasterID) &
                 IsParcel == FALSE, .(PropertyID, GeoCode)]
cm[, MasterGeoCode := paste0(substr(GeoCode, 1, 18), "-7000")]
cm <- merge(cm, property[IsParcel == TRUE, .(MasterGeoCode = GeoCode, CondoMasterID2 = PropertyID)],
            by = "MasterGeoCode")
uniqueN(cm$PropertyID)
uniqueN(cm$CondoMasterID2)

property <- merge(property, cm[, .(PropertyID, CondoMasterID2)], by = "PropertyID", all.x = TRUE)
property[is.na(CondoMasterID), CondoMasterID := CondoMasterID2]
property[, CondoMasterID2 := NULL]



# MOBILE HOMES --------------------------------
# Join Mobile Homes to their Parcel ... by GeoCode
mh <- property[grepl("9\\d{3}$", GeoCode, perl = TRUE) & IsParcel == FALSE, .(PropertyID, GeoCode)]
mh[, MasterGeoCode := paste0(substr(GeoCode, 1, 18), "-0000")]
mh <- merge(mh, property[IsParcel == TRUE, .(MasterGeoCode = GeoCode, MobileHomeMasterID = PropertyID)],
            by = "MasterGeoCode")
uniqueN(mh$PropertyID)
uniqueN(mh$MobileHomeMasterID)

property <- merge(property, mh[, .(PropertyID, MobileHomeMasterID)], by = "PropertyID", all.x = TRUE)


# Linked Properties -----------------------------
linked_property <- fread("data/orion/LinkedProperty.csv")
linked_property <- linked_property[LinkTypeDesc %in%
                                     c("3 - Agricultural (tieback is primary parcel)", "Real Property/Personal Property Link", 
                                       "9 - Other", "1 - Imps Linked to Land Owned by Others", 
                                       "7 - Combination", "2 - Commercial (tieback is primary parcel)", 
                                       "5 - Condo Master/cost = % ownr final val + 600,700,800 flds")]
linked_props <- linked_property[!duplicated(linked_property$PropertyID), .(PropertyID, LinkedPropertyID)]
property <- merge(property, linked_props, all.x = TRUE, by = "PropertyID")

# Other Unlinkec 4XXX Properties
# property[, BaseGeoCode := substr(GeoCode, 1, 18)]
op <- property[grepl("4\\d{3}$", GeoCode, perl = TRUE) & 
                 is.na(CondoMasterID) &
                 IsParcel == FALSE, .(PropertyID, GeoCode)]
op[, MasterGeoCode := paste0(substr(GeoCode, 1, 18), "-0000")]
op <- merge(op, property[IsParcel == TRUE, .(MasterGeoCode = GeoCode, MasterID = PropertyID)],
            by = "MasterGeoCode")            
uniqueN(op$PropertyID)
uniqueN(op$MasterID)
property <- merge(property, op[, .(PropertyID, MasterID)], by = "PropertyID", all.x = TRUE)

op <- property[grepl("4\\d{3}$", GeoCode, perl = TRUE) & 
                 is.na(CondoMasterID) & is.na(MasterID) &
                 IsParcel == FALSE, .(PropertyID, GeoCode)]

# Assign ParcelID based on lookup

property[!is.na(MobileHomeMasterID), ParcelID := MobileHomeMasterID]
property[!is.na(MasterID), ParcelID := MasterID]
property[!is.na(CondoMasterID), ParcelID := CondoMasterID]
property[is.na(ParcelID) & IsParcel, ParcelID := PropertyID]

# Okay, now let's join parcel-less properties to the first parcel WITH an ADDRESS
# The STRAGGLERS
parcels <- property[IsParcel == TRUE]
parcel_addys <- parcels[!duplicated(parcels[, .(Addr_Number, Addr_PreDirectional, Addr_Street, Addr_City)]),
                         .(ParcelPropertyID = PropertyID, Addr_Number, Addr_PreDirectional, Addr_Street, Addr_City)]
parcel_addys <- parcel_addys[!(is.na(Addr_Number) | is.na(Addr_Street) | is.na(Addr_City))]
setorder(parcel_addys, Addr_City, Addr_Street, Addr_PreDirectional, Addr_Number)

parcelless <- property[IsParcel == FALSE & 
                         is.na(ParcelID) & !(is.na(Addr_Number) | is.na(Addr_Street) | is.na(Addr_City)), 
                       .(PropertyID, Addr_Number, Addr_PreDirectional, Addr_Street, Addr_City)]

parcel_match <- merge(parcelless, parcel_addys, by = c("Addr_Number", "Addr_PreDirectional", "Addr_Street", "Addr_City"))
parcel_match <- parcel_match[!duplicated(parcel_match)]
property <- merge(property, parcel_match[, .(PropertyID, ParcelPropertyID)],
                  by = "PropertyID", all.x = TRUE)

property[is.na(ParcelID) & !is.na(ParcelPropertyID), ParcelID := ParcelPropertyID]
rm(parcels, parcel_addys, parcelless)


# View(property[is.na(ParcelID)])
View(property[Addr_Number == "105" & Addr_PreDirectional == "W" & Addr_Street == "MAIN"])
View(property[Addr_Number == "5" & Addr_PreDirectional == "W" & Addr_Street == "MENDENHALL"])

# OWNER
owner <- fread("data/orion/Owner.csv")
party_name <- fread("data/orion/PartyName.csv")
party_addr <- fread("data/orion/PartyAddr.csv")
parties <- party_name[, .(owner_name = paste(FullName, collapse = " & ")), by = PartyID]
owner <- merge(owner, parties, by = "PartyID")
rm(parties, party_name, party_addr)

property <- merge(property, owner, by = "PropertyID")

# MERGE RECORDS
parcels <- property[IsParcel == TRUE, 
              .(ParcelID,
                PropertyID,
                Address = paste(Addr_Number, Addr_PreDirectional, Addr_Street, Addr_RoadSuffix, Addr_PostDirectional,
                                              ifelse(Addr_UnitNumber == "", "", paste("UNIT", Addr_UnitNumber)),
                                              Addr_City, Addr_State, Addr_Zip),
                GeoCode, 
                GeoCodeSearch,
                AssessmentCode,
                COBAssessmentCode = gsub("(?<![0-9])0+", "", AssessmentCode, perl = TRUE),
                Subdivision,
                LegalDescription,
                ParcelAcres = as.numeric(TotMarket_Acres),
                ParcelPropType = PropType
                )]

# ASSESSMENTS ------------------------------------
assessment <- fread("data/orion/Assessment.csv")
class_codes <- fread("data/orion/ClassCodes.csv")
assessment <- merge(assessment, class_codes, by.x = "ClassCode", by.y = "Code")
rm(class_codes)

assessments <- merge(property, assessment, by = "PropertyID")

#View(DT[ParcelID == "641937"])
# Aggregate to parcels

Mode <- function(v, na.rm = FALSE) {
  uniqv <- unique(v)
  if (na.rm == TRUE)
    uniqv <- uniqv[!is.na(uniqv)]
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

parcel_sums <- assessments[, 
  .(
    PropType = Mode(PropType),
    PropertyClass = paste(Description, collapse = " & "),
    LivUnits = Mode(as.numeric(LivUnits), na.rm = TRUE), 
    LandValue = sum(as.numeric(LandValue), na.rm = TRUE), 
    BldgValue = sum(as.numeric(BuildingValue), na.rm = TRUE), 
    MktValue = sum(as.numeric(TaxableMktValue), na.rm = TRUE), 
    TaxablePct = mean(as.numeric(TaxablePct), 0, na.rm = TRUE), 
    TaxableVal = sum(as.numeric(TaxableValue), na.rm = TRUE), 
    TotAcres = sum(as.numeric(Acres), na.rm = TRUE), 
    Mills = Mode(TotalMills, na.rm = TRUE), 
    TaxAmount = sum(as.numeric(TaxAmount), na.rm = TRUE),
    PropCount = uniqueN(PropertyID),
    AssmntCount = .N
   ), 
   by = ParcelID]

res <- merge(parcels, parcel_sums, by = "ParcelID")

View(res[ParcelID == "641927"]) # Me
View(res[ParcelID == "641937"]) # Five West
View(res[ParcelID == "639417"]) # Mall
fwrite(res, file = "out/parcel_tax.csv")


# create a shapefile

sum(res$TaxAmount)

parcels <- st_read("shp/GallatinOwnerParcel_shp/GallatinOwnerParcel_shp.shp", stringsAsFactors = FALSE)
parcels <- st_transform(parcels, 26912)
parcels[parcels$TotalAcres == 0, ]$TotalAcres <- parcels[parcels$TotalAcres == 0, ]$GISAcres
parcels <- parcels[, c("PARCELID", "COUNTYCD", "GISAcres", "TaxYear", "PropertyID", 
                       "Assessment", "Township", "Range", "Section_", "LegalDescr", 
                       "Subdivisio", "Certificat", "AddressLin", "AddressL_1", "CityStateZ", 
                       "PropAccess", "LevyDistri", "PropType", "Continuous", 
                       "TotalAcres", "TotalBuild", "TotalLandV", "TotalValue", 
                       "OwnerName", "OwnerAddre", "OwnerAdd_1", "OwnerAdd_2", "OwnerCity", 
                       "OwnerState", "OwnerZipCo", 
                       "geometry")]

cob_limits <- st_read("../shapefiles/City_Limits/City_Limits.shp")
stopifnot(st_crs(parcels) == st_crs(cob_limits))

setDF(res)
is.nan.data.frame <- function(x)
  do.call(cbind, lapply(x, is.nan))
res[is.nan(res)] <- NA

sf <- st_filter(parcels, cob_limits)
sf <- merge(sf, res, by.x = "PARCELID", by.y = "GeoCodeSearch")
sf$PropertyID <- sf$ParcelID
sf$ParcelID <- NULL

sf <- sf[, c("PARCELID","Assessment", "TaxYear",
             #  "LegalDescr",
             # 
             # "PropAccess", "LevyDistri", "Continuous", 
             # , "OwnerAddre",
             # "OwnerAdd_1", "OwnerAdd_2","OwnerZipCo",
             "TotalAcres", "TotalBuild", "TotalLandV", "TotalValue", 
               "GeoCode", "AssessmentCode", "Address", 
             "COBAssessmentCode", "Subdivision", # "LegalDescription",
             "ParcelPropType", "PropertyClass", "LivUnits",
             "LandValue", "BldgValue", "MktValue", "TaxablePct", "TaxableVal",
             "Mills", "TaxAmount", "PropCount", "AssmntCount",
             "Subdivisio", "Certificat", "AddressLin", "AddressL_1", "CityStateZ",
             "OwnerName",  "OwnerCity", "OwnerState", 
             "geometry")]

sf$TaxPerAcre <- sf$TaxAmount / sf$TotalAcres

st_write(sf, "out/bozeman_parcel_tax/parcels.gpkg", delete_layer = TRUE)
