# Loading Libraries
library(googledrive)
library(arrow) 
library(readr) 
library(janitor)
library(tidyverse)

# Authenticating email
drive_auth(email = "eappelson@laaclu.org") 

# Defining drive ID's
file_ids <- c("11cvsANbefofj9BrjOYcaHgbJaXcZMUe9", 
              "1k-oF3hD5KCcK0o4XO-V8x4KT47YuXlT5", 
              "19vb1DX4Rbkssx5MWoS79Zyg1OQQm5dx7")

# Downloading file 1
drive_download(as_id(file_ids[1]), overwrite = TRUE)

# Downloading file 2
drive_download(as_id(file_ids[2]), overwrite = TRUE)

# Downloading file 3
drive_download(as_id(file_ids[3]), overwrite = TRUE)

# Downloading the data
data1 <- read_feather('ice_unique_stays_fy11-24ytd.feather')
data2 <- read_feather('ice_detentions_fy11-24ytd.feather')
data3 <- read_delim("headcount_fy11-24ytd.csv.gz", delim = "|")

# Defining louisiana detention centers
la_centers <- c(
  "ALEXANDRIA STAGING FACILITY",
  "ALLEN PARISH PUBLIC SAFETY COMPLEX",
  "BOSSIER PARISH COR. CENTER",
  "CATAHOULA CORRECTIONAL CENTER",
  "CENTRAL LOUISIANA ICE PROC CTR",
  "JACKSON PARISH CORRECTIONAL CENTER",
  "LASALLE CORR CTR OLLA",
  "NATCHITOCHES PARISH DET. CENTER",
  "NEW ORLEANS HOLD ROOM",
  "OAKDALE FED.DET.CENTER",
  "OAKDALE HOLDING ROOM",
  "ORLEANS PARISH SHERIFF",
  "PINE PRAIRIE ICE PROCESSING CENTER",
  "RICHWOOD COR CENTER",
  "River Correctional Center",
  "SHREVEPORT HOLD ROOM",
  "SOUTH LOUISIANA ICE PROC CTR",
  "ST TAMMANY PARISH JAIL",
  "TENSAS PARISH DET. CNTR.",
  "US MARSHALS, W.D. LA.",
  "WINN CORRECTIONAL CENTER"
)

# Filtering the data to Louisiana +
la_data1 <- data1 %>%
  filter(area_of_responsibility == "New Orleans Area of Responsibility") %>%
  filter(detention_facility %in% la_centers)

# Filtering the data to Louisiana +
la_data2 <- data2 %>%
  filter(area_of_responsibility == "New Orleans Area of Responsibility") %>%
  filter(detention_facility %in% la_centers)
