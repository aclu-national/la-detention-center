# ------------------- SETUP ---------------------

# Loading Libraries
library(DBI)
library(RSQLite)
library(ggplot2)


# Connecting to SQL
con <- dbConnect(RSQLite::SQLite(), dbname = "local.sqlite")

# Loading CSV's
df1 <- read.csv("data/data1.csv")
df2 <- read.csv("data/data2.csv")
df3 <- read.csv("data/data3.csv")
df4 <- read.csv("data/data4.csv")
df5 <- read.csv("data/data5.csv")
df6 <- read.csv("data/data6.csv")

# Writing tables to SQL
dbWriteTable(con, "data1_table", df1, append = TRUE, row.names = FALSE)
dbWriteTable(con, "data2_table", df2, append = TRUE, row.names = FALSE)
dbWriteTable(con, "data3_table", df3, append = TRUE, row.names = FALSE)
dbWriteTable(con, "data4_table", df4, append = TRUE, row.names = FALSE)
dbWriteTable(con, "data5_table", df5, append = TRUE, row.names = FALSE)
dbWriteTable(con, "data6_table", df6, append = TRUE, row.names = FALSE)


# ---------------- ANALYSIS BY CENTER ------------------

# Mean, Median, Max, Min by center
center_length <- dbGetQuery(con, 
'
SELECT 
detention_facility,
COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median,
MAX(stay_length / 86400) AS max_stay_length,
MAX(placement_length / 86400) AS max_placement_length,
MIN(stay_length / 86400) AS min_stay_length,
MIN(placement_length / 86400) AS min_placement_length
FROM data1_table
WHERE area_of_responsibility = "New Orleans Area of Responsibility"
GROUP BY detention_facility
ORDER BY detention_facility
'
)

# Mean, Median, Max, Min by center + race
center_length_race <- dbGetQuery(con, 
'
SELECT 
detention_facility,
COUNT(*) AS n,
race,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE area_of_responsibility = "New Orleans Area of Responsibility"
GROUP BY detention_facility, race
ORDER BY detention_facility, race
'
)

# Mean, Median, Max, Min by center + citizenship
center_length_citizenship <- dbGetQuery(con, 
'
SELECT 
detention_facility,
COUNT(*) AS n,
citizenship_country,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE area_of_responsibility = "New Orleans Area of Responsibility"
GROUP BY detention_facility, citizenship_country
ORDER BY detention_facility, citizenship_country
'
)

# Mean, Median, Max, Min by center + gender
center_length_gender <- dbGetQuery(con, 
'
SELECT 
detention_facility,
COUNT(*) AS n,
gender,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE area_of_responsibility = "New Orleans Area of Responsibility"
GROUP BY detention_facility, gender
ORDER BY detention_facility, gender
'
)

# Mean, Median, Max, Min by center + ethnicity
center_length_ethnicity <- dbGetQuery(con, 
'
SELECT 
detention_facility,
COUNT(*) AS n,
ethnic,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE area_of_responsibility = "New Orleans Area of Responsibility"
GROUP BY detention_facility, ethnic
ORDER BY detention_facility, ethnic
'
)

# Mean, Median, Max, Min by center + birth year group
center_length_age <- dbGetQuery(con, 
'
SELECT 
  detention_facility,
  COUNT(*) AS n,
  CASE
    WHEN birth_year BETWEEN 1900 AND 1920 THEN "1900 - 1920"
    WHEN birth_year BETWEEN 1921 AND 1940 THEN "1921 - 1940"
    WHEN birth_year BETWEEN 1941 AND 1960 THEN "1941 - 1960"
    WHEN birth_year BETWEEN 1961 AND 1980 THEN "1961 - 1980"
    WHEN birth_year BETWEEN 1981 AND 2000 THEN "1981 - 2000"
    WHEN birth_year BETWEEN 2001 AND 2020 THEN "2001 - 2020"
    WHEN birth_year BETWEEN 2021 AND 2030 THEN "2021 - 2030"
    ELSE "Unknown"
  END AS birth_groups,
  AVG(stay_length / 86400) AS stay_days_mean, 
  AVG(placement_length / 86400) AS placement_days_mean,
  MEDIAN(stay_length / 86400) AS stay_days_median, 
  MEDIAN(placement_length / 86400) AS placement_days_median
  FROM data1_table
  WHERE area_of_responsibility = "New Orleans Area of Responsibility"
  GROUP BY detention_facility, birth_groups
  ORDER BY detention_facility, birth_groups
'
)

# Mean, Median, Max, Min by center + stay year
center_length_stay_year <- dbGetQuery(con,
'
SELECT 
detention_facility,
COUNT(*) AS n,
strftime("%Y", stay_book_in_date_time) AS stay_book_year,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE area_of_responsibility = "New Orleans Area of Responsibility"
GROUP BY detention_facility, stay_book_year
ORDER BY detention_facility, stay_book_year
'
)

# Mean, Median, Max, Min by center + detention year
center_length_detention_year <- dbGetQuery(con,
'
SELECT 
detention_facility,
COUNT(*) AS n,
strftime("%Y", detention_book_in_date_and_time) AS stay_book_year,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE area_of_responsibility = "New Orleans Area of Responsibility"
GROUP BY detention_facility, stay_book_year
ORDER BY detention_facility, stay_book_year
'
)

# --------------- LOUISIANA FACILITIES -----------------

# Mean, Median, Max, Min by age
length_age <- dbGetQuery(con,
'
SELECT 
CASE
    WHEN birth_year BETWEEN 1900 AND 1920 THEN "1900 - 1920"
    WHEN birth_year BETWEEN 1921 AND 1940 THEN "1921 - 1940"
    WHEN birth_year BETWEEN 1941 AND 1960 THEN "1941 - 1960"
    WHEN birth_year BETWEEN 1961 AND 1980 THEN "1961 - 1980"
    WHEN birth_year BETWEEN 1981 AND 2000 THEN "1981 - 2000"
    WHEN birth_year BETWEEN 2001 AND 2020 THEN "2001 - 2020"
    WHEN birth_year BETWEEN 2021 AND 2030 THEN "2021 - 2030"
    ELSE "Unknown"
  END AS birth_groups,
  COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY birth_groups
'
)

# Mean, Median, Max, Min by age + charge
length_age_charge <- dbGetQuery(con,
'
SELECT 
msc_charge,
CASE
    WHEN birth_year BETWEEN 1900 AND 1920 THEN "1900 - 1920"
    WHEN birth_year BETWEEN 1921 AND 1940 THEN "1921 - 1940"
    WHEN birth_year BETWEEN 1941 AND 1960 THEN "1941 - 1960"
    WHEN birth_year BETWEEN 1961 AND 1980 THEN "1961 - 1980"
    WHEN birth_year BETWEEN 1981 AND 2000 THEN "1981 - 2000"
    WHEN birth_year BETWEEN 2001 AND 2020 THEN "2001 - 2020"
    WHEN birth_year BETWEEN 2021 AND 2030 THEN "2021 - 2030"
    ELSE "Unknown"
  END AS birth_groups,
  COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY birth_groups, msc_charge
ORDER BY msc_charge, birth_groups
'
)

# Mean, Median, Max, Min by race
length_race <- dbGetQuery(con,
'
SELECT 
race,
COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY race
'
)

# Mean, Median, Max, Min by race + charge
length_race_charge <- dbGetQuery(con,
'
SELECT 
msc_charge,
race,
COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY race, msc_charge
ORDER BY msc_charge, race
'
)

# Mean, Median, Max, Min by gender
length_gender <- dbGetQuery(con,
'
SELECT 
gender,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY gender
'
)

# Mean, Median, Max, Min by gender + charge
length_gender_charge <- dbGetQuery(con,
                                 '
SELECT 
msc_charge,
gender,
COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY gender, msc_charge
ORDER BY msc_charge, gender
'
)


# Mean, Median, Max, Min by citizenship
length_citizenship <- dbGetQuery(con,
'
SELECT 
citizenship_country,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY citizenship_country
'
)

# Mean, Median, Max, Min by citizenship + charge
length_citizenship_charge <- dbGetQuery(con,
                                   '
SELECT 
msc_charge,
citizenship_country,
COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY citizenship_country, msc_charge
ORDER BY msc_charge, citizenship_country
'
)

# Mean, Median, Max, Min by ethnicity
length_ethnicity <- dbGetQuery(con,
                                 '
SELECT 
ethnic,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY ethnic
'
)

# Mean, Median, Max, Min by ethnicity + charge
length_ethnicity_charge <- dbGetQuery(con,
'
SELECT 
msc_charge,
ethnic,
COUNT(*) AS n,
AVG(stay_length / 86400) AS stay_days_mean, 
AVG(placement_length / 86400) AS placement_days_mean,
MEDIAN(stay_length / 86400) AS stay_days_median, 
MEDIAN(placement_length / 86400) AS placement_days_median
FROM data1_table
WHERE detention_facility IN ("ALEXANDRIA STAGING FACILITY",
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
  "WINN CORRECTIONAL CENTER")
GROUP BY ethnic, msc_charge
ORDER BY msc_charge, ethnic
'
)


# ----------------- PUSHING TABLES ------------------
dbWriteTable(con, "center_length", center_length, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "center_length_race", center_length_race, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "center_length_citizenship", center_length_citizenship, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "center_length_gender", center_length_gender, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "center_length_ethnicity", center_length_ethnicity, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "center_length_age", center_length_age, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "center_length_stay_year", center_length_stay_year, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "center_length_detention_year", center_length_detention_year, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "length_age", length_age, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "length_age_charge", length_age_charge, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "length_race", length_race, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "length_race_charge", length_race_charge, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "length_gender", length_gender, overwrite = TRUE, row.names = FALSE)

dbDisconnect(con)



