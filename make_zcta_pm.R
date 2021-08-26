library(tidyverse)
library(addPmData)

# get zcta h3 crosswalk
zcta_to_h3 <- s3::s3_get("s3://geomarker/h3/zcta_to_h3/zcta_2010_to_h3.rds") %>%
  readRDS()

# get list of zctas
options(tigris_use_cache = TRUE)
d_all <- tigris::zctas() %>%
  sf::st_drop_geometry() %>%
  select(zcta = ZCTA5CE10) %>%
  filter(zcta %in% zcta_to_h3$zcta)

# reads in individual chunk files and joins to data for h3-year
read_chunk_join_h3 <- function(fl, d) {
  chunk <- fst::read_fst(fl, as.data.table = TRUE)
  out <- left_join(d, chunk, by = "h3")
  rm(chunk)
  return(out)
}

# downloads all h3 chunk files one year at a time
get_chunk_yr <- function(d, yr) {
  fl_path <- s3::s3_get_files(
    s3_uri = glue::glue("s3://pm25-brokamp/{unique(d$h3_3)}_{yr}_h3pm.fst"),
    confirm = FALSE
  )
  on.exit(fs::dir_delete("s3_downloads/pm25-brokamp"))
  fl_path <- arrange(fl_path, file_path)

  d_split <- split(d, f = d$h3_3)

  d_pm <- map2(fl_path$file_path, d_split, read_chunk_join_h3)
  d_pm <- bind_rows(d_pm)

  d_pm <-
    d_pm %>%
    mutate(wt = weight * pm_pred) %>%
    group_by(zcta, date) %>%
    summarize(wt_pm_pred = sum(wt))

  return(d_pm)
}

# loops over all years
zcta_2dig_pm <- function(d) {
  dig2 <- substr(d$zcta[1], 1, 2)
  print(dig2)

  d <- d %>%
    left_join(zcta_to_h3, by = "zcta") %>%
    filter(!is.na(h3)) %>%
    mutate(h3_3 = map_chr(h3, ~ h3::h3_to_parent(.x, res = 3)))

  # safe harbor lookup
  d <- d %>%
    dplyr::left_join(addPmData:::safe_hex_lookup, by = "h3_3") %>%
    dplyr::mutate(h3_3 = ifelse(!is.na(safe_hex), safe_hex, h3_3)) %>%
    dplyr::select(-safe_hex)

  # exclude ZCTAs that require unavailable chunks
  exclude_zctas <- d %>%
    filter(
      !h3_3 %in% safe_harbor_h3_avail,
      !duplicated(zcta)
    ) %>%
    .$zcta

  write_csv(data.frame(exclude_zctas), "skipped_zctas.csv", append = T)

  d <- filter(d, !zcta %in% exclude_zctas)

  d_pm <- mappp::mappp(2000:2020, ~ get_chunk_yr(d = d, yr = .x))

  d_pm <- bind_rows(d_pm) %>%
    arrange(zcta, date)

  out_file_name <- glue::glue("zcta_2000/pm_{dig2}XXX.rds")
  saveRDS(d_pm, out_file_name)
  on.exit(fs::file_delete(out_file_name))

  system(glue::glue("aws s3 cp {out_file_name} s3://pm25-brokamp/{out_file_name}"))

}

# create csv file to keep skipped ZCTAs
fs::dir_create("zcta_2000")
blank <- data.frame("skipped" = "")
write_csv(x = blank, "skipped_zctas.csv")

# loop over all two-digit zctas
d_all_split <- split(d_all, f = substr(d_all$zcta, 1, 2), drop = TRUE)
walk(d_all_split, possibly(zcta_2dig_pm, NA))
