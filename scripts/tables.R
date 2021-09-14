# setup ----
library(furdeb)
library(readxl)
library(dplyr)
config <- furdeb::configuration_file(path_file = "D:\\projets_themes\\dpma\\ptn\\ptn_2022_2024\\data\\configfile_ptn_2022_2024_mathieu.yml",
                                     silent = TRUE)

# parameters  ----
periode_reference <- as.integer(2017:2019)
countries <- c(1, 41)
implementation_year <- c(2022:2024)

t3_con <- furdeb::postgresql_db_connection(
  db_user = config[["databases_configuration"]][["t3_prod_vmot7"]][["login"]],
  db_password = config[["databases_configuration"]][["t3_prod_vmot7"]][["password"]],
  db_dbname = config[["databases_configuration"]][["t3_prod_vmot7"]][["dbname"]],
  db_host = config[["databases_configuration"]][["t3_prod_vmot7"]][["host"]],
  db_port = config[["databases_configuration"]][["t3_prod_vmot7"]][["port"]]
)

observe_con <- furdeb::postgresql_db_connection(
  db_user = config[["databases_configuration"]][["observe_vmot5"]][["login"]],
  db_password = config[["databases_configuration"]][["observe_vmot5"]][["password"]],
  db_dbname = config[["databases_configuration"]][["observe_vmot5"]][["dbname"]],
  db_host = config[["databases_configuration"]][["observe_vmot5"]][["host"]],
  db_port = config[["databases_configuration"]][["observe_vmot5"]][["port"]]
)

# data associated ----
iotc_data <- read_xlsx(path = paste0(config[["wd_path"]],
                                     "\\data\\IOTC-LATEST-NC-ALL-1950-2019_2021_05_21.xlsx"),
                       sheet = "Catches_Captures") %>%
  filter(`Year/An` %in% periode_reference
         & substr(Fleet, 1, 3) == "EU.")

iccat_data <- read_xlsx(path = paste0(config[["wd_path"]],
                                      "\\data\\t1nc-ALL_20201218.xlsx"),
                        sheet = "dsT1NC_v1",
                        range = cell_limits(ul = c(4, 1),
                                            lr = c(NA, 22))) %>%
  filter(YearC %in% periode_reference
         & substr(Fleet, 1, 3) == "EU.")

# table 2.1 ----
template_2_1 <- read_xlsx(path = paste0(config[["wd_path"]],
                                        "\\data\\2021.07.19_WP_2022-2024_TABLES_IRD.xlsx"),
                          sheet = "Table 2.1 Stocks",
                          range = cell_limits(ul = c(2, 1),
                                              lr = c(NA, 16)))

landings_sql <- paste(readLines(con = paste0(config[["wd_path"]],
                                             "\\scripts\\sql\\table_2_1_average_landings.sql")),
                      collapse = "\n")
landings_sql_final <- DBI::sqlInterpolate(conn = t3_con,
                                          sql = landings_sql,
                                          period = DBI::SQL(paste0(periode_reference,
                                                                   collapse = ", ")),
                                          countries = DBI::SQL(paste0("'",
                                                                      paste0(countries,
                                                                             collapse = "', '"),
                                                                      "'")))

landings <- DBI::dbGetQuery(conn = t3_con,
                            statement = landings_sql_final)

# manual correction link to missing information in the database referential (github ticket https://github.com/OB7-IRD/data-analysis/issues/128)
# no necessary after ticket resolution
for (a in seq_len(length.out = nrow(landings))) {
  if (landings[a, "label1"] %in% c("RIBEIRA", "POBRA DO CARAMINAL")) {
    landings[a, "ocean_code"] <- 1
  }
}

if (any(is.na(landings$ocean_code))) {
  stop("problem with the harbour-ocean referential")
}

# design for landings
landings_final <- landings %>%
  group_by(ocean_code, specie_code, specie_name) %>%
  summarise(mean_weight_t = mean(x = weight_t),
            .groups = "drop") %>%
  mutate(rfmo = case_when(
    ocean_code == 1 ~ "ICCAT",
    ocean_code == 2 ~ "IOTC",
    TRUE ~ "Referential_error"
  ))

# design for IOTC data
# work to do on the species from the PTN to optimise the correspondence with the RFMOs data
iotc_data_landings <- iotc_data %>%
  group_by(Fleet, SpLat) %>%
  summarise(total_landings = sum(`Catch/Capture(t)`),
            .groups = "drop")

iotc_data_landings_eu_france_total <- filter(.data = iotc_data_landings,
                                             substr(x = Fleet,
                                                    start = 1,
                                                    stop = 9) == "EU.FRANCE") %>%
  group_by(SpLat) %>%
  summarise(total_landings = sum(total_landings),
            .groups = "drop") %>%
  mutate(Fleet = "EU.FRANCE_total")

iotc_data_landings <- filter(.data = iotc_data_landings,
                             ! substr(x = Fleet,
                                      start = 1,
                                      stop = 9) == "EU.FRANCE") %>%
  bind_rows(iotc_data_landings_eu_france_total) %>%
  group_by(SpLat) %>%
  mutate(percentage_landings_iotc = total_landings / sum(total_landings) * 100,
         rfmo = "IOTC",
         SpLat = case_when(
           SpLat == "Makaira nigricans" ~ "Makaira nigricans (or mazara)",
           SpLat == "Carcharhinus falciformis" ~ "Carcharhinus falciformes",
           SpLat == "Carcharhinidae" ~ "Carcharhinus spp.",
           TRUE ~ SpLat
         )) %>%
  ungroup() %>%
  filter(Fleet == "EU.FRANCE_total")

# design for ICCAT data
# same comment than for IOTC data (example with Mobula spp.)
iccat_data_landings <- iccat_data %>%
  group_by(Fleet, ScieName) %>%
  summarise(total_landings = sum(Qty_t),
            .groups = "drop")

iccat_data_landings_eu_france_total <- filter(.data = iccat_data_landings,
                                              substr(x = Fleet,
                                                     start = 1,
                                                     stop = 6) == "EU.FRA") %>%
  group_by(ScieName) %>%
  summarise(total_landings = sum(total_landings),
            .groups = "drop") %>%
  mutate(Fleet = "EU.FRA_total")

iccat_data_landings <- filter(.data = iccat_data_landings,
                              ! substr(x = Fleet,
                                       start = 1,
                                       stop = 6) == "EU.FRA") %>%
  bind_rows(iccat_data_landings_eu_france_total) %>%
  group_by(ScieName) %>%
  mutate(percentage_landings_iccat = total_landings / sum(total_landings) * 100,
         rfmo = "ICCAT",
         ScieName = case_when(
           ScieName == "Makaira nigricans" ~ "Makaira nigricans (or mazara)",
           ScieName == "Carcharhinidae" ~ "Carcharhinus spp.",
           ScieName == "Mobulidae" ~ "Mobula spp.",
           TRUE ~ ScieName
         )) %>%
  ungroup() %>%
  filter(Fleet == "EU.FRA_total")

table_2_1 <- filter(.data = template_2_1,
                    `RFMO/RFO/IO` %in% c("ICCAT", "IOTC")) %>%
  left_join(landings_final[, c("specie_name",
                               "rfmo",
                               "mean_weight_t")],
            by = c("RFMO/RFO/IO" = "rfmo",
                   "Species" = "specie_name")) %>%
  left_join(iotc_data_landings[, c("rfmo"
                                   ,"SpLat"
                                   ,"percentage_landings_iotc")],
            by = c("RFMO/RFO/IO" = "rfmo",
                   "Species" = "SpLat")) %>%
  left_join(iccat_data_landings[, c("rfmo",
                                    "ScieName",
                                    "percentage_landings_iccat")],
            by = c("RFMO/RFO/IO" = "rfmo",
                   "Species" = "ScieName")) %>%
  mutate(`Average landings in the reference years (tonnes)` = as.character(trunc(mean_weight_t)),
         `Average landings in the reference years (tonnes)` = case_when(
           `RFMO/RFO/IO` %in% c("ICCAT", "IOTC") & is.na(mean_weight_t) ~ "None",
           TRUE ~ `Average landings in the reference years (tonnes)`
         ),
         `Data source used for average national landings` =  case_when(
           `RFMO/RFO/IO` %in% c("ICCAT", "IOTC") ~ "Logbooks",
           TRUE ~ ""),
         `Data source used for EU landings` =  case_when(
           `RFMO/RFO/IO` %in% c("ICCAT", "IOTC") ~ "RFMO statistics",
           TRUE ~ "")
         ,`Share (%) in EU landings` = case_when(
           `RFMO/RFO/IO` == "ICCAT" ~ as.character(percentage_landings_iccat),
           `RFMO/RFO/IO` == "IOTC" ~ as.character(percentage_landings_iotc),
           TRUE ~ ""
         ),
         `Reference period` = case_when(
           `RFMO/RFO/IO` %in% c("ICCAT", "IOTC") ~ "2017-2019",
           TRUE ~ ""
         ),
         MS = case_when(
           `RFMO/RFO/IO` %in% c("ICCAT", "IOTC") ~ "FRA",
           TRUE ~ ""
         ),
         `Share (%) in EU landings` = case_when(
           `RFMO/RFO/IO` %in% c("ICCAT", "IOTC") & is.na(`Share (%) in EU landings`) ~ "None",
           TRUE ~ `Share (%) in EU landings`
         ),
         `Regional coordination agreement at stock level` = case_when(
           `RFMO/RFO/IO` %in% c("ICCAT", "IOTC") ~ "N",
           TRUE ~ "")) %>%
  select(-mean_weight_t,
         -percentage_landings_iotc,
         -percentage_landings_iccat)

# table 2.2 ----
# template importation
template_2_2 <- read_xlsx(path = paste0(config[["wd_path"]],
                                        "\\data\\2021.07.19_WP_2022-2024_TABLES_IRD.xlsx"),
                          sheet = "Table 2.2 Biol variables",
                          range = cell_limits(ul = c(2, 1),
                                              lr = c(NA, 15)))
# sql queries
ps_nontarget_samples_sql <- paste(readLines(con = paste0(config[["wd_path"]],
                                                         "\\scripts\\sql\\table_2_2_ps_nontarget_samples.sql")),
                                  collapse = "\n")
ps_nontarget_samples_sql_final <- DBI::sqlInterpolate(conn = observe_con,
                                                      sql = ps_nontarget_samples_sql,
                                                      period = DBI::SQL(paste0(periode_reference,
                                                                               collapse = ", ")),
                                                      countries = DBI::SQL(paste0(paste0(countries,
                                                                                         collapse = ", "))))

ps_nontarget_samples <- DBI::dbGetQuery(conn = observe_con,
                                        statement = ps_nontarget_samples_sql_final)


ps_target_samples_sql <- paste(readLines(con = paste0(config[["wd_path"]],
                                                      "\\scripts\\sql\\table_2_2_ps_target_samples.sql")),
                               collapse = "\n")
ps_target_samples_sql_final <- DBI::sqlInterpolate(conn = observe_con,
                                                   sql = ps_target_samples_sql,
                                                   period = DBI::SQL(paste0(periode_reference,
                                                                            collapse = ", ")),
                                                   countries = DBI::SQL(paste0(paste0(countries,
                                                                                      collapse = ", "))))

ps_target_samples <- DBI::dbGetQuery(conn = observe_con,
                                     statement = ps_target_samples_sql_final)

landings_samples_sql <- paste(readLines(con = paste0(config[["wd_path"]],
                                                     "\\scripts\\sql\\table_2_2_landings_samples.sql")),
                              collapse = "\n")
landings_samples_sql_final <- DBI::sqlInterpolate(conn = t3_con,
                                                  sql = landings_samples_sql,
                                                  period = DBI::SQL(paste0(periode_reference,
                                                                           collapse = ", ")),
                                                  countries = DBI::SQL(paste0(paste0(countries,
                                                                                     collapse = ", "))))

landings_samples <- DBI::dbGetQuery(conn = t3_con,
                                    statement = landings_samples_sql_final)
# design on landings length samples
landings_samples_length <- landings_samples %>%
  group_by(landing_year,
           ocean_code,
           specie_name) %>%
  summarise(number_sample = sum(count),
            .groups = "drop") %>%
  group_by(ocean_code,
           specie_name) %>%
  summarise(count = n(),
            .groups = "drop") %>%
  filter(count >= 3) %>%
  mutate(covered_length = "Y",
         Region = "Other regions",
         `RFMO/RFO/IO` = case_when(
           ocean_code == 1 ~ "ICCAT",
           ocean_code == 2 ~ "IOTC",
           TRUE ~ "error"
         )) %>%
  rename(`Species` = specie_name) %>%
  select(-ocean_code,
         -count)
# design on biological variables sampled (all variables except length from observe database)
ps_biological_variables_offshore <- data.frame()
for (year in unique(ps_nontarget_samples$sampling_year)) {
  ps_nontarget_samples_year <- filter(.data = ps_nontarget_samples,
                                      sampling_year == year)
  for (ocean in unique(ps_nontarget_samples_year$ocean_code)) {
    ps_nontarget_samples_year_ocean <- filter(.data = ps_nontarget_samples_year,
                                              ocean_code == ocean)
    for (specie in unique(ps_nontarget_samples_year_ocean$specie_name)) {
      ps_nontarget_samples_year_ocean_specie <- filter(.data = ps_nontarget_samples_year_ocean,
                                                       specie_name == specie)
      if (any(! is.na(ps_nontarget_samples_year_ocean_specie$weight)
              && ps_nontarget_samples_year_ocean_specie$weight != 0)) {
        ps_nontarget_samples_year_ocean_specie_weight <- filter(.data = ps_nontarget_samples_year_ocean_specie,
                                                                ps_nontarget_samples_year_ocean_specie$weight != 0)
        ps_biological_variables_offshore <- rbind(ps_biological_variables_offshore,
                                                  data.frame("year" = year,
                                                             "region" = "Other regions",
                                                             "rfmo" = ocean,
                                                             "specie" = specie,
                                                             "biological_variable" = "Weight",
                                                             "number_sample" = sum(ps_nontarget_samples_year_ocean_specie$count)))
        rm(ps_nontarget_samples_year_ocean_specie_weight)
      }
      if (any(! is.na(ps_nontarget_samples_year_ocean_specie$length)
              && ps_nontarget_samples_year_ocean_specie$length != 0)) {
        ps_nontarget_samples_year_ocean_specie_length <- filter(.data = ps_nontarget_samples_year_ocean_specie,
                                                                ps_nontarget_samples_year_ocean_specie$length != 0)
        ps_biological_variables_offshore <- rbind(ps_biological_variables_offshore,
                                                  data.frame("year" = year,
                                                             "region" = "Other regions",
                                                             "rfmo" = ocean,
                                                             "specie" = specie,
                                                             "biological_variable" = "Length",
                                                             "number_sample" = sum(ps_nontarget_samples_year_ocean_specie_length$count)))
        rm(ps_nontarget_samples_year_ocean_specie_length)
      }
      if (any(ps_nontarget_samples_year_ocean_specie$sex %in% c("Male",
                                                                "Female",
                                                                "Juvenile"))) {
        ps_nontarget_samples_year_ocean_specie_sex <- filter(.data = ps_nontarget_samples_year_ocean_specie,
                                                             sex %in% c("Male",
                                                                        "Female",
                                                                        "Juvenile"))
        ps_biological_variables_offshore <- rbind(ps_biological_variables_offshore,
                                                  data.frame("year" = year,
                                                             "region" = "Other regions",
                                                             "rfmo" = ocean,
                                                             "specie" = specie,
                                                             "biological_variable" = "Sex",
                                                             "number_sample" = sum(ps_nontarget_samples_year_ocean_specie_sex$count)))
        rm(ps_nontarget_samples_year_ocean_specie_sex)
      }
    }
  }
}
rm(ps_nontarget_samples_year,
   ps_nontarget_samples_year_ocean,
   ps_nontarget_samples_year_ocean_specie)

for (year in unique(ps_target_samples$sampling_year)) {
  ps_target_samples_year <- filter(.data = ps_target_samples,
                                   sampling_year == year)
  for (ocean in unique(ps_target_samples_year$ocean_code)) {
    ps_target_samples_year_ocean <- filter(.data = ps_target_samples_year,
                                           ocean_code == ocean)
    for (specie in unique(ps_target_samples_year_ocean$specie_name)) {
      ps_target_samples_year_ocean_specie <- filter(.data = ps_target_samples_year_ocean,
                                                    specie_name == specie)
      if (any(! is.na(ps_target_samples_year_ocean_specie$weight)
              && ps_target_samples_year_ocean_specie$weight != 0)) {
        ps_target_samples_year_ocean_specie_weight <- filter(.data = ps_target_samples_year_ocean_specie,
                                                             ps_target_samples_year_ocean_specie$weight != 0)
        ps_biological_variables_offshore <- rbind(ps_biological_variables_offshore,
                                                  data.frame("year" = year,
                                                             "region" = "Other regions",
                                                             "rfmo" = ocean,
                                                             "specie" = specie,
                                                             "biological_variable" = "Weight",
                                                             "number_sample" = sum(ps_target_samples_year_ocean_specie$count)))
        rm(ps_target_samples_year_ocean_specie_weight)
      }
      if (any(! is.na(ps_target_samples_year_ocean_specie$length)
              && ps_target_samples_year_ocean_specie$length != 0)) {
        ps_target_samples_year_ocean_specie_length <- filter(.data = ps_target_samples_year_ocean_specie,
                                                             ps_target_samples_year_ocean_specie$length != 0)
        ps_biological_variables_offshore <- rbind(ps_biological_variables_offshore,
                                                  data.frame("year" = year,
                                                             "region" = "Other regions",
                                                             "rfmo" = ocean,
                                                             "specie" = specie,
                                                             "biological_variable" = "Length",
                                                             "number_sample" = sum(ps_target_samples_year_ocean_specie_length$count)))
        rm(ps_target_samples_year_ocean_specie_length)
      }
      if (any(ps_target_samples_year_ocean_specie$sex %in% c("Male",
                                                             "Female",
                                                             "Juvenile"))) {
        ps_target_samples_year_ocean_specie_sex <- filter(.data = ps_target_samples_year_ocean_specie,
                                                          sex %in% c("Male",
                                                                     "Female",
                                                                     "Juvenile"))
        ps_biological_variables_offshore <- rbind(ps_biological_variables_offshore,
                                                  data.frame("year" = year,
                                                             "region" = "Other regions",
                                                             "rfmo" = ocean,
                                                             "specie" = specie,
                                                             "biological_variable" = "Sex",
                                                             "number_sample" = sum(ps_target_samples_year_ocean_specie_sex$count)))
        rm(ps_target_samples_year_ocean_specie_sex)
      }
    }
  }
}
rm(ps_target_samples_year,
   ps_target_samples_year_ocean,
   ps_target_samples_year_ocean_specie)

biological_variables_template <- c("Age",
                                   "Weight",
                                   "Sex",
                                   "Maturity",
                                   "Fecundity")
ps_biological_variables_offshore_new <- data.frame()
for (ocean in unique(filter(.data = ps_biological_variables_offshore,
                            biological_variable != "Length")$rfmo)) {
  ps_biological_variables_offshore_ocean <- filter(.data = ps_biological_variables_offshore,
                                                   rfmo == ocean
                                                   & biological_variable != "Length")
  for (specie_id in unique(ps_biological_variables_offshore_ocean$specie)) {
    ps_biological_variables_offshore_ocean_specie <- filter(.data = ps_biological_variables_offshore_ocean,
                                                            specie == specie_id)
    for (biological_variable_id in unique(ps_biological_variables_offshore_ocean_specie$biological_variable)) {
      ps_biological_variables_offshore_ocean_specie_biovariable <- filter(.data = ps_biological_variables_offshore_ocean_specie,
                                                                          biological_variable == biological_variable_id)
      if (nrow(x = ps_biological_variables_offshore_ocean_specie_biovariable) != length(periode_reference)) {
        for (new_year in setdiff(x = periode_reference,
                                 y = ps_biological_variables_offshore_ocean_specie_biovariable$year)) {
          ps_biological_variables_offshore_new <- rbind(ps_biological_variables_offshore_new,
                                                        data.frame("year" = new_year,
                                                                   "region" = "Other regions",
                                                                   "rfmo" = ocean,
                                                                   "specie" = specie_id,
                                                                   "biological_variable" = biological_variable_id,
                                                                   "number_sample" = 0))
        }
      }
    }
    if (! all(biological_variables_template %in% unique(ps_biological_variables_offshore_ocean_specie$biological_variable))) {
      for (new_variable in setdiff(x = biological_variables_template,
                                   y = unique(ps_biological_variables_offshore_ocean_specie$biological_variable))) {
        for (new_year in periode_reference) {
          ps_biological_variables_offshore_new <- rbind(ps_biological_variables_offshore_new,
                                                        data.frame("year" = new_year,
                                                                   "region" = "Other regions",
                                                                   "rfmo" = ocean,
                                                                   "specie" = specie_id,
                                                                   "biological_variable" = new_variable,
                                                                   "number_sample" = 0))
        }
      }
    }
  }
}
rm(ocean,
   specie_id,
   biological_variable_id,
   new_year,
   new_variable,
   ps_biological_variables_offshore_ocean,
   ps_biological_variables_offshore_ocean_specie,
   ps_biological_variables_offshore_ocean_specie_biovariable)

ps_biological_variables_offshore_final <- filter(.data = ps_biological_variables_offshore,
                                                 biological_variable != "Length") %>%
  bind_rows(ps_biological_variables_offshore_new) %>%
  group_by(region,
           rfmo,
           specie,
           biological_variable) %>%
  summarise(mean_samples = as.character(mean(number_sample)),
            .groups = "drop") %>%
  mutate(MS = "FRA",
         rfmo = case_when(
           rfmo == 1 ~ "ICCAT",
           rfmo == 2 ~ "IOTC",
           TRUE ~ "error"
         ),
         Area = "All area",
         `Observation type` = "SciObsAtSea",
         `Sampling scheme type` = "Commercial fishing trip",
         `Sampling scheme identifier` = paste(`Observation type`,
                                              `Sampling scheme type`,
                                              sep = "_"),
         `Regional work plan name` = "N") %>%
  rename(Region = region,
         `RFMO/RFO/IO` = rfmo,
         `Species` = specie,
         `Biological variable` = biological_variable,
         average_individuals_sampled_last_3_years = mean_samples) %>%
  arrange(`RFMO/RFO/IO`,
           `Species`,
           `Biological variable`)

table_2_2 <- template_2_2[-c(1:nrow(template_2_2)),] %>%
  bind_rows(ps_biological_variables_offshore_final) %>%
  mutate(`Implementation year` = "2022-2024")

# design on offshore length samples
ps_biological_variables_offshore_length <- filter(.data = ps_biological_variables_offshore,
                                                  biological_variable == "Length") %>%
  group_by(region,
           rfmo,
           specie) %>%
  summarise(count = n(),
            .groups = "drop") %>%
  filter(count >= 3) %>%
  mutate(covered_length = "Y",
         `RFMO/RFO/IO` = case_when(
           rfmo == 1 ~ "ICCAT",
           rfmo == 2 ~ "IOTC",
           TRUE ~ "error"
         )) %>%
  rename(`Species` = specie,
         Region = region) %>%
  select(-count,
         -rfmo)

# Link with the table 2.1 and biological variables sampled
final_samples_length <- full_join(x = ps_biological_variables_offshore_length,
                                  y = landings_samples_length,
                                  by = c("Region",
                                         "Species",
                                         "covered_length",
                                         "RFMO/RFO/IO"))

ps_biological_variables_sampling <- unique(ps_biological_variables_offshore_final[,c("Region",
                                                                                     "RFMO/RFO/IO",
                                                                                     "Species")]) %>%
  mutate("selected_sampling" = "Y")

table_2_1_final <- table_2_1 %>%
  left_join(ps_biological_variables_sampling,
            by = c("Region",
                   "RFMO/RFO/IO",
                   "Species")) %>%
  left_join(final_samples_length,
            by = c("Region",
                   "RFMO/RFO/IO",
                   "Species")) %>%
  mutate(`Covered by a commercial sampling scheme for length`= case_when(
    covered_length == "Y" ~ "Y",
    TRUE ~ "N"
  ),
  `Selected for sampling of biological variables` = case_when(
    selected_sampling == "Y" ~ "Y",
    TRUE ~ "N"
  )) %>%
  select(-covered_length,
         -selected_sampling)

# table 2.5 ----

# extraction ----
write.csv2(x = table_2_1_final,
           file = paste0(config[["output_path"]],
                         "\\",
                         format(Sys.time()
                                ,"%Y%m%d_%H%M%S")
                         ,"_table_2_1.csv"),
           row.names = FALSE)

write.csv2(x = table_2_2,
           file = paste0(config[["output_path"]],
                         "\\",
                         format(Sys.time()
                                ,"%Y%m%d_%H%M%S")
                         ,"_table_2_2.csv"),
           row.names = FALSE)








