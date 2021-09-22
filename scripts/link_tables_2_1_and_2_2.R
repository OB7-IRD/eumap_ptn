# setup ----
library(furdeb)
library(readxl)
library(dplyr)

wd_path <- "D:\\projets_themes\\dpma\\eumap_ptn\\data\\"

# data importation ----
table_2_1 <- read_xlsx(path = paste0(wd_path,
                                     "WP AR template tables 2.1 2.2 2.5 donnees biologiques.xlsx"),
                       sheet = "Table 2.1 Stocks",
                       range = cell_limits(ul = c(2, 1),
                                           lr = c(NA, 16)))

table_2_2 <- read_xlsx(path = paste0(wd_path,
                                     "WP AR template tables 2.1 2.2 2.5 donnees biologiques.xlsx"),
                       sheet = "Table 2.2 Biol variables",
                       range = cell_limits(ul = c(2, 1),
                                           lr = c(NA, 15)))
# table 2.1 design ----
table_2_1_final <- table_2_1[-1, ] %>%
  left_join(as_tibble(unique(table_2_2[-1, c("MS",
                                             "Region",
                                             "RFMO/RFO/IO",
                                             "Species",
                                             "Area")])) %>%
              mutate(selected_sampling_biological_variables = "Y"),
            by = c("MS",
                   "Region",
                   "RFMO/RFO/IO",
                   "Species",
                   "Area")) %>%
  mutate(`Selected for sampling of biological variables` =  case_when(
    selected_sampling_biological_variables == "Y" ~ "Y",
    TRUE ~ "N"
  )) %>%
  select(-selected_sampling_biological_variables)

# extraction ----
write.csv2(x = table_2_1_final,
           file = paste0("D:\\projets_themes\\dpma\\eumap_ptn\\data",
                         "\\",
                         format(Sys.time()
                                ,"%Y%m%d_%H%M%S")
                         ,"_table_2_1.csv"),
           row.names = FALSE)

