library(readxl)
library(dplyr)

wd_path <- "D:\\projets_themes\\dpma\\eumap_ptn\\data\\"
wd_path <- 'C:/_PROGRAMME/DCF/2021/COPIL DCF/PTN 2022/Elements du PTN/'

file_path <- "WP AR template tables 2.1 2.2 2.5 donnees biologiques"

# data importation ----
table_2_1 <- read_xlsx(path = paste0(wd_path,
                                     file_path,
                                     ".xlsx"),
                       sheet = "Table 2.1 Stocks",
                       range = cell_limits(ul = c(2, 1),
                                           lr = c(NA, 16)))

table_2_2 <- read_xlsx(path = paste0(wd_path,
                                     file_path,
                                     ".xlsx"),
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

# table 2.1 controls (we have to integrate that in the Joel control below) ----
control_consistency_2_1_and_2_2 <- filter(.data = table_2_1_final,
                                          `Selected for sampling of biological variables` == "Y") %>%
  select(MS,
         Region,
         Area,
         `RFMO/RFO/IO`,
         Species) %>%
  left_join(unique(select(.data = table_2_2,
                          MS,
                          Region,
                          Area,
                          `RFMO/RFO/IO`,
                          Species)) %>%
              mutate(consistency_2_1_and_2_2 = "Y"),
            by = c("MS",
                   "Region",
                   "Area",
                   "RFMO/RFO/IO",
                   "Species"))

# extraction ----
write.csv2(x = table_2_1_final,
           file = paste0(wd_path,"/",
                         format(Sys.time(),
                                "%Y%m%d_%H%M%S"),
                         "_table_2_1.csv"),
           row.names = FALSE)

# Table 2.2 control
names(table_2_1)[4] <- 'RFMO'
table_2_2_aggr <- aggregate(list(nVar = table_2_2$'Biological variable'),
                             by=list(MS=table_2_2$MS, Region=table_2_2$Region, RFMO=table_2_2$RFMO, Species=table_2_2$Species, Area=table_2_2$Area),
                                             function(x) length(unique(x)))
# tControl <- merge(table_2_1, table_2_2_aggr, by=c('MS','Region','RFMO','Species','Area'))
#
# Mcontrol <- 'FRA'
# Rcontrol <- 'Mediterranean and Black Sea'
# RFcontrol <- 'GFCM'
# Scontrol <- 'Engraulis encrasicolus'
#
# table_2_1[table_2_1$MS %in% Mcontrol & table_2_1$Region %in% Rcontrol & table_2_1$RFMO %in% RFcontrol & table_2_1$Species %in% Scontrol,]
# table_2_2[table_2_2$MS %in% Mcontrol & table_2_2$Region %in% Rcontrol & table_2_2$RFMO %in% RFcontrol & table_2_2$Species %in% Scontrol,]
# table_2_2_aggr[table_2_2_aggr$MS %in% Mcontrol &table_2_2_aggr$Region %in% Rcontrol & table_2_2_aggr$RFMO %in% RFcontrol & table_2_2_aggr$Species %in% Scontrol,]


### Basic checks
# 1 - Occurrence table with threshold and coverage for length sampling
table(table_2_1$`Threshold rules used`, table_2_1$`Covered by a commercial sampling scheme for length`)

# 2 - les spp*area > 200 t. sont échantillonnées en taille?
tmp <- table_2_1[as.numeric(table_2_1$'Average landings in the reference years (tonnes)') >=200,]
table(tmp$'Threshold rules used',tmp$'Covered by a commercial sampling scheme for length')
ind <- which(as.numeric(table_2_1$`Average landings in the reference years (tonnes)`)>=200 &
             table_2_1$`Threshold rules used` %in% 'None' &
             table_2_1$`Covered by a commercial sampling scheme for length` %in% 'N')

View(table_2_1[ind,])

### Consistency table_2_1 vs table_2_2
tControl <- merge(table_2_1, table_2_2_aggr, by=c('MS','Region','RFMO','Species','Area'), all=TRUE)

# 1 species*area covered for biological variables in table 2.1 absent in table 2.2
ind <- which(tControl$`Covered by a commercial sampling scheme for length` %in% 'Y' & is.na(tControl$nVar))
# number of occurrences of stocks said covered for a biological sampling in 2.1 and absent in table 2.2
table(tControl[ind,'Region'], tControl[ind, 'Selected for sampling of biological variables'])[,'Y']
pipo <- tControl[ind,]
# These are the stocks not in table 2.2 although they are said covered for biological sampling
pipo[pipo$`Selected for sampling of biological variables` %in% 'Y',c('Region','Species','Area')]

# 2 number of occurrences of stocks present in table 2.2 and not present in table 2.1
ind <- which(tControl$nVar>=1 & is.na(tControl$'Average landings in the reference years (tonnes)'))
table(tControl[ind,'Region'])
# Which one?
tControl[ind,c('Region','Species','Area')]

# 3 number of occurrences of stocks present in table 2.2 and in table 2.1
ind <- which(tControl$nVar>=1 & !(is.na(tControl$'Average landings in the reference years (tonnes)')))
table(tControl[ind,'Region'])



