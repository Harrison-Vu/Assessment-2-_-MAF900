# ============================
# 0) Libraries
# ============================
install.packages(c("RPostgres","dplyr","tidyr","zoo","readxl","stringr","ggplot2"))
library(RPostgres)
library(dplyr)
library(tidyr)
library(zoo)
library(readxl)
library(stringr)
library(ggplot2)

install.packages("tidyverse") 
library(tidyverse)        

# ============================
# 1) Connect to WRDS + Pull Compustat
# ============================
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='harrisonvu')

qry <- "
SELECT gvkey, conm, fyear, naicsh, sich, 
       at, che, capx, xrd, sale, dltt, dlc,
       oibdp, xint, txt, dvc
FROM comp.funda
WHERE indfmt = 'INDL'
  AND datafmt = 'STD'
  AND popsrc = 'D'
  AND consol = 'C'
  AND fic = 'USA'
  AND fyear BETWEEN 2010 AND 2024
"

res   <- dbSendQuery(wrds, qry)
funda <- dbFetch(res, n = -1)
dbClearResult(res)

funda <- funda %>%
  mutate(sich = as.numeric(sich)) %>%
  filter(!(sich >= 4900 & sich <= 4999),   # drop utilities
         !(sich >= 6000 & sich <= 6999)) %>%  # drop financials
# if SIC is missing, fallback to NAICS rule: drop 22 (utilities) and 52 (finance/insurance)
  filter(!(is.na(sich) & str_detect(naicsh, "^(22|52)"))) %>%
  # ALSO drop by NAICS regardless, to catch any mapping mismatch
  filter(!(str_detect(naicsh, "^(22|52)")))


# ============================
# 2) Clean panel + core variables
# ============================

safe_div <- function(num, den) {
  ifelse(!is.na(den) & den != 0, num / den, NA_real_)
}


funda <- funda %>%
  # Convert to numeric
  mutate(across(c(at, che, capx, xrd, sale, dltt, dlc,
                  oibdp, xint, txt, dvc), as.numeric)) %>%
  # Keep rows with essential inputs present; require positive assets
  filter(!if_any(c(at, che, sale, oibdp, xint, txt, dvc, dltt, dlc), is.na),
         at > 0) %>%
  mutate(naics6 = suppressWarnings(as.integer(naicsh))) %>%
  group_by(gvkey) %>%
  arrange(fyear, .by_group = TRUE) %>%
  mutate(
    # Cash measures
    net_assets           = at - che,
    cash_to_net_assets   = safe_div(che, net_assets),         # Cash / (Assets - Cash)
    ln_cash_to_net_assets= ifelse(cash_to_net_assets > 0,
                                  log(cash_to_net_assets), NA_real_),
    # change in (Cash / Net Assets)
    d_cash_to_net_assets   = cash_to_net_assets - lag(cash_to_net_assets),
    
    # Size and growth
    size_ln_at   = ifelse(at > 0, log(at), NA_real_),
    sales_growth = safe_div(sale - lag(sale), lag(sale)),
    
    # Cash flow / assets
    cash_flow = safe_div(oibdp - xint - txt - dvc, at),
    
    # Rolling 5-year cash flow volatility
    cf_vol_5y = zoo::rollapplyr(cash_flow, width = 5, FUN = sd,
                                partial = FALSE, fill = NA_real_),
    
    # Leverage
    debt = ifelse(is.na(dltt) & is.na(dlc),
                           NA_real_, coalesce(dltt, 0) + coalesce(dlc, 0)),
    leverage_debt = safe_div(debt, at),
    
    # Investment & innovation
    capex_over_at = safe_div(capx, at),
    rd_over_sales = safe_div(xrd, sale),
    
    # Dividend dummy = 1 if firm pays dividend, 0 otherwise
    div_dummy = case_when(
      !is.na(dvc) & dvc > 0 ~ 1L,
      !is.na(dvc) & dvc == 0 ~ 0L,
      TRUE ~ NA_integer_)
  ) %>%
  ungroup()
 

#Winsorize to reduce influence of extreme but valid outliers

winsorize_vec <- function(x, probs = c(0.01, 0.99), na.rm = TRUE) {
  qs <- quantile(x, probs = probs, na.rm = na.rm)
  pmin(pmax(x, qs[1]), qs[2])
}

cols_winsor <- c(
  "ln_cash_to_net_assets","d_cash_to_net_assets","size_ln_at","sales_growth",
  "cash_flow","cf_vol_5y","leverage_debt","capex_over_at","rd_over_sales"
)

funda <- funda %>%
  mutate(across(all_of(cols_winsor), ~ winsorize_vec(.x, probs = c(0.01, 0.99))))

# ============================
# 3) Ingest WUI (USA only) from WUI Excel (sheet = "T2")
#     - Keep USA only
#     - Annualize by averaging quarters within year
# ============================
# Put the WUI excel file in your working directory and update the filename if needed
wui_t2 <- read_excel("WUI_Data.xlsx", sheet = "T2")

wui_long <- wui_t2 %>%
  filter(!is.na(year), str_detect(year, "^\\d{4}q[1-4]$")) %>%
  mutate(fyear = as.integer(str_sub(year, 1, 4))) %>%
  pivot_longer(cols = -c(year, fyear), names_to = "iso3", values_to = "wui")

wui_annual <- wui_long %>%
  filter(iso3 == "USA") %>%
  group_by(fyear) %>%
  summarise(wui_usa = mean(wui, na.rm = TRUE), .groups = "drop")

# Remove any previous WUI columns that may exist, then merge clean USA WUI
funda <- funda %>%
  select(-matches("(?i)^wui_")) %>%
  left_join(wui_annual, by = "fyear")

# ============================
# 4) Descriptive statistics table (example)
# ============================
mode <- function(x, na.rm = TRUE) {
  if (na.rm) x <- x[!is.na(x)]
  ux <- unique(x)
  if (length(ux) == 0) return(NA)   # handle empty case
  tab <- tabulate(match(x, ux))
  ux[which.max(tab)]
}


stat_row <- function(x, var_label) {
  tibble(
    Variable = var_label,
    Mean     = mean(x, na.rm = TRUE),
    Median   = median(x, na.rm = TRUE),
    Mode     = mode(x, na.rm = TRUE),
    `Standard Deviation` = sd(x, na.rm = TRUE),
    N        = sum(!is.na(x))
  )
}

dummy_row <- function(x, var_label) {
  tibble(
    Variable = var_label,
    Mean     = mean(x, na.rm = TRUE),   # interpreted as share of 1’s
    Median   = median(x, na.rm = TRUE),
    Mode     = mode(x, na.rm = TRUE),
    `Standard Deviation` = sd(x, na.rm = TRUE),
    N        = sum(!is.na(x))
  )
}


#---- Build the table row-by-row for available variables ----#
desc_tbl <- bind_rows(
  stat_row(funda$ln_cash_to_net_assets,"Ln (Cash/Net Assets)"),
  stat_row(funda$d_cash_to_net_assets, "Change in Cash/Net Assets"),
  if ("wui_usa" %in% names(funda)) stat_row(funda$wui_usa, "WUI") else NULL,
  dummy_row(funda$div_dummy,           "Dividend Dummy"),
  stat_row(funda$size_ln_at,           "Firm Size"),
  stat_row(funda$rd_over_sales,        "R&D/Sales"),
  stat_row(funda$cash_flow,            "Cash Flow/Total Assets"),
  stat_row(funda$capex_over_at,        "Capital Expenditures/Total Assets"),
  stat_row(funda$leverage_debt,        "Leverage"),
  stat_row(funda$sales_growth,         "Sales Growth"),
  stat_row(funda$cf_vol_5y,            "Cash Flow Volatility (5y)"),
) %>%
  mutate(across(c(Mean, Median, Mode, `Standard Deviation`), ~ round(.x, 3)))

desc_tbl





# ============================
# 5) Simple visuals 
# ============================



#1) Dual-axis line plot: WUI (right axis) vs Average Cash/Net Assets (left axis)

# --- Build annual means ---
annual <- funda %>%
  dplyr::group_by(fyear) %>%
  dplyr::summarise(
    mean_cna = mean(cash_to_net_assets, na.rm = TRUE),  # Average Cash / (Assets − Cash)
    wui      = mean(wui_usa, na.rm = TRUE),             # Average WUI (USA) per year
    N        = sum(!is.na(cash_to_net_assets)),
    .groups  = "drop"
  ) %>%
  dplyr::filter(!is.na(fyear), !is.na(mean_cna), !is.na(wui))

# --- Scale WUI to Cash/Net Assets axis (right -> left) ---
min_wui <- min(annual$wui, na.rm = TRUE);  max_wui <- max(annual$wui, na.rm = TRUE)
min_cna <- min(annual$mean_cna, na.rm = TRUE); max_cna <- max(annual$mean_cna, na.rm = TRUE)

b <- (max_cna - min_cna) / (max_wui - min_wui)
a <- min_cna - b * min_wui

annual_plot <- annual %>%
  dplyr::mutate(wui_scaled = a + b * wui)

# --- Plot: left axis = Avg Cash/Net Assets, right axis = WUI ---
ggplot(annual_plot, aes(x = fyear)) +
  geom_line(aes(y = mean_cna, color = "Cash/Net Assets"), linewidth = 1.1) +
  geom_point(aes(y = mean_cna, color = "Cash/Net Assets"), size = 1.8) +
  geom_line(aes(y = wui_scaled, color = "WUI"), linewidth = 1.1) +
  geom_point(aes(y = wui_scaled, color = "WUI"), size = 1.8) +
  
  scale_y_continuous(
    name = "Average Cash to Net Assets (%)",
    sec.axis = sec_axis(~ (.-a)/b, name = "World Uncertainty Index (points)")
  ) +
  scale_x_continuous(
    breaks = seq(min(annual_plot$fyear, na.rm = TRUE),
                 max(annual_plot$fyear, na.rm = TRUE),
                 by = 2)
  ) +
  scale_color_manual(
    NULL,
    values = c("Cash/Net Assets" = "red", "WUI" = "#00A6D6")
  ) +
  labs(
    title = "Cash Holdings and World Uncertainty Index",
    x = "Fiscal Year", y = NULL
  ) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position = "top",
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    axis.line.x = element_line(color = "black"),
    axis.line.y = element_line(color = "black"),
    axis.ticks = element_line(color = "grey45"),      
    axis.ticks.length = unit(-0.15, "cm"),
    plot.title = element_text(face = "bold"), 
    axis.text.x  = element_text(size = 11.7, color = "black"),
    axis.text.y  = element_text(size = 11.7, color = "black"),
  )

#2) Two-bar chart: Average Cash/Net Assets for Above vs Below Mean WUI

# Split at the overall mean WUI across firm-years
overall_mean_wui <- mean(funda$wui_usa, na.rm = TRUE)

funda_split <- funda %>%
  dplyr::mutate(
    wui_group = dplyr::case_when(
      !is.na(wui_usa) & wui_usa >= overall_mean_wui ~ "Above Average WUI",
      !is.na(wui_usa) & wui_usa <  overall_mean_wui ~ "Below Average WUI",
      TRUE ~ NA_character_
    )
  )

# Pick high-variation years 
years_focus <- c(2012, 2014, 2019, 2020, 2022)

bar_dat_overall <- funda_split %>%
  dplyr::filter(fyear %in% years_focus, !is.na(wui_group)) %>%
  dplyr::group_by(wui_group) %>%
  dplyr::summarise(
    mean_cna = mean(cash_to_net_assets, na.rm = TRUE),
    .groups  = "drop"
  )

ggplot(bar_dat_overall, aes(x = wui_group, y = mean_cna, fill = wui_group)) +
  geom_col(width = 0.6) +
  labs(
    title = "Average Cash Holdings and World Uncertainty Index",
    x = NULL,
    y = "Average Cash / (Assets − Cash)"
  ) +
  scale_y_continuous(
    expand = expansion(mult = c(0, 0.05))  
  ) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.6) +  
  theme_minimal(base_size = 13) +
  theme(
    legend.position = "none",
    panel.grid.major = element_line(color = "grey85", linewidth = 0.3),
    panel.grid.minor = element_blank(),
    panel.border = element_rect(color = "black", fill = NA, linewidth = 1), 
    axis.ticks.x = element_blank(),      
    axis.text.x  = element_text(color = "black", size = 13),
    axis.text.y  = element_text(color = "black", size = 13),
    axis.line = element_blank(),
    plot.title = element_text(face = "bold")
  )


