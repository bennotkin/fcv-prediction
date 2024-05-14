# Adjust codebook

# Write/append codebook-variables.csv to be used in codebook
vars <- names(read_csv("FCV_training_dataset.csv"))
if(!file.exists("codebook-variables.csv")) {
  codebook <- tibble(
    `Data Source` = str_extract(vars, "^([^_]*)"),
    `Variable Label` = vars,
    Definition = "")
  write_csv(codebook, "codebook-variables.csv")
} else {
  codebook <- read_csv("codebook-variables.csv", col_types = "ccc")
  # names(codebook) <- trimws(names(codebook))
  vars_included <- na.omit(str_replace_all(codebook$`Variable Label`, c("\\n|;\\s*" = "|", "_…" = ".*")))
  # new_vars <- vars[!str_detect(vars, paste0(vars_included, collapse = "|"))]
  var_matches <- str_match(vars, paste0("(", paste(vars_included, collapse = "$)|("), ")"))
  colnames(var_matches) <- c("variable", vars_included)
  new_vars <- vars[is.na(var_matches[,1])]
  removed_vars <- names(which(!matrixStats::colAnys(!is.na(var_matches[,-1]))))
  codebook_addition <- tibble(
    `Data Source` = str_extract(new_vars, "^([^_]*)"),
    `Variable Label` = new_vars,
    Definition = "")
  codebook <- bind_rows(codebook, codebook_addition) %>%
    filter(`Variable Label` %ni% removed_vars)
  # Arrange codebook alphabetically
  codebook <- bind_rows(
    filter(codebook, `Data Source` == "Ad-hoc"),
    filter(codebook, `Data Source` != "Ad-hoc" & !str_detect(tolower(`Data Source`), "trigger")) %>% arrange(`Data Source`, `Variable Label`),
    filter(codebook, str_detect(tolower(`Data Source`), "trigger")) %>% arrange(desc(`Data Source`))
    ) %>%
    sjmisc::replace_na(everything(), value = "")
  # Arrange codebook by appearance in dataset
  # var_order <- str_match(vars, paste0("(", paste0(vars_included, collapse = ")|("), ")")) %>% 
  #   head(n = 10) %>%
  #   .[,-1] %>%
  #   apply(2, \(x) which(!is.na(x))) %>% unlist()
  # codebook %>%
  # mutate(`Variable Label` = ordered(`Variable Label`, levels = unique(vars_included[var_order]))) %>%
  #   arrange(`Variable Label`)
  write_excel_csv(codebook, "codebook-variables.csv")
}

vars_included_now <- str_replace_all(codebook$`Variable Label`, c("\\n|;\\s*" = "|", "_…" = ".*"))
var_order <- str_match(vars, paste0("(", paste0(vars_included_now, collapse = ")|("), ")")) %>% 
  # .[1:10,1:10] %>%
  .[,-1] %>%
  apply(2, \(x) which(!is.na(x))) %>%
  unlist()
codebook %>%
  mutate(`Variable Label` = ordered(`Variable Label`, levels = unique(`Variable Label`[var_order]))) %>%
  arrange(`Variable Label`)
