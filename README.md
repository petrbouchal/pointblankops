

<!-- README.md is generated from README.qmd. Please edit that file -->

# pointblankops

<!-- badges: start -->

<!-- badges: end -->

`{pointblankops}` extends the functionality of the
[`{pointblank}`](https://rstudio.github.io/pointblank/) to enable
efficient row-level failure reporting.

It adds the concept of **operatives** as lightweight alternatives to
pointblank’s agents, designed for streamlined data validation and
reporting. Operatives are memory-efficient and can handle large datasets
by processing them in chunks. You get information from them by
`debrief()`ing them, which returns a tibble of failures with specified
row identifiers.

## Installation

You can install the development version of pointblankops from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
remotes::install_github("petrbouchal/pointblankops")
```

## Example

This is a basic example which shows you how to use operatives for data
validation:

``` r
library(pointblankops)
library(dplyr)

# Create some test data
test_data <- data.frame(
  batch = c("A", "A", "B", "B", "C"),
  id = c(1, 2, 3, 4, 5),
  value = c(10, NA, 15, 8, 12),
  category = c("X", "Y", "X", "Z", "Y")
)

# Create an operative and add validation steps
operative <- create_operative(test_data) %>%
  pointblank::col_vals_not_null(columns = dplyr::vars(value)) %>%
  pointblank::col_vals_between(columns = dplyr::vars(value), left = 5, right = 20)

# Debrief the operative to get only the failures
failures <- debrief(operative, row_id_col = c("batch", "id"))
```

``` r
failures
#> # A tibble: 1 × 6
#>   batch id    test_name test_type         column_name failure_details           
#>   <chr> <chr> <chr>     <chr>             <chr>       <chr>                     
#> 1 A     2     step_1    col_vals_not_null value       Failed col_vals_not_null …
```

For database operations, install DBI package:

``` r
install.packages("DBI")
```

For parquet file operations, install arrow package:

``` r
install.packages("arrow")
```

## Key Features

- **Lightweight operatives**: Streamlined alternatives to pointblank
  agents
- **Memory-efficient processing**: Chunked processing for large
  datasets  
- **Multiple output formats**: Return tibbles, save to parquet files, or
  write to databases
- **Database compatibility**: Works with local data frames and database
  tables (DuckDB, SQLite)
- **Flexible ID columns**: Support for multiple row identifier columns
- **Consistent naming**: Follows pointblank’s playful terminology
  (agents → operatives, interrogate → debrief)
