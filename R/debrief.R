#' @importFrom rlang .data
#' @importFrom rlang %||%
NULL

#' Debrief operative and extract failure intelligence
#'
#' Extracts failure information from an operative (or agent) that has been
#' configured with validation steps. This is a memory-efficient alternative to
#' full interrogation that focuses only on identifying and reporting validation
#' failures.
#'
#' @param operative A pointblank agent or operative object with validation steps
#' @param row_id_col Character vector of column names to use as row identifiers.
#'   These columns will be included in the output to identify failing rows.
#' @param parquet_path Optional path to save failures as a parquet file. If provided,
#'   failures will be written to this file instead of returned as a tibble.
#'   Requires the 'arrow' package to be installed.
#' @param con Optional database connection to save failures. If provided, failures
#'   will be inserted into a database table. Requires the 'DBI' package to be installed.
#' @param output_tbl Optional table name for database output. If not provided and
#'   `con` is specified, the table name will be inferred as `{source_table}_failures`.
#' @param chunk_size Number of rows to process at once for memory efficiency.
#'   Default is 1000.
#'
#' @return If no output path is specified, returns a tibble containing failure
#'   records with ID columns, test metadata, and failure details. If `parquet_path`
#'   or `con` is provided, returns `NULL` invisibly after writing the failures.
#'
#' @examples
#' \dontrun{
#' # Basic usage - return failures as tibble
#' operative <- create_operative(mtcars) |>
#'   col_vals_not_null(columns = vars(mpg)) |>
#'   col_vals_between(columns = vars(cyl), left = 4, right = 8)
#' 
#' failures <- debrief(operative, row_id_col = c("gear", "carb"))
#' 
#' # Save to parquet file (requires arrow package)
#' # install.packages("arrow")
#' debrief(operative, row_id_col = "gear", parquet_path = "failures.parquet")
#' 
#' # Save to database (requires DBI package)
#' # install.packages("DBI")
#' con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
#' debrief(operative, row_id_col = "gear", con = con, output_tbl = "car_failures")
#' }
#'
#' @export
debrief <- function(operative, 
                   row_id_col,
                   parquet_path = NULL,
                   con = NULL,
                   output_tbl = NULL,
                   chunk_size = 1000) {
  
  # Validate inputs
  if (missing(row_id_col)) {
    stop("row_id_col parameter is required")
  }
  
  if (!is.character(row_id_col) || length(row_id_col) == 0) {
    stop("row_id_col must be a character vector with at least one column name")
  }
  
  # Determine output mode
  output_to_parquet <- !is.null(parquet_path)
  output_to_db <- !is.null(con)
  output_to_df <- !output_to_parquet && !output_to_db
  
  # Check for required packages based on output mode
  if (output_to_parquet) {
    check_pkg_available("arrow", "saving to parquet files")
  }
  
  if (output_to_db) {
    check_pkg_available("DBI", "database operations")
  }
  
  # Get the target table from operative (use the original table data)
  target_tbl <- operative$tbl
  validation_steps <- operative$validation_set
  
  # Determine output table name for database
  if (output_to_db) {
    if (is.null(output_tbl)) {
      # Infer table name from operative's table
      if (inherits(target_tbl, "tbl_sql")) {
        # For database tables, use the database table name from operative metadata
        if (!is.null(operative$db_tbl_name) && operative$db_tbl_name != "NA") {
          tbl_name <- operative$db_tbl_name
        } else {
          tbl_name <- "table"  # fallback name for database tables
        }
      } else {
        # For local data frames, use the table name from operative metadata
        if (!is.null(operative$tbl_name) && operative$tbl_name != "NA" && !grepl("operative\\$tbl", operative$tbl_name)) {
          tbl_name <- operative$tbl_name
        } else {
          tbl_name <- "data"  # fallback name
        }
      }
      failure_table_name <- paste0(tbl_name, "_failures")
    } else {
      failure_table_name <- output_tbl
    }
  } else {
    failure_table_name <- NULL
  }
  
  # Initialize output structures
  all_failures <- list()
  
  # Setup parquet writer or database table
  if (output_to_parquet) {
    # Ensure directory exists
    dir.create(dirname(parquet_path), recursive = TRUE, showWarnings = FALSE)
    
    # Remove existing parquet file to start fresh
    if (file.exists(parquet_path)) {
      file.remove(parquet_path)
    }
  }
  
  if (output_to_db) {
    # Drop existing failures table to start fresh
    drop_table_sql <- sprintf("DROP TABLE IF EXISTS %s", failure_table_name)
    DBI::dbExecute(con, drop_table_sql)
    
    # Create new failures table with dynamic ID columns
    id_columns_sql <- paste(sapply(row_id_col, function(col) paste(col, "TEXT")), collapse = ",\n        ")
    create_failures_table_sql <- sprintf("
      CREATE TABLE %s (
        %s,
        test_name TEXT,
        test_type TEXT,
        column_name TEXT,
        failure_details TEXT
      )", failure_table_name, id_columns_sql)
    
    DBI::dbExecute(con, create_failures_table_sql)
  }
  
  # Process each validation step
  for (i in seq_len(nrow(validation_steps))) {
    step <- validation_steps[i, ]
    
    # Extract validation parameters
    assertion_type <- step$assertion_type
    column <- step$column[[1]]  # Extract first column name from list
    values <- step$values
    preconditions <- step$preconditions
    
    # For now, ignore preconditions and work directly with target_tbl
    # Generate failure query directly
    base_query <- dplyr::select(target_tbl, dplyr::all_of(row_id_col), dplyr::all_of(column))
    
    test_query <- switch(assertion_type,
      "col_vals_not_null" = {
        dplyr::filter(base_query, is.na(!!dplyr::sym(column)))
      },
      "col_vals_null" = {
        dplyr::filter(base_query, !is.na(!!dplyr::sym(column)))
      },
      "col_vals_between" = {
        vals <- values[[1]]
        left_val <- vals[1]
        right_val <- vals[2]
        dplyr::filter(base_query, !(!!dplyr::sym(column) >= left_val & !!dplyr::sym(column) <= right_val))
      },
      "col_vals_not_between" = {
        vals <- values[[1]]
        left_val <- vals[1]
        right_val <- vals[2]
        dplyr::filter(base_query, !!dplyr::sym(column) >= left_val & !!dplyr::sym(column) <= right_val)
      },
      "col_vals_in_set" = {
        vals <- values[[1]]
        dplyr::filter(base_query, !(!!dplyr::sym(column) %in% vals))
      },
      "col_vals_not_in_set" = {
        vals <- values[[1]]
        dplyr::filter(base_query, !!dplyr::sym(column) %in% vals)
      },
      "col_vals_gt" = {
        threshold <- values[[1]]
        dplyr::filter(base_query, !(!!dplyr::sym(column) > threshold))
      },
      "col_vals_gte" = {
        threshold <- values[[1]]
        dplyr::filter(base_query, !(!!dplyr::sym(column) >= threshold))
      },
      "col_vals_lt" = {
        threshold <- values[[1]]
        dplyr::filter(base_query, !(!!dplyr::sym(column) < threshold))
      },
      "col_vals_lte" = {
        threshold <- values[[1]]
        dplyr::filter(base_query, !(!!dplyr::sym(column) <= threshold))
      },
      "col_vals_equal" = {
        expected_val <- values[[1]]
        dplyr::filter(base_query, !!dplyr::sym(column) != expected_val)
      },
      "col_vals_not_equal" = {
        expected_val <- values[[1]]
        dplyr::filter(base_query, !!dplyr::sym(column) == expected_val)
      },
      {
        # Skip unsupported assertion types
        message(paste("Skipping unsupported assertion type:", assertion_type))
        # Return empty query result with correct column structure
        base_query |> dplyr::filter(FALSE)
      }
    )
    
    # Execute test and get failures in chunks
    if (inherits(target_tbl, "tbl_sql")) {
      # For database tables - process in chunks and get failure records
      db_failure_records <- process_db_failures(test_query, step, chunk_size, 
                                               output_to_parquet, parquet_path,
                                               output_to_db, con, failure_table_name,
                                               output_to_df, row_id_col)
      
      # Accumulate failure records for data frame output
      if (output_to_df && !is.null(db_failure_records) && nrow(db_failure_records) > 0) {
        all_failures[[length(all_failures) + 1]] <- db_failure_records
      }
    } else {
      # For local tibbles/data.frames - use test_query directly since we already computed it
      failures <- test_query
      
      if (nrow(failures) > 0) {
        # Create standardized failure records
        failure_records <- create_failure_records(failures, step, row_id_col)
        
        # Output failures immediately
        if (output_to_parquet) {
          # Append to parquet file (file was cleared at start)
          if (file.exists(parquet_path)) {
            existing_data <- arrow::read_parquet(parquet_path)
            combined_data <- dplyr::bind_rows(existing_data, failure_records)
            arrow::write_parquet(combined_data, parquet_path)
          } else {
            arrow::write_parquet(failure_records, parquet_path)
          }
        }
        
        if (output_to_db) {
          # Insert into database table
          DBI::dbAppendTable(con, failure_table_name, failure_records)
        }
        
        if (output_to_df) {
          # Store in list for later combination
          all_failures[[length(all_failures) + 1]] <- failure_records
        }
      }
    }
  }
  
  # Return tibble if no output specified
  if (output_to_df && length(all_failures) > 0) {
    return(dplyr::bind_rows(all_failures))
  } else if (output_to_df) {
    # Return empty tibble with dynamic ID columns structure
    empty_data <- list(
      test_name = character(0),
      test_type = character(0),
      column_name = character(0),
      failure_details = character(0)
    )
    # Add ID columns to the list
    for (col in row_id_col) {
      empty_data[[col]] <- character(0)
    }
    # Convert to tibble
    return(tibble::tibble(!!!empty_data))
  }
  
  invisible(NULL)
}

#' Check if required package is available
#' @param pkg Package name
#' @param reason Description of why the package is needed
#' @keywords internal
check_pkg_available <- function(pkg, reason) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(
      paste0("Package '", pkg, "' is required for ", reason, ".\n",
             "Please install it with: install.packages('", pkg, "')"),
      call. = FALSE
    )
  }
}

#' Process failures for database tables
#' @keywords internal
process_db_failures <- function(failure_query, step, chunk_size,
                               output_to_parquet, parquet_path,
                               output_to_db, con, failure_table_name,
                               output_to_df, row_id_col) {
  
  # Count total failures first
  total_failures <- failure_query |> dplyr::summarise(n = dplyr::n()) |> dplyr::pull(.data$n)
  
  if (total_failures == 0) return(NULL)
  
  # For database backends, collect all failures at once to avoid slice() issues
  # Database queries are already optimized and chunking is less beneficial
  chunk_failures <- failure_query |> dplyr::collect()
  
  # Initialize list to collect all failure records for return
  all_failure_records <- list()
  
  if (nrow(chunk_failures) > 0) {
    # Process in chunks for memory efficiency after collecting
    n_failures <- nrow(chunk_failures)
    for (i in seq(1, n_failures, by = chunk_size)) {
      end_idx <- min(i + chunk_size - 1, n_failures)
      chunk <- chunk_failures[i:end_idx, ]
      
      # Create standardized failure records
      failure_records <- create_failure_records(chunk, step, row_id_col)
      
      # Output failures immediately
      if (output_to_parquet) {
        # Append to parquet file (file was cleared at start)
        if (file.exists(parquet_path)) {
          existing_data <- arrow::read_parquet(parquet_path)
          combined_data <- dplyr::bind_rows(existing_data, failure_records)
          arrow::write_parquet(combined_data, parquet_path)
        } else {
          arrow::write_parquet(failure_records, parquet_path)
        }
      }
      
      if (output_to_db) {
        # Insert into database table
        DBI::dbAppendTable(con, failure_table_name, failure_records)
      }
      
      # Collect failure records for data frame output
      if (output_to_df) {
        all_failure_records[[length(all_failure_records) + 1]] <- failure_records
      }
    }
  }
  
  # Return combined failure records for data frame output
  if (output_to_df && length(all_failure_records) > 0) {
    return(dplyr::bind_rows(all_failure_records))
  } else {
    return(NULL)
  }
}

#' Create standardized failure records
#' @keywords internal
create_failure_records <- function(failures, step, row_id_col) {
  if (nrow(failures) == 0) {
    # Create empty data frame with dynamic ID columns
    empty_data <- list(
      test_name = character(0),
      test_type = character(0),
      column_name = character(0),
      failure_details = character(0)
    )
    # Add ID columns to the list
    for (col in row_id_col) {
      empty_data[[col]] <- character(0)
    }
    # Convert to tibble
    return(tibble::tibble(!!!empty_data))
  }
  
  # Extract all ID columns separately and create failure data
  all_data <- list()
  
  # Add ID columns
  for (col in row_id_col) {
    all_data[[col]] <- as.character(failures[[col]])
  }
  
  # Add failure metadata
  column_name <- step$column[[1]] %||% "unknown"
  all_data$test_name <- paste0("step_", step$i %||% "unknown")
  all_data$test_type <- step$assertion_type %||% "unknown"
  all_data$column_name <- column_name
  all_data$failure_details <- paste("Failed", step$assertion_type, "on column", column_name)
  
  # Return as tibble
  tibble::tibble(!!!all_data)
}