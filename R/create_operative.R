#' Create operative for focused failure detection
#'
#' Creates a lightweight operative object for specialized data validation
#' intelligence gathering. Operatives are streamlined versions of pointblank
#' agents designed for efficient failure detection without the overhead of
#' full reporting capabilities.
#'
#' @param tbl A data.frame, tibble, or database table (tbl_sql) to validate
#' @param tbl_name Optional name for the table (if not provided, inferred from object)
#' @param label Optional label for the operative
#'
#' @return A pointblank agent object configured as an operative with class 
#'   `ptblank_operative`
#'
#' @examples
#' \dontrun{
#' # Create operative from data frame
#' data <- data.frame(id = 1:5, value = c(1, 2, NA, 4, 5))
#' operative <- create_operative(data) |>
#'   col_vals_not_null(columns = vars(value))
#' 
#' # Create operative from database table
#' con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
#' operative <- create_operative(tbl(con, "my_table")) |>
#'   col_vals_between(columns = vars(amount), left = 0, right = 1000)
#' }
#'
#' @export
create_operative <- function(tbl, 
                           tbl_name = NULL, 
                           label = NULL) {
  
  # Create a minimal agent with lightweight defaults
  operative <- pointblank::create_agent(
    tbl = tbl,
    tbl_name = tbl_name,
    label = label,
    actions = NULL,        # No actions for operatives - they just gather intel
    end_fns = NULL,        # No end functions
    embed_report = FALSE   # No embedded reports
  )
  
  # Add a class to identify it as an operative
  class(operative) <- c("ptblank_operative", class(operative))
  
  return(operative)
}