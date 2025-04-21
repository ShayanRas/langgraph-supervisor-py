import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field
from langchain.tools import tool

# Assuming SessionLocal is correctly defined in connection.py
# If it's in a different location, adjust the import path.
from langgraph_supervisor.database_tools.connection import SessionLocal

# Set up logging
logger = logging.getLogger(__name__)

class ExecuteSQLInput(BaseModel):
    """Input schema for the execute_sql tool."""
    sql_query: str = Field(..., description="The raw SQL query to execute. Use parameterized queries where possible to prevent SQL injection.")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Optional dictionary of parameters to bind to the SQL query. Keys should match the named parameters in the query (e.g., :param_name).")
    fetch_results: bool = Field(True, description="Set to True to fetch results (e.g., for SELECT statements). Set to False for statements like INSERT, UPDATE, DELETE where results are not typically needed.")
    commit_transaction: bool = Field(True, description="Set to True to automatically commit the transaction after execution. Set to False if you need to manage transactions across multiple tool calls (use with caution).")

@tool(args_schema=ExecuteSQLInput)
def execute_sql(sql_query: str, parameters: Optional[Dict[str, Any]] = None, fetch_results: bool = True, commit_transaction: bool = True) -> Dict[str, Any]:
    """Executes a given SQL query against the configured database.

    Args:
        sql_query (str): The raw SQL query string to be executed. 
                         **WARNING:** Directly embedding user input into this query can lead to SQL injection vulnerabilities. 
                         Use the `parameters` argument for safe variable substitution whenever possible.
        parameters (Optional[Dict[str, Any]]): A dictionary of parameters to bind to the SQL query. 
                                               Keys should match named parameters in the query (e.g., `SELECT * FROM users WHERE id = :user_id`). 
                                               Using parameters is the **strongly recommended** way to include variable data in queries.
        fetch_results (bool): If True (default), attempts to fetch all results from the query (suitable for SELECT). 
                              If False, executes the query without fetching results (suitable for INSERT, UPDATE, DELETE).
        commit_transaction (bool): If True (default), the transaction is automatically committed upon successful execution. 
                                   If False, the transaction remains open, requiring a subsequent commit (e.g., via another tool or explicit call). 
                                   Set to False only if you have a specific need to manage transactions across multiple steps.

    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'status' (str): 'success' or 'error'.
            - 'message' (str, optional): A success message or error details.
            - 'results' (List[Dict[str, Any]], optional): A list of dictionaries representing the fetched rows, present only if 'fetch_results' was True and the query was successful.
            - 'row_count' (int, optional): The number of rows affected or returned (if available from the database driver), present on success.

    **Important Considerations:**
    - **Security:** NEVER construct SQL queries by directly concatenating strings with untrusted input. ALWAYS use the `parameters` argument to prevent SQL injection.
    - **Permissions:** The database user configured in the connection needs appropriate permissions to execute the provided query.
    - **Transaction Management:** Be mindful when setting `commit_transaction` to False. Uncommitted transactions can hold locks and consume resources. Ensure commits or rollbacks occur appropriately.
    - **Error Handling:** The tool attempts to catch common SQLAlchemy errors, but complex database issues might still arise.
    - **Resource Usage:** Very large queries or result sets can consume significant memory and time.
    """
    db = None
    try:
        db = SessionLocal()
        logger.info(f"Executing SQL (Fetch: {fetch_results}, Commit: {commit_transaction}): {sql_query}")
        if parameters:
            logger.info(f"With parameters: {parameters}")
            # Use text() for parameters with SQLAlchemy Core execute
            stmt = text(sql_query)
            result_proxy = db.execute(stmt, parameters)
        else:
            # Execute directly if no parameters
            result_proxy = db.execute(text(sql_query))

        results_data = []
        row_count = result_proxy.rowcount # Get row count (might be -1 if not applicable)

        if fetch_results:
            # Fetch all results and convert to list of dictionaries
            # .mappings() provides RowMapping, .all() gets all rows
            results_data = [dict(row) for row in result_proxy.mappings().all()]
            logger.info(f"Fetched {len(results_data)} rows.")
        else:
             logger.info(f"Executed statement, row count: {row_count}. Results not fetched.")

        if commit_transaction:
            db.commit()
            logger.info("Transaction committed successfully.")
            status_message = "Query executed and committed successfully."
        else:
            # If not committing here, the caller is responsible.
            # Flushing ensures statements are sent to DB without ending transaction.
            db.flush() 
            logger.info("Query executed, transaction flushed but not committed.")
            status_message = "Query executed. Transaction remains open (needs commit/rollback)."

        return {
            "status": "success",
            "message": status_message,
            "results": results_data if fetch_results else None,
            "row_count": row_count
        }

    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy Error executing query: {e}", exc_info=True)
        if db:
            db.rollback()
            logger.info("Transaction rolled back due to error.")
        return {"status": "error", "message": f"SQLAlchemy Error: {e}", "results": None, "row_count": -1}
    except Exception as e:
        logger.error(f"Unexpected Error executing query: {e}", exc_info=True)
        if db:
            db.rollback()
            logger.info("Transaction rolled back due to unexpected error.")
        return {"status": "error", "message": f"Unexpected Error: {e}", "results": None, "row_count": -1}
    finally:
        if db:
            db.close()
            logger.debug("Database session closed.")

# Example Usage (for testing purposes, not part of the tool itself)
if __name__ == '__main__':
    # Configure logging for testing
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # --- SELECT Example (Fetch Results) --- 
    print("\n--- SELECT Example ---")
    # Note: Table/data must exist in your DB
    select_query = "SELECT * FROM av_economic_data_feeds WHERE indicator_key = :key LIMIT :limit"
    select_params = {"key": "REAL_GDP", "limit": 2}
    select_result = execute_sql(sql_query=select_query, parameters=select_params, fetch_results=True, commit_transaction=False) # SELECT doesn't need commit usually
    print(f"SELECT Result: {select_result}")

    # --- INSERT Example (No Fetch, Commit) ---
    # print("\n--- INSERT Example (Illustrative - Modify Table/Cols) ---")
    # insert_query = "INSERT INTO your_table (column1, column2) VALUES (:val1, :val2)" # MODIFY TABLE/COLUMNS
    # insert_params = {"val1": "test_data", "val2": 123}
    # insert_result = execute_sql(sql_query=insert_query, parameters=insert_params, fetch_results=False, commit_transaction=True)
    # print(f"INSERT Result: {insert_result}")

    # --- Example without Parameters (Use with extreme caution) ---
    # print("\n--- SELECT Example (No Params - CAUTION) ---")
    # select_no_param = "SELECT COUNT(*) as count FROM av_economic_data_feeds"
    # select_no_param_result = execute_sql(sql_query=select_no_param, fetch_results=True)
    # print(f"SELECT (No Param) Result: {select_no_param_result}")

    # --- Error Example --- 
    print("\n--- Error Example (Invalid SQL) ---")
    error_query = "SELECT * FROM non_existent_table"
    error_result = execute_sql(sql_query=error_query, fetch_results=True)
    print(f"Error Result: {error_result}")