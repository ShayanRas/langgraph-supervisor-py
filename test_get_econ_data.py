import asyncio
import logging
from sqlalchemy.sql import text
import sys
import os
import pandas as pd

# --- Add project root to sys.path --- 
# This ensures imports like 'from langgraph_supervisor...' work when running the script directly.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# ------------------------------------

# Configure logging for the test script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - TEST - %(levelname)s - %(message)s')

# Adjust the import path based on the script's location relative to the tool
# Assuming test_get_econ_data.py is in the project root
from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_econ_data import get_econ_data
# Import SessionLocal directly for verification purposes
from langgraph_supervisor.database_tools.connection import SessionLocal 

async def main():
    """Runs the test cases for the get_econ_data tool."""
    logging.info("--- Starting get_econ_data tool test --- ")

    # --- Test Case 1: Fetch Real GDP (Annual) and Write to DB ---
    indicator_to_test = 'real_gdp'
    interval_param = 'annual'
    maturity_param = None
    test_case_desc = f"{indicator_to_test} ({interval_param or 'default'} interval)"

    logging.info(f"Running Test Case 1: Fetching {test_case_desc} with DB write ENABLED")
    try:
        # Call the tool using the .invoke() method, passing arguments as a dictionary
        tool_input = {
            "indicator": indicator_to_test,
            "interval": interval_param,
            "maturity": maturity_param,
            "write_to_db": True
        }
        result = await get_econ_data.ainvoke(tool_input) # Use ainvoke for async tool

        logging.info(f"Tool Result for {test_case_desc}:")
        # Print metadata, especially DB status
        metadata = result.get('metadata', {})
        logging.info(f"  Metadata: {metadata}")
        # Print number of data points returned by API
        data_payload = result.get('data')
        data_points_count = 0
        if data_payload is not None and isinstance(data_payload, pd.DataFrame) and not data_payload.empty:
            data_points_count = len(data_payload)
        logging.info(f"  API Data Points Returned: {data_points_count}")

        # --- Verification Step (Optional but Recommended) ---
        db_status = metadata.get('database_status', 'Unknown')
        if SessionLocal and 'Completed' in db_status and 'Feed ID:' in db_status:
            try:
                feed_id_str = db_status.split('Feed ID:')[1].split(',')[0].strip()
                feed_id = int(feed_id_str)
                logging.info(f"Attempting DB verification for Feed ID: {feed_id}...")
                with SessionLocal() as db:
                    # Check data_feeds
                    feed_check = db.execute(text("SELECT status, row_count, error_message FROM data_feeds WHERE feed_id = :fid"), {'fid': feed_id}).fetchone()
                    if feed_check:
                        logging.info(f"  DB Verification (data_feeds): Status='{feed_check[0]}', RowCount={feed_check[1]}, Error='{feed_check[2]}'")
                    else:
                        logging.warning(f"  DB Verification Warning: Feed ID {feed_id} not found in data_feeds.")
                    
                    # Check av_economic_data_points count
                    point_count = db.execute(text("SELECT COUNT(*) FROM av_economic_data_points WHERE feed_id = :fid"), {'fid': feed_id}).scalar_one_or_none()
                    logging.info(f"  DB Verification (av_economic_data_points): Points found = {point_count}")

            except Exception as e:
                logging.error(f"  DB Verification Error for Feed ID {feed_id}: {e}", exc_info=True)
        elif 'Error' in db_status:
             logging.error(f"Database write reported an error: {metadata.get('database_error', 'No details')}")
        elif db_status == 'Write Disabled' or db_status == 'Skipped - No API data':
             logging.warning(f"Database write was not performed: {db_status}")
        elif db_status == 'Error: DB Connection not available' or db_status == 'Error: DB Connection/Session Failed':
             logging.error(f"Database write skipped due to connection/session issue: {db_status}")

    except Exception as e:
        logging.error(f"Error calling get_econ_data for {test_case_desc}: {e}", exc_info=True)

    logging.info("--- Test Finished --- ")


if __name__ == "__main__":
    # Ensure .env file is loaded (tool does it, but good practice here too)
    from dotenv import load_dotenv
    load_dotenv()
    asyncio.run(main())
