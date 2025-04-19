from alpha_vantage.econindicators import EconIndicators
import os
import dotenv
from typing import Dict, Any, Optional, List, Union
import asyncio
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import InjectedToolArg, tool
from typing_extensions import Annotated
import logging
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from decimal import Decimal, InvalidOperation
import pandas as pd

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from langgraph_supervisor.database_tools.connection import SessionLocal

logger = logging.getLogger(__name__)

@tool
async def get_econ_data(
    indicator: str,
    write_to_db: bool = False,
    interval: Optional[str] = None,
    maturity: Optional[str] = None,
    *,
    config: Annotated[RunnableConfig, InjectedToolArg]
) -> Dict[str, Any]:
    """Get economic indicator data from Alpha Vantage.

    Use this tool to retrieve various US economic indicators such as GDP, inflation,
    treasury yields, unemployment, and more. The data is sourced from Alpha Vantage's
    Economic Indicators API.

    Args:
        indicator: The economic indicator to retrieve. Options include 'real_gdp', 'real_gdp_per_capita', 'treasury_yield', 'federal_funds_rate', 'cpi', 'inflation', 'retail_sales', 'durables', 'unemployment', 'nonfarm_payroll'.
        write_to_db: If True, writes the fetched data to the database.
        interval: The time interval for data points. Options: 'real_gdp': 'annual'/'quarterly'; 'treasury_yield': 'daily'/'weekly'/'monthly'; 'federal_funds_rate': 'daily'/'weekly'/'monthly'; 'cpi': 'semiannual'/'monthly'.
        maturity: For treasury_yield only - the maturity period of the treasury bond.
        config: Runtime configuration (automatically injected).

    Returns:
        A dictionary containing the economic indicator data and metadata.
    """
    # Define a synchronous helper function to run in the thread
    def _get_and_convert(
        indicator_key: str,
        write_db_flag: bool,
        interval_param: Optional[str],
        maturity_param: Optional[str],
    ) -> Dict[str, Any]:
        api_data: Optional[list] = None
        api_meta_data: Optional[dict] = None
        db_status = "Write Disabled"
        db_error = None
        feed_id_created = None
        db_op_exception = None

        try:
            api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            if not api_key:
                raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables.")

            ei = EconIndicators(key=api_key, output_format='json')
            logger.info(f"Fetching '{indicator_key}' (Interval: {interval_param}, Maturity: {maturity_param})")

            # --- Map indicator to appropriate method --- 
            if indicator_key == 'real_gdp':
                if interval_param and interval_param in ['annual', 'quarterly']:
                    api_data, api_meta_data = ei.get_real_gdp(interval=interval_param)
                else:
                    api_data, api_meta_data = ei.get_real_gdp()  # Default is annual
        
            elif indicator_key == 'real_gdp_per_capita':
                api_data, api_meta_data = ei.get_real_gdp_per_capita()
        
            elif indicator_key == 'treasury_yield':
                if interval_param and maturity_param:
                    api_data, api_meta_data = ei.get_treasury_yield(interval=interval_param, maturity=maturity_param)
                elif interval_param:
                    api_data, api_meta_data = ei.get_treasury_yield(interval=interval_param)  # Default maturity is 10year
                elif maturity_param:
                    api_data, api_meta_data = ei.get_treasury_yield(maturity=maturity_param)  # Default interval is monthly
                else:
                    api_data, api_meta_data = ei.get_treasury_yield()  # Default is monthly, 10year
        
            elif indicator_key == 'federal_funds_rate':
                if interval_param and interval_param in ['daily', 'weekly', 'monthly']:
                    api_data, api_meta_data = ei.get_ffr(interval=interval_param)
                else:
                    api_data, api_meta_data = ei.get_ffr()  # Default is monthly
        
            elif indicator_key == 'cpi':
                if interval_param and interval_param in ['semiannual', 'monthly']:
                    api_data, api_meta_data = ei.get_cpi(interval=interval_param)
                else:
                    api_data, api_meta_data = ei.get_cpi()  # Default is monthly
        
            elif indicator_key == 'inflation':
                api_data, api_meta_data = ei.get_inflation()
        
            elif indicator_key == 'retail_sales':
                api_data, api_meta_data = ei.get_retail_sales()
        
            elif indicator_key == 'durables':
                api_data, api_meta_data = ei.get_durables()
        
            elif indicator_key == 'unemployment':
                api_data, api_meta_data = ei.get_unemployment()
        
            elif indicator_key == 'nonfarm_payroll':
                api_data, api_meta_data = ei.get_nonfarm_payroll()
        
            else:
                raise ValueError(f"Unknown indicator: {indicator_key}")
        
            logger.info(f"Successfully fetched data for '{indicator_key}'.")

            # --- Database Write Logic --- 
            if write_db_flag:
                db_session = SessionLocal() # Get session
                if db_session:
                    db_session.begin() # Start transaction
                    try:
                        # 1. Insert into data_feeds
                        # Safely get name and unit, providing defaults if metadata is None
                        api_name = indicator_key # Default name
                        api_unit = 'unknown'   # Default unit
                        if api_meta_data:
                            api_name = api_meta_data.get('Name') or api_meta_data.get('2. Name') or indicator_key
                            api_unit = api_meta_data.get('Unit') or api_meta_data.get('4. Unit') or 'unknown'

                        feed_sql = text("""
                            INSERT INTO data_feeds (indicator_key, interval_param, maturity_param, api_indicator_name, api_unit, status)
                            VALUES (:ikey, :inter, :mat, :name, :unit, 'new')
                            RETURNING feed_id;
                        """)
                        feed_result = db_session.execute(feed_sql, {
                            'ikey': indicator_key, 'inter': interval_param,
                            'mat': maturity_param, 'name': api_name, 
                            'unit': api_unit
                        }).fetchone()

                        if not feed_result:
                             raise Exception("Failed to create data_feeds record.")
                        feed_id = feed_result[0]
                        feed_id_created = feed_id
                        logger.info(f"Created data_feeds record with feed_id: {feed_id}")

                        # 2. Prepare data points from DataFrame
                        points_to_insert = []
                        # Check if api_data is a pandas DataFrame and not empty
                        if isinstance(api_data, pd.DataFrame) and not api_data.empty:
                            logger.info(f"Processing {len(api_data)} rows from DataFrame for feed_id {feed_id}")
                            for index, row in api_data.iterrows(): # index is likely int, row contains data
                                value_str = row.get('value') # Get value from the row
                                date_str = row.get('date')   # Get date string from the row

                                # Check if date or value is missing in the row
                                if date_str is None or value_str is None:
                                     logger.warning(f"Skipping row: missing date ('{date_str}') or value ('{value_str}') for index {index}, feed {feed_id}")
                                     continue
                                
                                # Try parsing the date string
                                try:
                                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                                except ValueError as e_date:
                                    logger.warning(f"Skipping row: date parse error '{e_date}' for date string '{date_str}', index {index}, feed {feed_id}")
                                    continue
                                
                                # Convert value_str for DB insertion
                                value_str = str(value_str) # Ensure it's a string first
                                parsed_numeric = None
                                parsed_text = value_str # Store original string representation

                                try:
                                    # Attempt to convert to Decimal
                                    parsed_numeric = Decimal(value_str)
                                except InvalidOperation:
                                    # Handle specific non-numeric strings if needed
                                    if value_str.lower() == 'none' or value_str == '.':
                                        logger.info(f"Value is '{value_str}', storing NULL numeric. Date: {parsed_date}, Feed: {feed_id}")
                                        # parsed_numeric remains None
                                    else:
                                        logger.warning(f"Cannot parse value '{value_str}' as Decimal. Storing NULL numeric. Date: {parsed_date}, Feed: {feed_id}")
                                        # parsed_numeric remains None
                                except Exception as e_conv: # Catch any other conversion error
                                    logger.warning(f"Error converting value '{value_str}' to Decimal: {e_conv}. Storing NULL numeric. Date: {parsed_date}, Feed: {feed_id}")
                                    # parsed_numeric remains None

                                points_to_insert.append({
                                    'feed_id': feed_id, 
                                    'data_date': parsed_date,
                                    'value_numeric': parsed_numeric, 
                                    'value_text': parsed_text
                                })
                        elif isinstance(api_data, list):
                             # Keep the old list logic as a fallback, though unlikely for econ indicators
                             logger.warning(f"API data is a list, processing using list logic for feed {feed_id}.")
                             for point in api_data:
                                 date_str = point.get('date')
                                 value_str = point.get('value')
 
                                 if not date_str or value_str is None: 
                                     logger.warning(f"Skipping list point: missing date/value {point} for feed {feed_id}")
                                     continue
                                 try:
                                     parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                                     parsed_numeric = None
                                     parsed_text = value_str
                                     try:
                                         parsed_numeric = Decimal(value_str)
                                     except InvalidOperation:
                                         if value_str.lower() == 'none' or value_str == '.':
                                             logger.info(f"List value is '{value_str}', storing NULL numeric. Date: {parsed_date}, Feed: {feed_id}")
                                         else:
                                             logger.warning(f"Cannot parse list value '{value_str}' as Decimal. Storing NULL numeric. Date: {parsed_date}, Feed: {feed_id}")
                                     
                                     points_to_insert.append({
                                         'feed_id': feed_id, 'data_date': parsed_date,
                                         'value_numeric': parsed_numeric, 'value_text': parsed_text
                                     })
                                 except ValueError as e:
                                     logger.warning(f"Skipping list point: date parse error {e} - {point} for feed {feed_id}")
                        else:
                             logger.warning(f"API data is not a DataFrame or list for feed {feed_id}. Type: {type(api_data)}. Cannot process points.")

                        # 3. Bulk insert points
                        if points_to_insert:
                            points_sql = text("""
                                INSERT INTO av_economic_data_points (feed_id, data_date, value_numeric, value_text)
                                VALUES (:feed_id, :data_date, :value_numeric, :value_text)
                                ON CONFLICT (feed_id, data_date) DO NOTHING;
                            """)
                            db_session.execute(points_sql, points_to_insert) # Execute bulk insert
                            inserted_count = len(points_to_insert) # Best estimate of successful inserts
                            logger.info(f"Attempted bulk insert for {inserted_count} points for feed_id {feed_id}.")
                        else:
                            logger.info(f"No valid points to insert for feed_id {feed_id}.")
                            inserted_count = 0
                        
                        # 4. Update data_feeds status
                        update_sql = text("""
                            UPDATE data_feeds
                            SET status = 'completed', row_count = :count
                            WHERE feed_id = :fid;
                        """)
                        db_session.execute(update_sql, {'count': inserted_count, 'fid': feed_id})
                        db_session.commit() # Commit transaction
                        db_status = f"Completed - Feed ID: {feed_id}, Points Attempted: {inserted_count}"
                        logger.info(f"Successfully committed DB changes for feed_id {feed_id}")

                    except SQLAlchemyError as e:
                        db_op_exception = e
                        db_session.rollback() # Rollback on error
                        logger.error(f"Database transaction failed for feed_id {feed_id_created or 'unknown'}: {e}", exc_info=True)
                        db_status = "Error"
                        db_error = str(e)
                        # Attempt to update status to 'error' if feed_id was created
                        if feed_id_created:
                            try:
                                db_err = SessionLocal()
                                error_update_sql = text("UPDATE data_feeds SET status = 'error', error_message = :msg WHERE feed_id = :fid;")
                                error_msg_truncated = (db_error[:497] + '...') if len(db_error) > 500 else db_error
                                db_err.execute(error_update_sql, {'msg': error_msg_truncated, 'fid': feed_id_created})
                                db_err.commit()
                                db_err.close()
                                logger.info(f"Updated feed_id {feed_id_created} status to 'error'.")
                            except Exception as db_e:
                                logger.error(f"CRITICAL: Failed to update feed_id {feed_id_created} status to 'error' after initial failure: {db_e}")
                    finally:
                        db_session.close() # Ensure session is closed

                else:
                    db_status = "Skipped - No API data"
                    logger.warning(f"DB write skipped for '{indicator_key}': API data/metadata missing.")
            else:
                 db_status = "Write Disabled"

            # --- Prepare final return dictionary --- 
            final_metadata = api_meta_data if api_meta_data else {}
            if write_db_flag: # Add DB status only if write was attempted
                final_metadata['database_status'] = db_status
                if db_error: 
                    final_metadata['database_error'] = db_error
            
            return {"data": api_data, "metadata": final_metadata}

        except ValueError as ve:
            logger.error(f"Value error in get_econ_data ({indicator_key}): {ve}", exc_info=True)
            return {"data": None, "metadata": {"error": str(ve), "database_status": db_status}}
        except Exception as e:
            logger.error(f"General error fetching/processing data for {indicator_key}: {e}", exc_info=True)
            error_meta = {"error": str(e)}
            if write_db_flag:
                 error_meta["database_status"] = db_status if db_status != "Write Disabled" else "Error during fetch/process"
                 if db_error:
                      error_meta["database_error"] = db_error
            return {"data": None, "metadata": error_meta}

    # --- Run the synchronous function in a thread pool --- 
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,  # Use default executor
        _get_and_convert,
        indicator, write_to_db, interval, maturity
    )

    if not isinstance(result, dict) or 'data' not in result or 'metadata' not in result:
         logger.error(f"Unexpected result structure from _get_and_convert: {result}")
         return {"data": None, "metadata": {"error": "Internal tool error: Unexpected data structure."}}
    
    return result