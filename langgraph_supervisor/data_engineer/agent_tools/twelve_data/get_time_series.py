from twelvedata import TDClient
import os
import dotenv
from typing import List, Dict, Any, Optional
from typing_extensions import Annotated
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import InjectedToolArg
from langchain.tools import tool
from datetime import datetime
from decimal import Decimal, InvalidOperation
import asyncio
import logging
from pydantic import BaseModel, Field

# Database imports - adjust path if necessary
from langgraph_supervisor.database_tools.connection import SessionLocal
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Set up logging
logger = logging.getLogger(__name__)

dotenv.load_dotenv()

class TimeSeriesInput(BaseModel):
    """Input schema for the get_time_series tool."""
    symbol: str = Field(..., description="The symbol of the financial instrument (e.g., 'AAPL', 'MSFT', 'EUR/USD').")
    interval: str = Field(..., description="The time interval between data points (e.g., '1min', '5min', '1h', '1day').")
    output_size: int = Field(..., description="The number of data points to retrieve.")
    write_to_db: bool = Field(False, description="Set to True to write the fetched data points to the 'td_time_series_data' table.")

@tool(args_schema=TimeSeriesInput)
async def get_time_series(
    symbol: str,
    interval: str,
    output_size: int,
    write_to_db: bool = False,
    *,
    config: Annotated[RunnableConfig, InjectedToolArg]
) -> Dict[str, Any]: 
    """Get historical time series data for a financial symbol from TwelveData.

    Use this tool to retrieve historical price data (like open, high, low, close, volume)
    for a specific stock symbol (e.g., 'AAPL', 'GOOG'), currency pair (e.g., 'EUR/USD'),
    or other financial instrument supported by TwelveData.

    Args:
        symbol: The symbol of the financial instrument.
        interval: The time interval between data points.
        output_size: The number of data points to retrieve.
        write_to_db: If True, attempts to write the fetched data to the 'td_time_series_data' database table.
        config: Runtime configuration (automatically injected).

    Returns:
        A dictionary containing:
            - 'api_data' (Dict): The raw JSON response from the TwelveData API, including 'meta' and 'values'.
            - 'database_status' (str): A message indicating the outcome of the database write attempt (e.g., 'Write successful', 'Write failed', 'Write skipped').
    """
    # Define a synchronous helper function to run in the thread
    def _fetch_data():
        try:
            # Instantiate client *inside* the thread function
            td = TDClient(apikey=os.getenv("TWELVE_DATA_API_KEY"))
            logger.info(f"Fetching TwelveData time series for {symbol}, interval {interval}, size {output_size}")
            timeseries = td.time_series(
                symbol=symbol, interval=interval, outputsize=output_size
            )
            # Return the raw dictionary structure
            return timeseries.as_json()
        except Exception as e:
            logger.error(f"Error fetching data from TwelveData API: {e}", exc_info=True)
            return {"status": "error", "message": f"TwelveData API Error: {e}", "values": [], "meta": None}

    # --- Fetch Data --- 
    api_result = await asyncio.to_thread(_fetch_data)
    db_status = "Write skipped: write_to_db was False."

    # --- Write to DB (if requested and API call was successful) ---
    if write_to_db and api_result.get("status") == "ok":
        db = None
        try:
            meta_data = api_result.get("meta")
            values_data = api_result.get("values")

            if not values_data or not meta_data:
                db_status = "Write skipped: No values or metadata returned from API."
                logger.warning(f"Skipping DB write for {symbol}/{interval}: Missing values or metadata.")
            else:
                logger.info(f"Attempting DB write for {len(values_data)} points for {symbol}/{interval}...")
                db = SessionLocal()
                points_to_insert = []
                parse_errors = 0

                for point in values_data:
                    try:
                        # Ensure required fields exist
                        dt_str = point.get("datetime")
                        open_val = point.get("open")
                        high_val = point.get("high")
                        low_val = point.get("low")
                        close_val = point.get("close")
                        volume_val = point.get("volume")

                        if dt_str is None:
                             logger.warning(f"Skipping point for {symbol}/{interval}: Missing datetime.")
                             parse_errors += 1
                             continue

                        # Parse datetime (assuming format like 'YYYY-MM-DD HH:MM:SS')
                        parsed_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

                        # Prepare data for insertion, handling potential None or conversion errors
                        points_to_insert.append({
                            "symbol": meta_data.get("symbol"),
                            "interval": meta_data.get("interval"),
                            "datetime": parsed_dt,
                            "open": Decimal(open_val) if open_val is not None else None,
                            "high": Decimal(high_val) if high_val is not None else None,
                            "low": Decimal(low_val) if low_val is not None else None,
                            "close": Decimal(close_val) if close_val is not None else None,
                            "volume": int(volume_val) if volume_val is not None else None,
                            "currency": meta_data.get("currency"),
                            "exchange_timezone": meta_data.get("exchange_timezone"),
                            "exchange": meta_data.get("exchange"),
                            "mic_code": meta_data.get("mic_code"),
                            "type": meta_data.get("type"),
                            # fetch_timestamp is handled by DB default
                        })
                    except (ValueError, TypeError, InvalidOperation) as e:
                         logger.warning(f"Skipping point for {symbol}/{interval} due to parsing error: {e} (Data: {point})", exc_info=False) # Avoid noisy stacktrace for common errors
                         parse_errors += 1
                    except Exception as e:
                        logger.error(f"Unexpected error processing point for {symbol}/{interval}: {e} (Data: {point})", exc_info=True)
                        parse_errors += 1
                
                if points_to_insert:
                    # Use SQLAlchemy Core execute with text for ON CONFLICT
                    # Define the base insert statement
                    insert_sql = text("""
                    INSERT INTO td_time_series_data (
                        symbol, interval, datetime, open, high, low, close, volume, 
                        currency, exchange_timezone, exchange, mic_code, type
                    ) VALUES (
                        :symbol, :interval, :datetime, :open, :high, :low, :close, :volume, 
                        :currency, :exchange_timezone, :exchange, :mic_code, :type
                    )
                    ON CONFLICT (symbol, interval, datetime) DO NOTHING
                    """)
                    
                    try:
                        # ExecuteMany with the list of dictionaries
                        result = db.execute(insert_sql, points_to_insert)
                        db.commit()
                        inserted_count = result.rowcount # This might not be accurate for ON CONFLICT DO NOTHING depending on DB/driver
                        logger.info(f"DB write attempt completed for {symbol}/{interval}. Parsed: {len(points_to_insert)}, Parse Errors: {parse_errors}. DB rowcount: {inserted_count}")
                        db_status = f"Write successful (Attempted: {len(values_data)}, Parsed: {len(points_to_insert)}, Parse Errors: {parse_errors}, DB rowcount: {inserted_count})"
                    except SQLAlchemyError as e:
                        db.rollback()
                        logger.error(f"Database error during bulk insert for {symbol}/{interval}: {e}", exc_info=True)
                        db_status = f"Write failed: Database error - {e}"
                    except Exception as e:
                        db.rollback()
                        logger.error(f"Unexpected error during bulk insert for {symbol}/{interval}: {e}", exc_info=True)
                        db_status = f"Write failed: Unexpected error - {e}"
                else:
                    db_status = f"Write skipped: No valid points found after parsing (Attempted: {len(values_data)}, Parse Errors: {parse_errors})."
                    logger.warning(f"No valid points to insert for {symbol}/{interval} after parsing.")

        except Exception as e:
            logger.error(f"Unexpected error during DB write preparation for {symbol}/{interval}: {e}", exc_info=True)
            db_status = f"Write failed: Error during preparation - {e}"
            if db: db.rollback()
        finally:
            if db: db.close()
    elif write_to_db and api_result.get("status") != "ok":
        db_status = f"Write skipped: API call failed with status '{api_result.get('status')}' and message '{api_result.get('message', 'N/A')}'"
        logger.warning(db_status)

    # Return both API result and DB status
    return {
        "api_data": api_result,
        "database_status": db_status
    }
