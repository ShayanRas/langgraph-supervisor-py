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

# Set up logging
logger = logging.getLogger(__name__)

dotenv.load_dotenv()

class TimeSeriesInput(BaseModel):
    """Input schema for the get_time_series tool."""
    symbol: str = Field(..., description="The symbol of the financial instrument (e.g., 'AAPL', 'MSFT', 'EUR/USD').")
    interval: str = Field(..., description="The time interval between data points (e.g., '1min', '5min', '1h', '1day').")
    output_size: int = Field(..., description="The number of data points to retrieve.")

@tool(args_schema=TimeSeriesInput)
async def get_time_series(
    symbol: str,
    interval: str,
    output_size: int,
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
        config: Runtime configuration (automatically injected).

    Returns:
        A dictionary containing:
            - 'api_data' (Dict): The raw JSON response from the TwelveData API, including 'meta' and 'values'.
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
    return {"api_data": api_result}
