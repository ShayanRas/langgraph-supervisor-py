import os
import dotenv
from typing import Dict, Any, Optional, List, Union
import asyncio
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import InjectedToolArg, tool
from typing_extensions import Annotated
import logging
from alpha_vantage.commodities import Commodities
from datetime import datetime
from decimal import Decimal, InvalidOperation
import pandas as pd

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

@tool
async def get_commodity_data(
    commodity: str,
    interval: Optional[str] = 'monthly', # Default to monthly as per README
    max_periods: Optional[int] = None, # Add max_periods parameter
    *,
    config: Annotated[RunnableConfig, InjectedToolArg]
) -> Dict[str, Any]:
    """Get commodity price data from Alpha Vantage.

    Use this tool to retrieve time series data for various commodities like Crude Oil (WTI, Brent),
    Natural Gas, Copper, Aluminum, Wheat, Corn, Cotton, Sugar, Coffee, and the Global All Commodities Price Index.
    The data is sourced from Alpha Vantage's Commodities API.

    Args:
        commodity: The commodity to retrieve. Options include 'wti', 'brent', 'natural_gas', 'copper', 'aluminum', 'wheat', 'corn', 'cotton', 'sugar', 'coffee', 'all_commodities_index'.
        interval: The time interval for data points. Options: 'monthly', 'quarterly', 'annual'. Defaults to 'monthly'.
        max_periods: Optional. The maximum number of recent data points (periods) to return. Useful for limiting context size.
        config: Runtime configuration (automatically injected).

    Returns:
        A dictionary containing the potentially truncated commodity data and metadata.
    """
    # Define a synchronous helper function to run in the thread
    def _get_and_convert_commodity(
        commodity_key: str,
        interval_param: str, # Interval is required by the library methods
        max_periods_param: Optional[int] # Pass max_periods
    ) -> Dict[str, Any]:
        api_data: Optional[Union[list, pd.DataFrame]] = None
        api_meta_data: Optional[dict] = None
        allowed_intervals = ['monthly', 'quarterly', 'annual']

        if interval_param not in allowed_intervals:
             return {"data": None, "metadata": {"error": f"Invalid interval '{interval_param}'. Allowed intervals: {allowed_intervals}"}}

        try:
            api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            if not api_key:
                raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables.")

            comm = Commodities(key=api_key, output_format='json') 
            logger.info(f"Fetching '{commodity_key}' (Interval: {interval_param})")

            # --- Map commodity to appropriate method ---
            if commodity_key == 'wti':
                api_data, api_meta_data = comm.get_wti(interval=interval_param)
            elif commodity_key == 'brent':
                api_data, api_meta_data = comm.get_brent(interval=interval_param)
            elif commodity_key == 'natural_gas':
                 api_data, api_meta_data = comm.get_natural_gas(interval=interval_param)
            elif commodity_key == 'copper':
                 api_data, api_meta_data = comm.get_copper(interval=interval_param)
            elif commodity_key == 'aluminum':
                 api_data, api_meta_data = comm.get_aluminum(interval=interval_param)
            elif commodity_key == 'wheat':
                 api_data, api_meta_data = comm.get_wheat(interval=interval_param)
            elif commodity_key == 'corn':
                 api_data, api_meta_data = comm.get_corn(interval=interval_param)
            elif commodity_key == 'cotton':
                 api_data, api_meta_data = comm.get_cotton(interval=interval_param)
            elif commodity_key == 'sugar':
                 api_data, api_meta_data = comm.get_sugar(interval=interval_param)
            elif commodity_key == 'coffee':
                 api_data, api_meta_data = comm.get_coffee(interval=interval_param)
            elif commodity_key == 'all_commodities_index':
                 api_data, api_meta_data = comm.get_price_index(interval=interval_param)
            else:
                raise ValueError(f"Unknown commodity: {commodity_key}")

            logger.info(f"Successfully fetched data for '{commodity_key}'.")

            # --- Prepare final return dictionary --- 
            final_metadata = api_meta_data if api_meta_data else {}

            # --- Apply max_periods limit if specified --- 
            if max_periods_param is not None and max_periods_param > 0 and isinstance(api_data, list):
                logger.info(f"Limiting results for '{commodity_key}' to {max_periods_param} periods.")
                api_data = api_data[:max_periods_param]
            elif max_periods_param is not None and max_periods_param > 0 and isinstance(api_data, pd.DataFrame):
                 logger.info(f"Limiting results for '{commodity_key}' to {max_periods_param} periods.")
                 api_data = api_data.head(max_periods_param) # Assuming DataFrame is newest first

            return {"data": api_data, "metadata": final_metadata}

        except ValueError as ve:
            logger.error(f"Value error in get_commodity_data ({commodity_key}): {ve}", exc_info=True)
            return {"data": None, "metadata": {"error": str(ve)}}
        except Exception as e:
            logger.error(f"General error fetching/processing data for {commodity_key}: {e}", exc_info=True)
            error_meta = {"error": str(e)}
            return {"data": None, "metadata": error_meta}

    # --- Run the synchronous function in a thread pool ---
    loop = asyncio.get_running_loop()
    # Ensure interval has a valid default if None was passed initially (shouldn't happen with default set)
    effective_interval = interval if interval else 'monthly'
    result = await loop.run_in_executor(
        None,  # Use default executor
        _get_and_convert_commodity,
        commodity, effective_interval, max_periods # Pass max_periods
    )

    if not isinstance(result, dict) or 'data' not in result or 'metadata' not in result:
         logger.error(f"Unexpected result structure from _get_and_convert_commodity: {result}")
         return {"data": None, "metadata": {"error": "Internal tool error: Unexpected data structure."}}

    return result
