import os
import dotenv
from typing import Dict, Any, Optional, List, Union
import asyncio
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import InjectedToolArg, tool
from typing_extensions import Annotated
import logging
from alpha_vantage.econindicators import EconIndicators
from datetime import datetime
from decimal import Decimal, InvalidOperation
import pandas as pd

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

@tool
async def get_econ_data(
    indicator: str,
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
        interval: The time interval for data points. Options: 'real_gdp': 'annual'/'quarterly'; 'treasury_yield': 'daily'/'weekly'/'monthly'; 'federal_funds_rate': 'daily'/'weekly'/'monthly'; 'cpi': 'semiannual'/'monthly'.
        maturity: For treasury_yield only - the maturity period of the treasury bond.
        config: Runtime configuration (automatically injected).

    Returns:
        A dictionary containing the economic indicator data and metadata.
    """
    # Define a synchronous helper function to run in the thread
    def _get_and_convert(
        indicator_key: str,
        interval_param: Optional[str],
        maturity_param: Optional[str],
    ) -> Dict[str, Any]:
        api_data: Optional[Union[list, pd.DataFrame]] = None # Allow DataFrame type hint
        api_meta_data: Optional[dict] = None

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

            # --- Prepare final return dictionary --- 
            final_metadata = api_meta_data if api_meta_data else {}
            
            # Ensure data is returned, potentially converting DataFrame to list of dicts if needed for consistency
            # For now, returning the raw api_data (could be list or DataFrame)
            return {"data": api_data, "metadata": final_metadata}

        except ValueError as ve:
            logger.error(f"Value error in get_econ_data ({indicator_key}): {ve}", exc_info=True)
            return {"data": None, "metadata": {"error": str(ve)}}
        except Exception as e:
            logger.error(f"General error fetching/processing data for {indicator_key}: {e}", exc_info=True)
            error_meta = {"error": str(e)}
            return {"data": None, "metadata": error_meta}

    # --- Run the synchronous function in a thread pool --- 
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,  # Use default executor
        _get_and_convert,
        indicator, interval, maturity
    )

    if not isinstance(result, dict) or 'data' not in result or 'metadata' not in result:
         logger.error(f"Unexpected result structure from _get_and_convert: {result}")
         return {"data": None, "metadata": {"error": "Internal tool error: Unexpected data structure."}}
    
    return result