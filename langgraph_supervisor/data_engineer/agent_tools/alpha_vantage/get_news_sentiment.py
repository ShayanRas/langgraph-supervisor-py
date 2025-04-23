import os
import dotenv
from typing import Dict, Any, Optional, List, Union
import asyncio
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import InjectedToolArg, tool
from typing_extensions import Annotated
import logging
from alpha_vantage.alphaintelligence import AlphaIntelligence
from datetime import datetime
from decimal import Decimal, InvalidOperation
import pandas as pd

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

@tool
async def get_news_sentiment(
    tickers: Optional[List[str]] = None,
    topics: Optional[List[str]] = None,
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    sort: Optional[str] = 'LATEST',
    max_articles: Optional[int] = 50, # Renamed 'limit' for clarity, default 50
    *,
    config: Annotated[RunnableConfig, InjectedToolArg]
) -> Dict[str, Any]:
    """Get news and sentiment data for specific tickers or topics from Alpha Vantage.

    Retrieves live and historical market news and sentiment data from global news outlets,
    sourced from Alpha Vantage's Alpha Intelligence API (NEWS_SENTIMENT function).

    Args:
        tickers: Optional list of stock/crypto/forex symbols. Filters for articles mentioning 
                 ALL provided tickers. Example: ['IBM'] or ['COIN', 'CRYPTO:BTC', 'FOREX:USD'] for articles that simultaneously mention all of them.
        topics: Optional list of news topics. Filters for articles covering ALL provided topics.
                Supported topics:
                'blockchain', 'earnings', 'ipo', 'mergers_and_acquisitions',
                'financial_markets', 'economy_fiscal', 'economy_monetary', 'economy_macro',
                'energy_transportation', 'finance', 'life_sciences', 'manufacturing',
                'real_estate', 'retail_wholesale', 'technology'.
                Example: ['technology', 'ipo'].
        time_from: Optional start time filter in YYYYMMDDTHHMM format (e.g., '20220410T0130').
                 If specified without time_to, returns articles from time_from to the current time.
        time_to: Optional end time filter in YYYYMMDDTHHMM format.
        sort: Sort order for articles. Options: 'LATEST', 'EARLIEST', 'RELEVANCE'. 
              Defaults to 'LATEST'.
        max_articles: Maximum number of articles to return. Options: 50, 1000. 
                      Defaults to 50 (maps to API 'limit' parameter).
        config: Runtime configuration (automatically injected).

    Returns:
        A dictionary containing the news feed data (under 'data') and metadata (under 'metadata').
    """
    def _get_and_convert_news(
        tickers_param: Optional[List[str]],
        topics_param: Optional[List[str]],
        time_from_param: Optional[str],
        time_to_param: Optional[str],
        sort_param: Optional[str],
        limit_param: Optional[int]
    ) -> Dict[str, Any]:
        api_data: Optional[list] = None
        api_meta_data: Optional[dict] = None

        try:
            api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            if not api_key:
                raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables.")

            ai = AlphaIntelligence(key=api_key, output_format='json')
            logger.info(f"Fetching news sentiment (Tickers: {tickers_param}, Topics: {topics_param}, Limit: {limit_param})")

            tickers_str = ','.join(tickers_param) if tickers_param else None
            topics_str = ','.join(topics_param) if topics_param else None

            api_data = ai.get_news_sentiment(
                tickers=tickers_str,
                topics=topics_str,
                time_from=time_from_param,
                time_to=time_to_param,
                sort=sort_param,
                limit=limit_param
            )

            logger.info(f"Successfully fetched news sentiment data.")

            final_metadata = {}

            return {"data": api_data, "metadata": final_metadata}

        except ValueError as ve:
            logger.error(f"Value error fetching news sentiment: {ve}", exc_info=True)
            return {"data": None, "metadata": {"error": str(ve)}}
        except Exception as e:
            logger.error(f"General error fetching news sentiment: {e}", exc_info=True)
            error_meta = {"error": str(e)}
            return {"data": None, "metadata": error_meta}

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        _get_and_convert_news,
        tickers, topics, time_from, time_to, sort, max_articles
    )

    if not isinstance(result, dict) or 'data' not in result or 'metadata' not in result:
         logger.error(f"Unexpected result structure from _get_and_convert_news: {result}")
         return {"data": None, "metadata": {"error": "Internal tool error: Unexpected data structure."}}

    return result