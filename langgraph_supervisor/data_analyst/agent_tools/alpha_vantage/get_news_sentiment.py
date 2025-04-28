import os
import dotenv
from typing import Dict, Any, Optional, List
import asyncio
import requests
from requests.exceptions import RequestException
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import InjectedToolArg, tool
from typing_extensions import Annotated
import logging

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

BASE_URL = "https://www.alphavantage.co/query"

@tool
async def get_news_sentiment(
    tickers: Optional[List[str]] = None,
    topics: Optional[List[str]] = None,
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    sort: Optional[str] = 'LATEST',
    max_articles: Optional[int] = 50,
    *,
    config: Annotated[RunnableConfig, InjectedToolArg]
) -> Dict[str, Any]:
    """Get news and sentiment data for specific tickers or topics from Alpha Vantage.

    Retrieves live and historical market news and sentiment data from global news outlets,
    using direct calls to the Alpha Vantage API (NEWS_SENTIMENT function).

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
        max_articles: Maximum number of articles to return. Allowed: up to 1000. 
                      Defaults to 50 (maps to API 'limit' parameter).
        config: Runtime configuration (automatically injected).

    Returns:
        A dictionary containing the news feed data (under 'data') and metadata (under 'metadata').
        The 'data' key holds the JSON response from the API, usually containing a 'feed' list.
    """
    def _fetch_news_via_api(
        tickers_param: Optional[List[str]],
        topics_param: Optional[List[str]],
        time_from_param: Optional[str],
        time_to_param: Optional[str],
        sort_param: Optional[str],
        limit_param: Optional[int]
    ) -> Dict[str, Any]:
        api_data: Optional[Dict[str, Any]] = None
        api_meta_data: Dict[str, Any] = {}

        try:
            api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            if not api_key:
                raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables.")

            params = {
                "function": "NEWS_SENTIMENT",
                "apikey": api_key,
            }
            if tickers_param:
                params["tickers"] = ','.join(tickers_param)
            if topics_param:
                valid_topics = {
                    'blockchain', 'earnings', 'ipo', 'mergers_and_acquisitions',
                    'financial_markets', 'economy_fiscal', 'economy_monetary', 'economy_macro',
                    'energy_transportation', 'finance', 'life_sciences', 'manufacturing',
                    'real_estate', 'retail_wholesale', 'technology'
                }
                if not all(topic in valid_topics for topic in topics_param):
                    raise ValueError(f"Invalid topic provided. Allowed: {', '.join(valid_topics)}")
                params["topics"] = ','.join(topics_param)
            if time_from_param:
                params["time_from"] = time_from_param
            if time_to_param:
                params["time_to"] = time_to_param
            if sort_param:
                if sort_param not in ['LATEST', 'EARLIEST', 'RELEVANCE']:
                     raise ValueError("Invalid sort parameter. Use 'LATEST', 'EARLIEST', or 'RELEVANCE'.")
                params["sort"] = sort_param
            if limit_param:
                if not isinstance(limit_param, int) or not (1 <= limit_param <= 1000):
                    raise ValueError("Invalid max_articles (limit). Must be an integer between 1 and 1000.")
                params["limit"] = limit_param

            logger.info(f"Fetching news sentiment via API. Params: { {k:v for k,v in params.items() if k != 'apikey'} }")

            # --- Make the API request --- 
            # Add detailed logging right before the request
            logger.debug(f"Making Alpha Vantage request to {BASE_URL} with params: {params}") 
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()

            api_data = response.json()

            if isinstance(api_data, dict) and ("Error Message" in api_data or "Information" in api_data):
                error_message = api_data.get("Error Message", api_data.get("Information", "Unknown API error"))
                logger.error(f"Alpha Vantage API error: {error_message}")
                api_meta_data["error"] = f"API Error: {error_message}"
                api_data = None
            else:
                 logger.info(f"Successfully fetched and parsed news sentiment data.")
                 pass 

            return {"data": api_data, "metadata": api_meta_data}

        except RequestException as re:
            logger.error(f"Network error fetching news sentiment: {re}", exc_info=True)
            return {"data": None, "metadata": {"error": f"Network Error: {re}"}}        
        except ValueError as ve:
            logger.error(f"Value error preparing/processing news sentiment request: {ve}", exc_info=True)
            return {"data": None, "metadata": {"error": str(ve)}}
        except Exception as e:
            logger.error(f"General error fetching/processing news sentiment: {e}", exc_info=True)
            return {"data": None, "metadata": {"error": f"General Error: {e}"}}

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        _fetch_news_via_api,
        tickers, topics, time_from, time_to, sort, max_articles
    )

    if not isinstance(result, dict) or 'data' not in result or 'metadata' not in result:
         logger.error(f"Unexpected result structure from _fetch_news_via_api: {result}")
         return {"data": None, "metadata": {"error": "Internal tool error: Unexpected data structure."}}

    return result