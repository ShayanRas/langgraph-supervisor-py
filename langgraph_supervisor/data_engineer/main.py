from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_econ_data import get_econ_data
from langgraph_supervisor.data_engineer.agent_tools.twelve_data.get_time_series import get_time_series
from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_commodities_timeseries import get_commodity_data
from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_news_sentiment import get_news_sentiment
from langgraph.prebuilt import create_react_agent
from langgraph_supervisor.data_engineer.prompt import prompt
from langgraph_supervisor.database_tools.db_tools import execute_sql
from langchain_openai import ChatOpenAI
from langgraph_supervisor.data_engineer.agent_tools.E2B.main import E2BCodeInterpreterTool
import os
import dotenv

dotenv.load_dotenv()

llm = ChatOpenAI(model="gpt-4.1", api_key=os.getenv("OPENAI_API_KEY"), temperature=0.1)

code_interpreter = E2BCodeInterpreterTool()

data_engineer = create_react_agent(
    model=llm,
    tools=[get_econ_data, 
    get_time_series, 
    execute_sql, 
    get_commodity_data, 
    get_news_sentiment,
    code_interpreter],
    name="data_engineer",
    prompt=prompt
)
