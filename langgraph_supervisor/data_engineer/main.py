from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_econ_data import get_econ_data
from langgraph_supervisor.data_engineer.agent_tools.twelve_data.get_time_series import get_time_series
from langgraph_supervisor.data_engineer.agent_tools.EtoB.code_interpreter import E2BCodeInterpreterTool
from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_commodities_timeseries import get_commodity_data
from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_news_sentiment import get_news_sentiment
from langgraph.prebuilt import create_react_agent
from langgraph_supervisor.data_engineer.prompt import prompt
from langgraph_supervisor.database_tools.db_tools import execute_sql
from langchain_openai import ChatOpenAI
import os
import dotenv

dotenv.load_dotenv()

llm = ChatOpenAI(model="gpt-4.1", api_key=os.getenv("OPENAI_API_KEY"), temperature=0.1)

e2b_interpreter = E2BCodeInterpreterTool()
code_interpreter_tool = e2b_interpreter.get_langchain_tool()

data_engineer = create_react_agent(
    model=llm,
    tools=[get_econ_data, 
    get_time_series, 
    code_interpreter_tool,
    execute_sql, 
    get_commodity_data, 
    get_news_sentiment],
    name="data_engineer",
    prompt=prompt
)

# TODO: Consider adding cleanup logic for the E2B sandbox 
# (e.g., using atexit or signal handling if running as a persistent server)
# Example (may need adjustment based on how the app runs):
# import atexit
# import asyncio
# def cleanup_e2b():
#     try:
#         loop = asyncio.get_event_loop()
#         if loop.is_running():
#             loop.create_task(e2b_interpreter.close())
#         else:
#             asyncio.run(e2b_interpreter.close())
#     except Exception as e:
#         print(f"Error closing E2B sandbox during cleanup: {e}")
# 
# atexit.register(cleanup_e2b)
