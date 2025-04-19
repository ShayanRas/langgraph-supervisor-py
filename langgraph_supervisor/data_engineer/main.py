from langgraph_supervisor.data_engineer.agent_tools.alpha_vantage.get_econ_data import get_econ_data
from langgraph_supervisor.data_engineer.agent_tools.twelve_data.get_time_series import get_time_series
from langgraph_supervisor.data_engineer.agent_tools.EtoB.code_interpreter import execute_python
from langgraph_prebuilt import create_react_agent
from langgraph_supervisor.data_engineer.prompt import prompt
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
import os
import dotenv

dotenv.load_dotenv()

llm = ChatOpenAI(model="gpt-4.1", api_key=os.getenv("OPENAI_API_KEY"), temperature=0.1)

data_engineer = create_react_agent(
    model=llm,
    tools=[get_econ_data, get_time_series, execute_python],
    name="data_engineer",
    prompt=prompt
)


