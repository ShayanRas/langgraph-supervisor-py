from langchain.tools import tool
from e2b_code_interpreter import AsyncSandbox
from dotenv import load_dotenv

load_dotenv()

@tool
async def execute_python(code: str) -> str:
    """
    Execute python code in a sandboxed environment provided by E2B. 
    Returns a string containing the stdout and stderr from the execution.
    Make sure to print any results you want to see.
    """
    async with AsyncSandbox() as sandbox:
        execution = await sandbox.run_code(code)
        
        result_str = f"Stdout:\n{execution.stdout}\n\nStderr:\n{execution.stderr}"
        
        if execution.artifacts:
            result_str += "\n\nGenerated Artifacts:"
            for artifact in execution.artifacts:
                result_str += f"\n- {artifact.name}"
                # To save the artifact if needed:
                # await artifact.save("path/to/save/" + artifact.name)
        
        return result_str
