import os
import asyncio
from typing import Dict, Any
from dotenv import load_dotenv
from langchain_core.tools import Tool
from pydantic.v1 import BaseModel, Field # Use pydantic v1 for Langchain compatibility
from e2b_code_interpreter import AsyncSandbox
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class E2BCodeInterpreterInput(BaseModel):
    """Input schema for the E2B Code Interpreter Tool."""
    code: str = Field(description="The python code to execute in the sandbox.")

class E2BCodeInterpreterTool:
    """ 
    A tool that executes Python code in a persistent E2B sandbox environment.
    Manages a single AsyncSandbox instance to improve performance by avoiding 
    repeated sandbox startup times.
    Requires the E2B_API_KEY environment variable to be set.
    """
    tool_name: str = "python_code_interpreter"
    description: str = ( 
        "Execute python code in a sandboxed Jupyter notebook environment. "
        "Input must be a JSON object with a single key 'code' containing the Python code string. "
        "Returns a dictionary containing 'stdout', 'stderr', 'error' (if any), and 'results' (for rich outputs like plots). "
        "Make sure to print any results you want to capture in stdout."
    )

    def __init__(self):
        if not os.getenv("E2B_API_KEY"):
            raise ValueError("E2B_API_KEY environment variable not set.")
        
        # Initialize the sandbox - this will be reused across calls
        # Note: In a long-running server, ensure this gets closed gracefully if possible,
        # although E2B handles timeouts automatically.
        try:
            # Running __init__ in a sync context, but need to start the async sandbox.
            # Use asyncio.run() if no event loop is running, or get_event_loop().run_until_complete() otherwise.
            # For simplicity in tool definition, we might defer the actual sandbox creation
            # to the first call, or assume an event loop exists in the LangGraph context.
            # Let's initialize it as None and create on first call.
            self._sandbox: AsyncSandbox | None = None
            self._sandbox_lock = asyncio.Lock()
            logger.info("E2BCodeInterpreterTool initialized, sandbox will be created on first use.")
        except Exception as e:
            logger.error(f"Failed to initialize E2B Sandbox: {e}", exc_info=True)
            raise

    async def _get_sandbox(self) -> AsyncSandbox:
        """Get or create the sandbox instance asynchronously."""
        if self._sandbox is None:
            async with self._sandbox_lock:
                # Double-check after acquiring lock
                if self._sandbox is None:
                    logger.info("Creating E2B AsyncSandbox instance...")
                    self._sandbox = AsyncSandbox()
                    await self._sandbox.open() # Explicitly open the sandbox
                    logger.info("E2B AsyncSandbox instance created and opened.")
        return self._sandbox

    async def _run_code(self, code: str) -> Dict[str, Any]:
        """Executes the given Python code in the sandbox."""
        logger.info(f"Executing code in E2B sandbox:\n{code[:500]}...\n")
        sandbox = await self._get_sandbox()
        try:
            execution = await sandbox.run_python(code=code)
            
            output = {
                "stdout": "\n".join(log.line for log in execution.logs.stdout),
                "stderr": "\n".join(log.line for log in execution.logs.stderr),
                "results": execution.results, # For potential rich outputs
                "error": execution.error.to_dict() if execution.error else None,
            }
            logger.info(f"Code execution finished. Stdout: {len(output['stdout'])} chars, Stderr: {len(output['stderr'])} chars.")
            if execution.error:
                logger.warning(f"Execution resulted in error: {execution.error.name}: {execution.error.value}")
            return output
        except Exception as e:
            logger.error(f"Error during E2B code execution: {e}", exc_info=True)
            return {"stdout": "", "stderr": "", "results": [], "error": {"name": "ToolExecutionError", "value": str(e), "traceback": ""}}

    def get_langchain_tool(self) -> Tool:
        """Creates and returns a Langchain Tool instance."""
        tool = Tool(
            name=self.tool_name,
            description=self.description,
            func=self._run_code, # Use the async run method
            coroutine=self._run_code, # Explicitly set coroutine
            args_schema=E2BCodeInterpreterInput
        )
        return tool

    async def close(self):
        """Closes the sandbox if it was created."""
        if self._sandbox:
            logger.info("Closing E2B AsyncSandbox...")
            await self._sandbox.close()
            self._sandbox = None
            logger.info("E2B AsyncSandbox closed.")

# You would typically instantiate this class once and pass its tool
# e.g.: 
# e2b_interpreter = E2BCodeInterpreterTool()
# code_tool = e2b_interpreter.get_langchain_tool()
