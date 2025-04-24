from dotenv import load_dotenv
load_dotenv()
from e2b_code_interpreter import Sandbox, Result

import os
import uuid
from dotenv import load_dotenv
from pydantic.v1 import BaseModel, Field
from langchain.tools import BaseTool
from typing import Type, Optional, Dict, Any, List

load_dotenv()


class FileWriteInput(BaseModel):
    """Input for writing a file to the sandbox."""
    path: str = Field(description="The absolute path inside the sandbox where the file should be written.")
    content: str = Field(description="The string content to write to the file.")


class E2BCodeInterpreterInput(BaseModel):
    """Input for the E2B Code Interpreter tool, supporting code execution and file operations."""
    code: Optional[str] = Field(default=None, description="The Python code to execute in the sandbox session.")
    write_files: Optional[List[FileWriteInput]] = Field(default=None, description="List of files to write to the sandbox before code execution.")
    read_files: Optional[List[str]] = Field(default=None, description="List of absolute file paths to read from the sandbox after code execution.")
    list_path: str = Field(default='/home/user', description="The directory path inside the sandbox to list files from after all operations.")


class E2BCodeInterpreterTool(BaseTool):
    """Tool to execute Python code in a persistent E2B sandbox session.

    Each instance of this tool maintains a single sandbox session.
    The session persists for the lifetime of the tool instance.
    Make sure to call the close() method when the session is no longer needed.
    """
    name: str = "e2b_code_interpreter_session"
    description: str = (
        "Executes Python code in a persistent, secure, isolated sandbox session. "
        "Variables, files, and installed packages persist between calls to the same session. "
        "Use this to run sequential Python code steps, install packages, and work with files."
    )
    args_schema: Type[BaseModel] = E2BCodeInterpreterInput

    # Internal state for the session
    # IMPORTANT: These fields are handled internally and are not part of the Pydantic model validation
    # for the tool's input/output. They are managed by the class instance itself.
    _sandbox: Optional[Sandbox] = None
    _instance_id: Optional[str] = None
    _timeout_seconds: int = 300  # Default timeout of 5 minutes (300 seconds)

    def __init__(self, timeout_seconds: int = 300, **kwargs):
        """Initialize the E2B Code Interpreter tool.
        
        Args:
            timeout_seconds: Timeout in seconds for the sandbox session (default: 300 seconds / 5 minutes)
        """
        super().__init__(**kwargs)
        self._timeout_seconds = timeout_seconds

    # Allow Pydantic V1/Langchain BaseTool to handle the non-standard Sandbox type
    class Config:
        arbitrary_types_allowed = True

    def _initialize_sandbox(self) -> None:
        """Initializes the sandbox if it doesn't exist."""
        if self._sandbox is None:
            print("--- Initializing New E2B Sandbox Session ---")
            if not os.getenv("E2B_API_KEY"):
                raise ValueError("E2B_API_KEY environment variable not set.")
            # Generate a UUID for tracking, associate it via metadata
            self._instance_id = str(uuid.uuid4())
            self._sandbox = Sandbox(
                metadata={'hinge_internal_id': self._instance_id},
                timeout=self._timeout_seconds
            )
            print(f"--- E2B Sandbox Session Initialized --- ID: {self._instance_id}")
            print(f"--- Sandbox timeout set to {self._timeout_seconds} seconds ---")
            print(f"--- Type of self._sandbox: {type(self._sandbox)} ---")

    def _run(self, code: Optional[str] = None, write_files: Optional[List[FileWriteInput]] = None, read_files: Optional[List[str]] = None, list_path: str = '/home/user', **kwargs: Any) -> Dict[str, Any]:
        """Use the tool to install packages, write files, run code, read files, and list files."""
        try:
            self._initialize_sandbox()

            if not self._sandbox or not self._instance_id:
                 # This should not happen if _initialize_sandbox worked
                return {"error": "Sandbox session not initialized."}

            # Ensure we have a local reference to the sandbox instance
            sandbox = self._sandbox # Assign the instance sandbox to a local variable
            session_id = self._instance_id

            # Initialize results dictionary structure
            results = {
                "session_id": session_id,
                "timeout_seconds": self._timeout_seconds,
                "code_stdout": [],
                "code_stderr": [],
                "code_results": [],
                "code_error": None,
                "write_files_errors": {},
                "read_files_content": {},
                "read_files_errors": {},
                "list_files_error": None,
                "sandbox_files": []
            }

            # 1. Write files (if requested)
            if write_files:
                for file_op in write_files:
                    try:
                        print(f"--- Writing file '{file_op.path}' (Session: {session_id}) ---")
                        sandbox.files.write(file_op.path, file_op.content)
                        print(f"--- File written successfully: {file_op.path} (Session: {session_id}) ---")
                    except Exception as e:
                        error_msg = f"Failed to write file '{file_op.path}': {e}"
                        print(f"--- Error: {error_msg} ---")
                        results["write_files_errors"][file_op.path] = str(e)

            # 2. Execute code (if requested)
            if code:
                try:
                    print(f"--- Running E2B Code (Session: {session_id}): ---\n{code}\n-------------------------")
                    execution = sandbox.run_code(code)
                    results["code_stdout"] = execution.logs.stdout
                    results["code_stderr"] = execution.logs.stderr
                    results["code_results"] = [res.text for res in execution.results if res.is_main_result]
                    results["code_error"] = str(execution.error) if execution.error else None
                    if execution.error:
                        error_msg = f"Failed to execute code: {results['code_error']}"
                        print(f"--- Error: {error_msg} (Session: {session_id}) ---")
                except Exception as e:
                    error_msg = f"Tool-level error during code execution: {e}"
                    print(f"--- Error: {error_msg} (Session: {session_id}) ---")
                    results["code_error"] = error_msg # Overwrite if tool-level error occurs

            # 3. Read files (if requested)
            if read_files:
                for path in read_files:
                    try:
                        print(f"--- Reading file '{path}' (Session: {session_id}) ---")
                        content = sandbox.files.read(path)
                        results["read_files_content"][path] = content
                        print(f"--- File read successfully: {path} (Session: {session_id}) ---")
                    except Exception as e:
                        error_msg = f"Failed to read file '{path}': {e}"
                        print(f"--- Error: {error_msg} ---")
                        results["read_files_errors"][path] = str(e)

            # 4. List files (always attempt, using specified path)
            try:
                print(f"--- Listing files in sandbox ({list_path}) (Session: {session_id}) ---")
                # Use sandbox.files.list based on previous findings
                files_info = sandbox.files.list(list_path)
                results["sandbox_files"] = [file_info.name for file_info in files_info]
                print(f"--- Found files ({list_path}): {results['sandbox_files']} ---")
            except Exception as e:
                error_msg = f"Failed to list files: {e}"
                print(f"--- Error: {error_msg} (Session: {session_id}) ---")
                results["list_files_error"] = str(e)

            # 5. Return combined results
            print(f"--- E2B Result (Session: {session_id}): ---\n{results}\n---------------------")
            return results
        except Exception as e:
            error_message = f"Failed to execute code in E2B sandbox session {self._instance_id}: {str(e)}"
            print(f"--- E2B Error (Session: {self._instance_id}): ---\n{e}\n---------------------")
            # Attempt to capture session_id even on error
            return {"session_id": self._instance_id, "error": error_message}

    # Optional: Implement _arun for asynchronous execution if needed

    def set_timeout(self, timeout_seconds: int) -> Dict[str, Any]:
        """Update the timeout for the sandbox session.
        
        Args:
            timeout_seconds: New timeout in seconds
            
        Returns:
            Dictionary with status information
        """
        self._timeout_seconds = timeout_seconds
        
        if self._sandbox:
            try:
                self._sandbox.set_timeout(timeout_seconds)
                print(f"--- Updated E2B Sandbox Timeout: {self._instance_id} to {timeout_seconds} seconds ---")
                return {
                    "session_id": self._instance_id,
                    "timeout_seconds": timeout_seconds,
                    "status": "updated"
                }
            except Exception as e:
                error_msg = f"Failed to update timeout: {str(e)}"
                print(f"--- Error: {error_msg} ---")
                return {
                    "session_id": self._instance_id,
                    "timeout_seconds": self._timeout_seconds,
                    "error": error_msg,
                    "status": "error"
                }
        else:
            return {
                "timeout_seconds": self._timeout_seconds,
                "status": "no_active_session",
                "message": "No active sandbox session. Timeout will be applied when a new session is created."
            }

    def close(self) -> None:
        """Closes the sandbox session and releases resources."""
        if self._sandbox:
            print(f"--- Closing E2B Sandbox Session: {self._instance_id} ---")
            try:
                self._sandbox.kill()
                print(f"--- E2B Sandbox Session Closed: {self._instance_id} ---")
            except Exception as e:
                print(f"--- Error closing E2B Sandbox Session {self._instance_id}: {e} ---")
            finally:
                self._sandbox = None
                self._instance_id = None
        else:
             print("--- No active E2B Sandbox Session to close for this instance ---")


# Example usage can be found in test_e2b.py
