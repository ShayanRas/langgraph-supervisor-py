"""
Simple tests for the E2B Code Interpreter tool.
These tests verify the core functionality of the tool.
"""

from main import E2BCodeInterpreterTool, FileWriteInput

def test_basic_functionality():
    """Test basic functionality of the E2B Code Interpreter tool."""
    print("\n--- Basic Functionality Test Start ---")
    
    # Create a tool instance with custom timeout
    tool = E2BCodeInterpreterTool(timeout_seconds=600)  # 10 minutes
    
    try:
        # Test 1: Simple code execution
        print("\n--- Test 1: Simple Code Execution ---")
        result1 = tool._run(code="x = 42; print(f'The answer is {x}'); x")
        print("Result 1: Success")
        
        # Verify code execution worked
        assert "The answer is 42" in "".join(result1.get("code_stdout", []))
        assert result1.get("code_results") == ['42']
        assert result1.get("code_error") is None
        assert result1.get("timeout_seconds") == 600  # Verify timeout is set correctly
        
        # Save session ID for later tests
        session_id = result1.get("session_id")
        assert session_id is not None
        
        # Test 2: State persistence
        print("\n--- Test 2: State Persistence ---")
        result2 = tool._run(code="x += 1; print(f'New value: {x}'); x")
        print("Result 2: Success")
        
        # Verify state persisted
        assert result2.get("session_id") == session_id
        assert "New value: 43" in "".join(result2.get("code_stdout", []))
        assert result2.get("code_results") == ['43']
        
        # Test 3: File operations
        print("\n--- Test 3: File Operations ---")
        result3 = tool._run(
            write_files=[FileWriteInput(path='/home/user/test.txt', content='Hello, E2B!')],
            code="with open('/home/user/test.txt', 'r') as f: content = f.read(); print(f'File content: {content}')",
            read_files=['/home/user/test.txt']
        )
        print("Result 3: Success")
        
        # Verify file operations
        assert "File content: Hello, E2B!" in "".join(result3.get("code_stdout", []))
        assert result3.get("read_files_content", {}).get('/home/user/test.txt') == 'Hello, E2B!'
        assert 'test.txt' in result3.get("sandbox_files", [])
        
        # Test 4: Error handling
        print("\n--- Test 4: Error Handling ---")
        result4 = tool._run(code="print(undefined_variable)")
        print("Result 4: Success (Error correctly handled)")
        
        # Verify error handling
        assert result4.get("code_error") is not None
        assert "undefined_variable" in result4.get("code_error", "")
        
        # Test 5: Session reset
        print("\n--- Test 5: Session Reset ---")
        # Close the session
        tool.close()
        
        # Create a new session
        result5 = tool._run(code="print(f'x is defined: {\"x\" in globals()}')")
        print("Result 5: Success")
        
        # Verify session was reset
        new_session_id = result5.get("session_id")
        assert new_session_id is not None
        assert new_session_id != session_id
        assert "x is defined: False" in "".join(result5.get("code_stdout", []))
        
    finally:
        # Clean up
        print("\n--- Cleaning Up ---")
        tool.close()
    
    print("\n--- Basic Functionality Test End ---")

def test_timeout_management():
    """Test timeout management capabilities of the E2B Code Interpreter tool."""
    print("\n--- Timeout Management Test Start ---")
    
    # Create a tool instance with default timeout
    tool = E2BCodeInterpreterTool()
    
    try:
        # Test 1: Check default timeout
        print("\n--- Test 1: Default Timeout ---")
        result1 = tool._run(code="print('Testing default timeout')")
        print("Result 1: Success")
        
        # Verify default timeout
        assert result1.get("timeout_seconds") == 300  # Default is 5 minutes (300 seconds)
        assert result1.get("session_id") is not None
        session_id = result1.get("session_id")
        
        # Test 2: Update timeout
        print("\n--- Test 2: Update Timeout ---")
        result2 = tool.set_timeout(900)  # 15 minutes
        print("Result 2: Success")
        
        # Verify timeout update
        assert result2.get("timeout_seconds") == 900
        assert result2.get("status") == "updated"
        assert result2.get("session_id") == session_id
        
        # Test 3: Verify timeout in subsequent operations
        print("\n--- Test 3: Verify Updated Timeout ---")
        result3 = tool._run(code="print('Testing with updated timeout')")
        print("Result 3: Success")
        
        # Verify timeout is reflected in results
        assert result3.get("timeout_seconds") == 900
        assert result3.get("session_id") == session_id
        
    finally:
        # Clean up
        print("\n--- Cleaning Up ---")
        tool.close()
    
    print("\n--- Timeout Management Test End ---")

if __name__ == "__main__":
    test_basic_functionality()
    test_timeout_management()
