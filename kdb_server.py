#!/usr/bin/env python3
"""
Clean KDB+ MCP Server - Connects to real KDB+ process on port 4000
"""

import os
import json
import sys
from datetime import datetime
from typing import Any, Dict

import polars as pl
import kola

# Global connection
kdb_connection = None

def connect_to_kdb():
    """Connect to KDB+ server"""
    global kdb_connection
    try:
        host = os.getenv('KDB_HOST', 'localhost')
        port = int(os.getenv('KDB_PORT', 4000))

        print(f"Connecting to KDB+ at {host}:{port}...")
        kdb_connection = kola.Q(host=host, port=port)
        print("✅ Connected to KDB+")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to KDB+: {e}")
        return False

def execute_kdb_query(query: str) -> str:
    """Execute KDB+ query on real server"""
    global kdb_connection

    try:
        # Connect if needed
        if kdb_connection is None:
            if not connect_to_kdb():
                return json.dumps({
                    "success": False,
                    "error": "Cannot connect to KDB+",
                    "query": query,
                    "timestamp": datetime.now().isoformat()
                })

        # Execute query
        print(f"Executing: {query}")
        kdb_connection.connect() # type: ignore
        result = kdb_connection.sync(query) # type: ignore

        # Convert to Python
        if hasattr(result, 'py'):
            py_result = result.py()
        else:
            py_result = result

        print(f"Raw result: {py_result} (type: {type(py_result)})")

        # Format response
        response = {
            "success": True,
            "error": None,
            "result": {
                "data": py_result,
                "type": str(type(py_result).__name__)
            },
            "query": query,
            "timestamp": datetime.now().isoformat()
        }

        return json.dumps(response, indent=2, default=str)

    except Exception as e:
        print(f"❌ Query failed: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "query": query,
            "timestamp": datetime.now().isoformat()
        })

def get_connection_status() -> str:
    """Get connection status"""
    global kdb_connection

    is_connected = kdb_connection is not None
    return json.dumps({
        "connected": is_connected,
        "host": os.getenv('KDB_HOST', 'localhost'),
        "port": int(os.getenv('KDB_PORT', 4000)),
        "timestamp": datetime.now().isoformat()
    })

def validate_syntax(query: str) -> str:
    """Basic syntax validation"""
    valid = len(query.strip()) > 0
    warnings = []

    if not query.strip():
        valid = False
        warnings.append("Empty query")

    return json.dumps({
        "query": query,
        "valid": valid,
        "warnings": warnings,
        "timestamp": datetime.now().isoformat()
    })

def get_help(topic: str = "") -> str:
    """Get help"""
    if topic == "select":
        return "KDB+ SELECT: select [columns] from table [where conditions]"
    return "Available help topics: select"

# MCP Server setup
try:
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("kdb-server")

    @mcp.tool()
    def execute_query(query: str) -> str:
        """Execute KDB+ query"""
        return execute_kdb_query(query)

    @mcp.tool()
    def connection_status() -> str:
        """Check KDB+ connection"""
        return get_connection_status()

    @mcp.tool()
    def syntax_check(query: str) -> str:
        """Validate query syntax"""
        return validate_syntax(query)

    @mcp.tool()
    def help_info(topic: str = "") -> str:
        """Get help information"""
        return get_help(topic)

    def run_server():
        """Run MCP server"""
        print("🚀 Starting KDB+ MCP Server...")
        print("Connecting to KDB+ on startup...")
        connect_to_kdb()
        print("Server ready for connections...")
        mcp.run()

except ImportError:
    print("❌ MCP not available")
    def run_server():
        print("Cannot run - MCP not installed")

def test_connection():
    """Test KDB+ connection"""
    print("🧪 Testing KDB+ Connection...")
    print("=" * 40)

    if not connect_to_kdb():
        print("❌ Cannot connect to KDB+")
        return False

    # Test simple query
    result = execute_kdb_query("1+1")
    parsed = json.loads(result)

    if parsed["success"] and parsed["result"]["data"] == 2:
        print("✅ Query test passed: 1+1 = 2")
    else:
        print(f"❌ Query test failed: {result}")
        return False

    # Test connection status
    status = get_connection_status()
    parsed_status = json.loads(status)

    if parsed_status["connected"]:
        print("✅ Connection status: Connected")
    else:
        print("❌ Connection status: Disconnected")
        return False

    print("🎉 All tests passed!")
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            success = test_connection()
            sys.exit(0 if success else 1)
        elif sys.argv[1] == "--server":
            run_server()
        else:
            print(f"Unknown option: {sys.argv[1]}")
            print("Usage: python kdb_server.py [--test|--server]")
    else:
        # Default: run server
        run_server()