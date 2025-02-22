import asyncio
import logging
import os
from mysql.connector import connect, Error, OperationalError
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from pydantic import AnyUrl
from urllib.parse import quote

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mysql_mcp_server")

# Global connection state
CONNECTION_STATE = {
    "is_connected": False,
    "last_error": None,
    "connection_details": None
}

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE"),
        "charset": os.getenv("MYSQL_CHARSET", "utf8"),
        "connection_timeout": int(os.getenv("MYSQL_CONNECTION_TIMEOUT", "5")),  # 5 second timeout
        "get_warnings": True
    }
    
    if not all([config["user"], config["password"], config["database"]]):
        logger.error("Missing required database configuration. Please check environment variables:")
        logger.error("MYSQL_USER, MYSQL_PASSWORD, and MYSQL_DATABASE are required")
        raise ValueError("Missing required database configuration")
    
    return config

# Initialize server
app = Server("mysql_mcp_server")

@app.list_resources()
async def list_resources() -> list[Resource]:
    """List MySQL tables as resources."""
    # Check connection state first
    if not CONNECTION_STATE["is_connected"]:
        logger.info("Attempted to list resources, but not connected to MySQL")
        return []
        
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                logger.info(f"Found tables: {tables}")
                
                resources = []
                for table in tables:
                    table_name = table[0]
                    encoded_table_name = quote(table_name) # Properly encode the table name
                    
                    resources.append(
                        Resource(
                            uri=f"mysql://{encoded_table_name}/data",
                            name=f"Table: {table_name}",
                            mimeType="text/plain",
                            description=f"Data in table: {table_name}"
                        )
                    )
                return resources
    except OperationalError as e:
        # Specifically handle connection failures
        logger.error(f"MySQL connection failed: {str(e)}")
        # Update connection state
        CONNECTION_STATE["is_connected"] = False
        CONNECTION_STATE["last_error"] = str(e)
        return []
    except Error as e:
        logger.error(f"Failed to list resources: {str(e)}")
        return []

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read table contents."""
    # Check connection state first
    if not CONNECTION_STATE["is_connected"]:
        return "Error: Not connected to MySQL. Please use the 'connect' tool first."
        
    config = get_db_config()
    uri_str = str(uri)
    logger.info(f"Reading resource: {uri_str}")
    
    if not uri_str.startswith("mysql://"):
        raise ValueError(f"Invalid URI scheme: {uri_str}")
        
    parts = uri_str[8:].split('/')
    table = parts[0]
    
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table} LIMIT 100")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = [",".join(map(str, row)) for row in rows]
                return "\n".join([",".join(columns)] + result)
                
    except OperationalError as e:
        # Specifically handle connection failures
        logger.error(f"MySQL connection failed: {str(e)}")
        # Update connection state
        CONNECTION_STATE["is_connected"] = False
        CONNECTION_STATE["last_error"] = str(e)
        return f"Error: Connection to MySQL lost. {str(e)}"
    except Error as e:
        logger.error(f"Database error reading resource {uri}: {str(e)}")
        return f"Error: {str(e)}"

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available MySQL tools."""
    # Updated list_tools to include connect and disconnect
    logger.info("Listing tools...")
    return [
        Tool(
            name="execute_sql",
            description="Execute an SQL query on the MySQL server",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="connect",
            description="Connect to the MySQL server",
            inputSchema={
                "type": "object",
                "properties": {
                    "host": {
                        "type": "string",
                        "description": "MySQL server hostname (defaults to env var or localhost)"
                    },
                    "port": {
                        "type": "integer",
                        "description": "MySQL server port (defaults to env var or 3306)"
                    },
                    "user": {
                        "type": "string",
                        "description": "MySQL username (defaults to env var)"
                    },
                    "password": {
                        "type": "string",
                        "description": "MySQL password (defaults to env var)"
                    },
                    "database": {
                        "type": "string",
                        "description": "MySQL database name (defaults to env var)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="disconnect",
            description="Disconnect from the MySQL server",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="connection_status",
            description="Check the current MySQL connection status",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute MySQL tools."""
    logger.info(f"Calling tool: {name} with arguments: {arguments}")
    
    # Handle connect tool
    if name == "connect":
        try:
            # Start with default config
            config = get_db_config()
            
            # Override with any provided arguments
            if "host" in arguments:
                config["host"] = arguments["host"]
            if "port" in arguments:
                config["port"] = int(arguments["port"])
            if "user" in arguments:
                config["user"] = arguments["user"]
            if "password" in arguments:
                config["password"] = arguments["password"]
            if "database" in arguments:
                config["database"] = arguments["database"]
                
            # Try to connect
            with connect(**config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    # If we get here, connection was successful
                    
                    # Update connection state
                    CONNECTION_STATE["is_connected"] = True
                    CONNECTION_STATE["last_error"] = None
                    CONNECTION_STATE["connection_details"] = {
                        "host": config["host"],
                        "port": config["port"],
                        "user": config["user"],
                        "database": config["database"]
                    }
                    
                    return [TextContent(type="text", text=f"Successfully connected to MySQL at {config['host']}:{config['port']} as {config['user']} (database: {config['database']})")]
        except OperationalError as e:
            CONNECTION_STATE["is_connected"] = False
            CONNECTION_STATE["last_error"] = str(e)
            return [TextContent(type="text", text=f"Failed to connect to MySQL: {str(e)}")]
        except Error as e:
            CONNECTION_STATE["is_connected"] = False
            CONNECTION_STATE["last_error"] = str(e)
            return [TextContent(type="text", text=f"Database error: {str(e)}")]
    
    # Handle disconnect tool
    elif name == "disconnect":
        CONNECTION_STATE["is_connected"] = False
        CONNECTION_STATE["last_error"] = None
        return [TextContent(type="text", text="Disconnected from MySQL.")]
    
    # Handle connection_status tool
    elif name == "connection_status":
        if CONNECTION_STATE["is_connected"]:
            details = CONNECTION_STATE["connection_details"]
            return [TextContent(type="text", text=f"Connected to MySQL at {details['host']}:{details['port']} as {details['user']} (database: {details['database']})")]
        else:
            error_msg = ""
            if CONNECTION_STATE["last_error"]:
                error_msg = f" Last error: {CONNECTION_STATE['last_error']}"
            return [TextContent(type="text", text=f"Not connected to MySQL.{error_msg}")]
    
    # Handle execute_sql tool
    elif name == "execute_sql":
        # Check if we're connected first
        if not CONNECTION_STATE["is_connected"]:
            return [TextContent(type="text", text="Not connected to MySQL. Please use the 'connect' tool first.")]
        
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        
        try:
            # Use the stored config from the successful connection
            config = get_db_config()
            with connect(**config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    
                    # Special handling for SHOW TABLES
                    if query.strip().upper().startswith("SHOW TABLES"):
                        tables = cursor.fetchall()
                        result = ["Tables_in_" + config["database"]]  # Header
                        result.extend([table[0] for table in tables])
                        return [TextContent(type="text", text="\n".join(result))]
                    
                    # Regular SELECT queries
                    elif query.strip().upper().startswith("SELECT"):
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        result = [",".join(map(str, row)) for row in rows]
                        return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
                    
                    # Non-SELECT queries
                    else:
                        conn.commit()
                        return [TextContent(type="text", text=f"Query executed successfully. Rows affected: {cursor.rowcount}")]
                    
        except OperationalError as e:
            # If we get a connection error, update our state
            CONNECTION_STATE["is_connected"] = False
            CONNECTION_STATE["last_error"] = str(e)
            return [TextContent(type="text", text=f"Connection lost: {str(e)}. Please reconnect using the 'connect' tool.")]
        except Error as e:
            return [TextContent(type="text", text=f"Error executing query: {str(e)}")]
    
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    logger.info("Starting MySQL MCP server...")
    
    # Start with not connected state
    CONNECTION_STATE["is_connected"] = False
    CONNECTION_STATE["last_error"] = None
    
    try:
        # Just load the config but don't attempt connection
        config = get_db_config()
        logger.info(f"Database config available: {config['host']}/{config['database']} as {config['user']}")
        logger.info("Use the 'connect' tool to establish a connection to MySQL")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        # Continue starting the server despite config issues
    
    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    asyncio.run(main())