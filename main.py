from mcp.server.fastmcp import FastMCP, Context
from opcua import Client
from contextlib import asynccontextmanager
from typing import AsyncIterator
import asyncio
import os
import sys
from typing import List, Dict, Any
from opcua import ua

server_url = os.getenv("OPCUA_SERVER_URL", "opc.tcp://192.168.0.100:5005")
username = os.getenv("OPCUA_USERNAME")
password = os.getenv("OPCUA_PASSWORD")

# Global client variable for lazy connection
_opcua_client = None

async def get_opcua_client():
    """Get or create OPC UA client connection."""
    global _opcua_client
    
    if _opcua_client is None:
        _opcua_client = Client(server_url)
        
        # Set security policy to None for basic connections
       # _opcua_client.set_security_string("None")
        
        # Set authentication if username and password are provided
        if username and password:
            _opcua_client.set_user(username)
            _opcua_client.set_password(password)
        
        # Connect
        await asyncio.to_thread(_opcua_client.connect)
        print(f"Connected to OPC UA server at {server_url}", file=sys.stderr)
    
    return _opcua_client

async def cleanup_client():
    """Cleanup the global client connection."""
    global _opcua_client
    if _opcua_client:
        try:
            await asyncio.to_thread(_opcua_client.disconnect)
            print("Disconnected from OPC UA server", file=sys.stderr)
        except Exception as e:
            print(f"Error during disconnect: {e}", file=sys.stderr)
        finally:
            _opcua_client = None

def detect_and_convert_value(node, value: str) -> ua.Variant:
    """
    Detect the correct data type for a node and convert the value accordingly.
    This is the key enhancement to fix type mismatch issues.
    """
    try:
        # Get the current data value to determine the expected type
        current_data_value = node.get_data_value()
        current_variant_type = current_data_value.Value.VariantType
        
        print(f"Node expects variant type: {current_variant_type}", file=sys.stderr)
        
        # Convert based on the detected type
        if current_variant_type == ua.VariantType.Int16:
            return ua.Variant(int(value), ua.VariantType.Int16)
        elif current_variant_type == ua.VariantType.Int32:
            return ua.Variant(int(value), ua.VariantType.Int32)
        elif current_variant_type == ua.VariantType.UInt16:
            return ua.Variant(int(value), ua.VariantType.UInt16)
        elif current_variant_type == ua.VariantType.UInt32:
            return ua.Variant(int(value), ua.VariantType.UInt32)
        elif current_variant_type == ua.VariantType.Byte:
            return ua.Variant(int(value), ua.VariantType.Byte)
        elif current_variant_type == ua.VariantType.SByte:
            return ua.Variant(int(value), ua.VariantType.SByte)
        elif current_variant_type == ua.VariantType.Int64:
            return ua.Variant(int(value), ua.VariantType.Int64)
        elif current_variant_type == ua.VariantType.UInt64:
            return ua.Variant(int(value), ua.VariantType.UInt64)
        elif current_variant_type == ua.VariantType.Float:
            return ua.Variant(float(value), ua.VariantType.Float)
        elif current_variant_type == ua.VariantType.Double:
            return ua.Variant(float(value), ua.VariantType.Double)
        elif current_variant_type == ua.VariantType.Boolean:
            bool_value = value.lower() in ('true', '1', 'on', 'yes')
            return ua.Variant(bool_value, ua.VariantType.Boolean)
        elif current_variant_type == ua.VariantType.String:
            return ua.Variant(str(value), ua.VariantType.String)
        else:
            # Fallback: try to auto-detect from the value
            print(f"Unknown variant type {current_variant_type}, attempting auto-detection", file=sys.stderr)
            return auto_detect_variant(value)
            
    except Exception as e:
        print(f"Could not detect type from node, using auto-detection: {e}", file=sys.stderr)
        return auto_detect_variant(value)

def auto_detect_variant(value: str) -> ua.Variant:
    """Auto-detect the appropriate variant type from the string value."""
    # Try boolean first
    if value.lower() in ('true', 'false', 'on', 'off', 'yes', 'no'):
        bool_value = value.lower() in ('true', 'on', 'yes', '1')
        return ua.Variant(bool_value, ua.VariantType.Boolean)
    
    # Try integer
    try:
        int_value = int(value)
        # Choose appropriate integer type based on range
        if -32768 <= int_value <= 32767:
            return ua.Variant(int_value, ua.VariantType.Int16)
        elif -2147483648 <= int_value <= 2147483647:
            return ua.Variant(int_value, ua.VariantType.Int32)
        else:
            return ua.Variant(int_value, ua.VariantType.Int64)
    except ValueError:
        pass
    
    # Try float
    try:
        float_value = float(value)
        return ua.Variant(float_value, ua.VariantType.Float)
    except ValueError:
        pass
    
    # Default to string
    return ua.Variant(str(value), ua.VariantType.String)

# Simple lifespan that doesn't connect immediately
@asynccontextmanager
async def opcua_lifespan(server: FastMCP) -> AsyncIterator[dict]:
    """Handle OPC UA client connection lifecycle."""
    try:
        yield {}  # Don't connect at startup
    finally:
        await cleanup_client()

# Create an MCP server instance
mcp = FastMCP("OPCUA-Control", lifespan=opcua_lifespan)

# Tool: Read the value of an OPC UA node
@mcp.tool()
async def read_opcua_node(node_id: str, ctx: Context) -> str:
    """
    Read the value of a specific OPC UA node.
    
    Parameters:
        node_id (str): The OPC UA node ID in the format 'ns=<namespace>;i=<identifier>'.
                       Example: 'ns=2;i=2'.
    
    Returns:
        str: The value of the node as a string, prefixed with the node ID.
    """
    try:
        client = await get_opcua_client()
        node = client.get_node(node_id)
        value = node.get_value()
        return f"Node {node_id} value: {value}"
    except Exception as e:
        return f"Error reading node {node_id}: {type(e).__name__}: {e}"

# Tool: Write a value to an OPC UA node - ENHANCED VERSION
@mcp.tool()
async def write_opcua_node(node_id: str, value: str, ctx: Context) -> str:
    """
    Write a value to a specific OPC UA node with enhanced type detection.
    
    Parameters:
        node_id (str): The OPC UA node ID in the format 'ns=<namespace>;i=<identifier>'.
                       Example: 'ns=2;i=3'.
        value (str): The value to write to the node. Will be automatically converted to the correct data type.
    
    Returns:
        str: A message indicating success or failure of the write operation.
    """
    try:
        client = await get_opcua_client()
        node = client.get_node(node_id)
        
        # Use enhanced type detection and conversion
        variant = await asyncio.to_thread(detect_and_convert_value, node, str(value))
        data_value = ua.DataValue(variant)
        
        # Write the value using proper OPC UA data types
        await asyncio.to_thread(node.set_data_value, data_value)
        
        # Verify the write
        new_value = await asyncio.to_thread(node.get_value)
        
        return f"Successfully wrote {value} to node {node_id}. Verified value: {new_value}"
        
    except Exception as e:
        return f"Error writing to node {node_id}: {type(e).__name__}: {e}"

@mcp.tool()
async def browse_opcua_node_children(node_id: str, ctx: Context) -> str:
    """
    Browse the children of a specific OPC UA node.

    Parameters:
        node_id (str): The OPC UA node ID to browse (e.g., 'ns=0;i=85' for Objects folder).

    Returns:
        str: A string representation of a list of child nodes, including their NodeId and BrowseName.
             Returns an error message on failure.
    """
    try:
        client = await get_opcua_client()
        node = client.get_node(node_id)
        children = node.get_children()
        
        children_info = []
        for child in children:
            try:
                browse_name = child.get_browse_name()
                children_info.append({
                    "node_id": child.nodeid.to_string(),
                    "browse_name": f"{browse_name.NamespaceIndex}:{browse_name.Name}"
                })
            except Exception as e:
                 children_info.append({
                     "node_id": child.nodeid.to_string(),
                     "browse_name": f"Error getting name: {e}"
                 })

        return f"Children of {node_id}: {children_info!r}" 
        
    except Exception as e:
        return f"Error browsing children of node {node_id}: {type(e).__name__}: {e}"

@mcp.tool()
async def read_multiple_opcua_nodes(node_ids: List[str], ctx: Context) -> str:
    """
    Read the values of multiple OPC UA nodes in a single request.

    Parameters:
        node_ids (List[str]): A list of OPC UA node IDs to read (e.g., ['ns=2;i=2', 'ns=2;i=3']).

    Returns:
        str: A string representation of a dictionary mapping node IDs to their values, or an error message.
    """
    try:
        client = await get_opcua_client()
        nodes_to_read = [client.get_node(nid) for nid in node_ids]
        values = []
        
        # Iterate over each node in nodes_to_read
        for node in nodes_to_read:
            try:
                value = node.get_value()
                values.append(value)
            except Exception as e:
                values.append(f"Error reading node {node.nodeid.to_string()}: {str(e)}")
        
        # Map node IDs to their corresponding values
        results = {node.nodeid.to_string(): value for node, value in zip(nodes_to_read, values)}
        
        return f"Read multiple nodes values: {results!r}"
        
    except Exception as e:
        return f"Error reading multiple nodes {node_ids}: {type(e).__name__}: {e}"
    
@mcp.tool()
async def write_multiple_opcua_nodes(nodes_to_write: List[Dict[str, Any]], ctx: Context) -> str:
    """
    Write values to multiple OPC UA nodes in a single request with enhanced type detection.

    Parameters:
        nodes_to_write (List[Dict[str, Any]]): A list of dictionaries, where each dictionary 
                                               contains 'node_id' (str) and 'value' (Any).
                                               Example: [{'node_id': 'ns=2;i=2', 'value': 10.5}, 
                                                         {'node_id': 'ns=2;i=3', 'value': 'active'}]

    Returns:
        str: A message indicating the success or failure of the write operation. 
             Returns status codes for each write attempt.
    """
    try:
        client = await get_opcua_client()
        
        # Iterate over nodes and values to set each value individually
        status_report = []
        for item in nodes_to_write:
            try:
                node = client.get_node(item['node_id'])
                
                # Use enhanced type detection and conversion
                variant = await asyncio.to_thread(detect_and_convert_value, node, str(item['value']))
                data_value = ua.DataValue(variant)
                
                # Write the value using proper OPC UA data types
                await asyncio.to_thread(node.set_data_value, data_value)
                
                # Verify the write
                new_value = await asyncio.to_thread(node.get_value)

                status_report.append({
                    "node_id": item['node_id'],
                    "value_written": item['value'],
                    "verified_value": new_value,
                    "status": "Success"
                })
            except Exception as e:
                status_report.append({
                    "node_id": item['node_id'],
                    "value_written": item['value'],
                    "status": f"Error: {e}"
                })
        
        return f"Write multiple nodes results: {status_report!r}"
        
    except Exception as e:
        return f"Error writing multiple nodes: {type(e).__name__}: {e}"

# Tool: Test connection
@mcp.tool()
async def test_opcua_connection(ctx: Context) -> str:
    """
    Test the OPC UA connection and return basic server information.
    
    Returns:
        str: Connection status and basic server info.
    """
    try:
        client = await get_opcua_client()
        
        # Get server info
        root = client.get_root_node()
        objects = client.get_objects_node()
        children = objects.get_children()
        
        return f"Connected successfully to {server_url}. Found {len(children)} objects in root folder."
        
    except Exception as e:
        return f"Connection test failed: {type(e).__name__}: {e}"

# Run the server
if __name__ == "__main__":
    mcp.run()
