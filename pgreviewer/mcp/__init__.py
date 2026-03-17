from pgreviewer.mcp.client import MCPClient, is_available
from pgreviewer.mcp.wrappers import (
    mcp_get_explain_plan,
    mcp_get_schema_info,
    mcp_get_slow_queries,
    mcp_recommend_indexes,
)

__all__ = [
    "MCPClient",
    "is_available",
    "mcp_get_explain_plan",
    "mcp_get_schema_info",
    "mcp_get_slow_queries",
    "mcp_recommend_indexes",
]
