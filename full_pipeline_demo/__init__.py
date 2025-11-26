"""
Full Pipeline Demo Package
==========================
End-to-end flight pricing pipeline demonstration.
"""

from .config import (
    SNOWFLAKE_CONFIG,
    DEMO_SCHEMAS,
    DEMO_TABLES,
    DATA_CONFIG,
    AIRPORTS,
    AIRLINES,
)

__version__ = '1.0.0'
__all__ = [
    'SNOWFLAKE_CONFIG',
    'DEMO_SCHEMAS', 
    'DEMO_TABLES',
    'DATA_CONFIG',
    'AIRPORTS',
    'AIRLINES',
]
