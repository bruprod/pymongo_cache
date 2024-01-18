"""Class representing a pymongo query and its information."""
from dataclasses import dataclass
from hashlib import md5
from typing import Dict, Any, Optional, Mapping, Sequence


@dataclass
class QueryInfo:
    """Class representing a pymongo query and its information."""
    function_name: Optional[str]
    query: Optional[Dict[str, Any]] = None
    projection: Optional[Dict[str, Any]] = None
    sort: Optional[Dict[str, Any]] = None
    skip: Optional[int] = None
    limit: Optional[int] = None
    pipeline: Optional[Sequence[Mapping[str, Any]]] = None

    def __hash__(self):
        """Return the hash of the query."""
        hash_val = int(md5(str(self).encode()).hexdigest(), 16) % (10 ** 8)
        return hash_val
