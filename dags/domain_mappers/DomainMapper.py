from abc import ABC, abstractmethod
from typing import List, Dict, Any

class DomainMapper(ABC):
    def map(seld, row: Dict[str, Any]) -> Dict[str, Any]:
        pass

class DomainMapperContext:
    def __init__(self, mapper: DomainMapper):
        self.mapper = mapper

    def execute(self, rows: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [self.mapper.map(rows) for row in rows if row]