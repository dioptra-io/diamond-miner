from dataclasses import fields
from typing import Any, Dict


def common_parameters(from_dataclass: Any, to_dataclass: Any) -> Dict[str, Any]:
    to_params = {field.name for field in fields(to_dataclass)}
    return {
        field.name: getattr(from_dataclass, field.name)
        for field in fields(from_dataclass)
        if field.name in to_params
    }
