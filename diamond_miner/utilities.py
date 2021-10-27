from dataclasses import fields


def common_parameters(from_dataclass, to_dataclass):
    to_params = {field.name for field in fields(to_dataclass)}
    return {
        field.name: getattr(from_dataclass, field.name)
        for field in fields(from_dataclass)
        if field.name in to_params
    }
