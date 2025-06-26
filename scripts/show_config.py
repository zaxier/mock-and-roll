import yaml
from config import get_config
from core.logging_config import setup_logging

if __name__ == "__main__":
    """Prints the fully resolved configuration object."""
    setup_logging(level="ERROR") # Keep output clean
    config = get_config()
    
    # Convert dataclasses to a dict for clean printing
    def config_to_dict(obj):
        if hasattr(obj, "__dict__"):
            return {k: config_to_dict(v) for k, v in obj.__dict__.items()}
        return obj

    config_dict = config_to_dict(config)
    print(yaml.dump(config_dict, default_flow_style=False, sort_keys=False))
