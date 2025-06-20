from typing import List, Optional
from pydantic import BaseModel, ConfigDict
import pandas as pd
from core.logging_config import get_logger


logger = get_logger(__name__)

class Dataset(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    name: str
    data: pd.DataFrame
    file_format: str = "parquet"
    subdirectory: Optional[str] = None
    
    def get_file_path(self, base_path: str) -> str:
        """Generate file path for this dataset."""
        if self.subdirectory:
            return f"{base_path.rstrip('/')}/{self.subdirectory}/{self.name}"
        return f"{base_path.rstrip('/')}/{self.name}"

class DataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    datasets: List[Dataset]
    base_path: Optional[str] = None
    
    def get_dataset(self, name: str) -> Optional[Dataset]:
        """Get dataset by name."""
        return next((ds for ds in self.datasets if ds.name == name), None)
