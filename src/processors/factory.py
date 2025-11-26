from typing import Dict, Type
from loguru import logger
from src.processors.import_processor import ImportProcessor
from src.processors.data_processor import DataProcessor


class ProcessorFactory:
    """Factory class for creating processors for both flows"""
    
    # Registry for data processors (Flow 2)
    _data_processors: Dict[str, Type[DataProcessor]] = {}
    
    @classmethod
    def register_data_processor(cls, file_type: str, processor_class: Type[DataProcessor]):
        """
        Register a data processor class for a specific file type
        
        Args:
            file_type: Type identifier (e.g., "type1", "type2")
            processor_class: DataProcessor class to register
        """
        cls._data_processors[file_type] = processor_class
        logger.info(f"Registered data processor {processor_class.__name__} for type '{file_type}'")
    
    @classmethod
    def get_data_processor(cls, file_type: str) -> DataProcessor:
        """
        Get data processor instance for a specific file type
        
        Args:
            file_type: Type identifier
            
        Returns:
            Instance of the appropriate data processor
            
        Raises:
            ValueError: If no processor registered for the type
        """
        processor_class = cls._data_processors.get(file_type)
        
        if processor_class is None:
            logger.warning(f"No data processor registered for type '{file_type}', using default")
            # Return default processor if available
            processor_class = cls._data_processors.get('default')
            if processor_class is None:
                raise ValueError(f"No data processor registered for type: {file_type}")
        
        logger.debug(f"Creating data processor {processor_class.__name__} for type '{file_type}'")
        return processor_class()
    
    @classmethod
    def get_import_processor(cls) -> ImportProcessor:
        """
        Get import processor instance (Flow 1)
        
        Returns:
            ImportProcessor instance
        """
        return ImportProcessor()
    
    @classmethod
    def list_registered_types(cls) -> list:
        """
        Get list of all registered data processor types
        
        Returns:
            List of registered type identifiers
        """
        return list(cls._data_processors.keys())
