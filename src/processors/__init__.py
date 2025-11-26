from typing import Dict
from loguru import logger

from src.processors.factory import ProcessorFactory
from src.processors.data_processor import Type1DataProcessor, Type2DataProcessor, DefaultDataProcessor


def register_processors():
    """Register all data processors with the factory"""
    ProcessorFactory.register_data_processor("type1", Type1DataProcessor)
    ProcessorFactory.register_data_processor("type2", Type2DataProcessor)
    ProcessorFactory.register_data_processor("default", DefaultDataProcessor)
    
    logger.info(f"Registered data processor types: {ProcessorFactory.list_registered_types()}")
