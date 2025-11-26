import json
from abc import ABC, abstractmethod
from typing import Dict, List
from loguru import logger

from src.database.mysql_client import MySQLClient
from src.database.redis_client import RedisClient


class DataProcessor(ABC):
    """Base class for Flow 2: Process data from database"""
    
    def __init__(self):
        self.mysql_client = MySQLClient()
        self.redis_client = RedisClient()
    
    @abstractmethod
    def process_row(self, row_data: Dict) -> Dict:
        """
        Process a single row of data
        
        Args:
            row_data: Row data dictionary from JSON
            
        Returns:
            Processed row data dictionary
        """
        pass
    
    def check_duplicate(self, row_data: Dict) -> bool:
        """
        Check if row is duplicate using Redis
        
        Args:
            row_data: Row data dictionary
            
        Returns:
            True if new (not duplicate), False if duplicate
        """
        return self.redis_client.check_and_add_if_new(row_data)
    
    def process_excel_row(self, excel_row: Dict) -> bool:
        """
        Process a single Excel row from database
        
        Args:
            excel_row: Excel row record from database with keys:
                      - id, upload_id, sheet_name, row_index, row_data (JSON string), status
            
        Returns:
            True if successful, False otherwise
        """
        row_id = excel_row['id']
        
        try:
            # Parse JSON row_data
            row_data = json.loads(excel_row['row_data']) if isinstance(excel_row['row_data'], str) else excel_row['row_data']
            
            logger.debug(f"Processing row {row_id} from sheet '{excel_row['sheet_name']}'")
            
            # Check for duplicates
            if not self.check_duplicate(row_data):
                logger.info(f"Row {row_id} is duplicate, skipping")
                self.mysql_client.update_excel_row_status(row_id, 2, error_message="Duplicate row")
                return False
            
            # Process the row (implemented by subclass)
            processed_data = self.process_row(row_data)
            
            # Update database with processed data
            self.mysql_client.update_excel_row_status(row_id, 1, updated_data=processed_data)
            
            logger.debug(f"Successfully processed row {row_id}")
            return True
            
        except Exception as e:
            error_msg = f"Error processing row: {str(e)}"
            logger.error(f"Row {row_id}: {error_msg}")
            self.mysql_client.update_excel_row_status(row_id, 2, error_message=error_msg)
            return False
    
    def process_pending_rows(self, batch_size: int = 100) -> int:
        """
        Process all pending Excel rows (status = 0)
        
        Args:
            batch_size: Number of rows to process in one batch
            
        Returns:
            Number of rows successfully processed
        """
        try:
            # Get pending rows
            pending_rows = self.mysql_client.get_pending_excel_rows(limit=batch_size)
            
            if not pending_rows:
                logger.info("No pending rows to process")
                return 0
            
            logger.info(f"Found {len(pending_rows)} pending rows to process")
            
            # Process each row
            success_count = 0
            for excel_row in pending_rows:
                if self.process_excel_row(excel_row):
                    success_count += 1
            
            logger.info(f"Processing batch complete: {success_count}/{len(pending_rows)} successful")
            return success_count
            
        except Exception as e:
            logger.error(f"Error processing pending rows: {e}")
            return 0


class Type1DataProcessor(DataProcessor):
    """Data processor for Type 1 files"""
    
    def process_row(self, row_data: Dict) -> Dict:
        """
        Process Type 1 row data
        
        Args:
            row_data: Row data dictionary
            
        Returns:
            Processed row data
        """
        # Example: Add custom processing logic here
        # For now, just return the original data
        logger.debug(f"Processing Type 1 row: {row_data}")
        
        # You can add transformations here, for example:
        # processed_data = row_data.copy()
        # processed_data['processed_at'] = datetime.now().isoformat()
        # processed_data['some_field'] = transform_function(row_data['some_field'])
        
        return row_data


class Type2DataProcessor(DataProcessor):
    """Data processor for Type 2 files"""
    
    def process_row(self, row_data: Dict) -> Dict:
        """
        Process Type 2 row data
        
        Args:
            row_data: Row data dictionary
            
        Returns:
            Processed row data
        """
        # Example: Different processing logic for Type 2
        logger.debug(f"Processing Type 2 row: {row_data}")
        
        # Add your custom logic here
        
        return row_data


class DefaultDataProcessor(DataProcessor):
    """Default data processor for unknown types"""
    
    def process_row(self, row_data: Dict) -> Dict:
        """
        Process row data with default logic
        
        Args:
            row_data: Row data dictionary
            
        Returns:
            Processed row data
        """
        logger.debug(f"Processing with default processor: {row_data}")
        return row_data
