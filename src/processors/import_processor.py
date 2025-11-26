from typing import Dict
from loguru import logger

from src.database.mysql_client import MySQLClient
from src.utils.file_downloader import FileDownloader
from src.utils.excel_parser import ExcelParser


class ImportProcessor:
    """Processor for Flow 1: Import Excel files into database"""
    
    def __init__(self):
        self.mysql_client = MySQLClient()
        self.file_downloader = FileDownloader()
        self.excel_parser = ExcelParser()
    
    def process_file(self, file_info: Dict, parse_chunk_size: int = 10000, insert_chunk_size: int = 5000) -> bool:
        """
        Process a file: download, parse Excel, import to database
        Uses chunking for large files to avoid memory issues
        
        Args:
            file_info: File information from database (id, path, type, status)
            parse_chunk_size: Number of rows to parse at once (default: 10000)
            insert_chunk_size: Number of rows to insert per batch (default: 5000)
            
        Returns:
            True if successful, False otherwise
        """
        file_id = file_info['id']
        file_path = file_info['path']
        temp_file_path = None
        
        try:
            logger.info(f"Starting import process for file {file_id}: {file_path}")
            
            # Step 1: Download/copy file to temp
            temp_file_path = self.file_downloader.download_file(file_path)
            logger.info(f"File downloaded to: {temp_file_path}")
            
            # Step 2: Parse Excel file with chunking
            logger.info(f"Parsing Excel file with chunk size: {parse_chunk_size}")
            rows_data = self.excel_parser.parse_excel_file(temp_file_path, chunk_size=parse_chunk_size)
            logger.info(f"Parsed {len(rows_data)} rows from Excel file")
            
            if not rows_data:
                logger.warning(f"No data found in file {file_id}")
                self.mysql_client.update_file_status(file_id, 2, "No data found in Excel file")
                return False
            
            # Step 3: Insert rows into database with chunking
            logger.info(f"Inserting rows with chunk size: {insert_chunk_size}")
            self.mysql_client.insert_excel_rows(file_id, rows_data, chunk_size=insert_chunk_size)
            
            # Step 4: Update file status and total rows
            self.mysql_client.update_file_total_rows(file_id, len(rows_data))
            self.mysql_client.update_file_status(file_id, 1)  # Status 1 = imported
            
            logger.info(f"Successfully imported file {file_id} with {len(rows_data)} rows")
            
            # Clear rows_data from memory
            del rows_data
            
            return True
            
        except Exception as e:
            error_msg = f"Error importing file: {str(e)}"
            logger.error(error_msg)
            self.mysql_client.update_file_status(file_id, 2, error_msg)
            return False
            
        finally:
            # Cleanup temp file
            if temp_file_path:
                self.file_downloader.cleanup_file(temp_file_path)
    
    def process_pending_files(self, batch_size: int = 10) -> int:
        """
        Process all pending files (status = 0)
        
        Args:
            batch_size: Number of files to process in one batch
            
        Returns:
            Number of files successfully processed
        """
        try:
            # Get pending files
            pending_files = self.mysql_client.get_pending_files(limit=batch_size)
            
            if not pending_files:
                logger.info("No pending files to import")
                return 0
            
            logger.info(f"Found {len(pending_files)} pending files to import")
            
            # Process each file
            success_count = 0
            for file_info in pending_files:
                if self.process_file(file_info):
                    success_count += 1
            
            logger.info(f"Import batch complete: {success_count}/{len(pending_files)} successful")
            return success_count
            
        except Exception as e:
            logger.error(f"Error processing pending files: {e}")
            return 0
