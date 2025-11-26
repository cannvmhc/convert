import pandas as pd
from typing import List, Dict, Any
from loguru import logger


class ExcelParser:
    """Utility class for parsing Excel files into structured data"""
    
    def __init__(self):
        """Initialize Excel parser"""
        pass
    
    def parse_excel_file(self, file_path: str, chunk_size: int = 10000) -> List[Dict[str, Any]]:
        """
        Parse Excel file and extract all sheets with rows as JSON
        Uses chunked reading for large files to avoid memory issues
        
        Args:
            file_path: Path to Excel file
            chunk_size: Number of rows to process at once (default: 10000)
            
        Returns:
            List of dictionaries with structure:
            [
                {
                    'sheet_name': 'Sheet1',
                    'row_index': 1,
                    'row_data': {'header1': 'value1', 'header2': 'value2', ...}
                },
                ...
            ]
        """
        try:
            logger.info(f"Parsing Excel file: {file_path}")
            
            # Read all sheets from Excel file
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            all_rows = []
            
            for sheet_name in excel_file.sheet_names:
                logger.info(f"Processing sheet: {sheet_name}")
                
                # Read sheet into DataFrame with chunking for large files
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Skip empty sheets
                if df.empty:
                    logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                    continue
                
                total_rows = len(df)
                logger.info(f"Sheet '{sheet_name}' has {total_rows} rows")
                
                # Process in chunks to avoid memory issues
                for chunk_start in range(0, total_rows, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, total_rows)
                    chunk_df = df.iloc[chunk_start:chunk_end]
                    
                    # Parse each row in chunk
                    for idx, row in chunk_df.iterrows():
                        # Convert row to dictionary (header: value pairs)
                        row_dict = row.to_dict()
                        
                        # Clean up NaN values (convert to None for JSON compatibility)
                        row_dict = {
                            k: (None if pd.isna(v) else v)
                            for k, v in row_dict.items()
                        }
                        
                        # Create row entry
                        row_entry = {
                            'sheet_name': sheet_name,
                            'row_index': idx + 1,  # 1-based index
                            'row_data': row_dict
                        }
                        
                        all_rows.append(row_entry)
                    
                    logger.info(f"Processed rows {chunk_start+1} to {chunk_end} of sheet '{sheet_name}'")
                    
                    # Clear chunk from memory
                    del chunk_df
                
                logger.info(f"Completed parsing {total_rows} rows from sheet '{sheet_name}'")
                
                # Clear dataframe from memory
                del df
            
            logger.info(f"Total rows parsed from file: {len(all_rows)}")
            return all_rows
            
        except Exception as e:
            logger.error(f"Error parsing Excel file {file_path}: {e}")
            raise
    
    def parse_sheet(self, file_path: str, sheet_name: str) -> List[Dict[str, Any]]:
        """
        Parse a specific sheet from Excel file
        
        Args:
            file_path: Path to Excel file
            sheet_name: Name of sheet to parse
            
        Returns:
            List of row dictionaries
        """
        try:
            logger.info(f"Parsing sheet '{sheet_name}' from file: {file_path}")
            
            # Read specific sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            
            if df.empty:
                logger.warning(f"Sheet '{sheet_name}' is empty")
                return []
            
            rows = []
            for idx, row in df.iterrows():
                row_dict = row.to_dict()
                
                # Clean up NaN values
                row_dict = {
                    k: (None if pd.isna(v) else v)
                    for k, v in row_dict.items()
                }
                
                row_entry = {
                    'sheet_name': sheet_name,
                    'row_index': idx + 1,
                    'row_data': row_dict
                }
                
                rows.append(row_entry)
            
            logger.info(f"Parsed {len(rows)} rows from sheet '{sheet_name}'")
            return rows
            
        except Exception as e:
            logger.error(f"Error parsing sheet '{sheet_name}' from {file_path}: {e}")
            raise
    
    def get_sheet_names(self, file_path: str) -> List[str]:
        """
        Get list of all sheet names in Excel file
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            List of sheet names
        """
        try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            logger.info(f"Found {len(sheet_names)} sheets in file: {sheet_names}")
            return sheet_names
        except Exception as e:
            logger.error(f"Error getting sheet names from {file_path}: {e}")
            raise
