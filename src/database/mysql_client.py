import pymysql
from pymysql.cursors import DictCursor
from typing import List, Dict, Optional
from loguru import logger
from src.config.settings import settings


class MySQLClient:
    """Singleton MySQL client with connection pooling"""
    
    _instance: Optional['MySQLClient'] = None
    _connection: Optional[pymysql.Connection] = None
    
    def __new__(cls):
        """Ensure only one instance exists (Singleton pattern)"""
        if cls._instance is None:
            cls._instance = super(MySQLClient, cls).__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance
    
    def _initialize_connection(self):
        """Initialize MySQL connection"""
        try:
            self._connection = pymysql.connect(
                host=settings.MYSQL_HOST,
                port=settings.MYSQL_PORT,
                user=settings.MYSQL_USER,
                password=settings.MYSQL_PASSWORD,
                database=settings.MYSQL_DATABASE,
                cursorclass=DictCursor,
                autocommit=True,  # Enable autocommit for faster inserts
                local_infile=True,  # Enable LOAD DATA LOCAL INFILE
                charset='utf8mb4'
            )
            logger.info(f"MySQL connection established to {settings.MYSQL_HOST}:{settings.MYSQL_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    def _ensure_connection(self):
        """Ensure connection is alive, reconnect if needed"""
        try:
            if self._connection is None or not self._connection.open:
                logger.warning("MySQL connection lost, reconnecting...")
                self._initialize_connection()
            else:
                # Ping to check if connection is alive
                self._connection.ping(reconnect=True)
        except Exception as e:
            logger.error(f"Failed to ensure MySQL connection: {e}")
            self._initialize_connection()
    
    def get_pending_files(self, limit: int = None) -> List[Dict]:
        """
        Get files with status = 0 from database
        
        Args:
            limit: Maximum number of files to fetch
            
        Returns:
            List of file records as dictionaries
        """
        self._ensure_connection()
        
        try:
            with self._connection.cursor() as cursor:
                query = "SELECT * FROM uploads WHERE status = 0 and id = 25"
                if limit:
                    query += f" LIMIT {limit}"
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                logger.info(f"Fetched {len(results)} pending files from database")
                return results
                
        except Exception as e:
            logger.error(f"Error fetching pending files: {e}")
            raise
    
    def update_file_status(self, file_id: int, status: int, error_message: str = None):
        """
        Update file status in database
        
        Args:
            file_id: ID of the file to update
            status: New status value (0=pending, 1=imported, 2=error)
            error_message: Optional error message if status is error
        """
        self._ensure_connection()
        
        try:
            with self._connection.cursor() as cursor:
                if error_message:
                    query = "UPDATE uploads SET status = %s, error_message = %s, updated_at = NOW() WHERE id = %s"
                    cursor.execute(query, (status, error_message, file_id))
                else:
                    query = "UPDATE uploads SET status = %s, updated_at = NOW() WHERE id = %s"
                    cursor.execute(query, (status, file_id))
                
                logger.info(f"Updated file {file_id} status to {status}")
                
        except Exception as e:
            self._connection.rollback()
            logger.error(f"Error updating file status: {e}")
            raise
    
    def update_file_total_rows(self, file_id: int, total_rows: int):
        """
        Update total rows count for a file
        
        Args:
            file_id: ID of the file to update
            total_rows: Total number of rows imported
        """
        self._ensure_connection()
        
        try:
            with self._connection.cursor() as cursor:
                query = "UPDATE uploads SET total_rows = %s, updated_at = NOW() WHERE id = %s"
                cursor.execute(query, (total_rows, file_id))
                logger.info(f"Updated file {file_id} total_rows to {total_rows}")
                
        except Exception as e:
            self._connection.rollback()
            logger.error(f"Error updating file total_rows: {e}")
            raise
    
    def insert_excel_rows(self, upload_id: int, rows_data: List[Dict], chunk_size: int = 50000):
        """
        Bulk insert Excel rows using LOAD DATA INFILE - FASTEST METHOD
        Writes to temp CSV file then uses MySQL's native bulk loader
        
        Args:
            upload_id: ID of the file upload
            rows_data: List of row dictionaries
            chunk_size: Number of rows per temp file (default: 50000)
        """
        self._ensure_connection()
        
        if not rows_data:
            logger.warning("No rows to insert")
            return
        
        try:
            import json
            import csv
            import tempfile
            import os
            from time import time
            
            total_rows = len(rows_data)
            logger.info(f"FAST INSERT: {total_rows} rows using LOAD DATA INFILE")
            
            start_time = time()
            total_inserted = 0
            
            # Process in chunks to avoid huge temp files
            for chunk_start in range(0, total_rows, chunk_size):
                chunk_start_time = time()
                chunk_end = min(chunk_start + chunk_size, total_rows)
                chunk_data = rows_data[chunk_start:chunk_end]
                
                # Create temp CSV file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp_file:
                    temp_path = tmp_file.name
                    csv_writer = csv.writer(tmp_file, escapechar='\\', quoting=csv.QUOTE_MINIMAL)
                    
                    # Write data to CSV
                    for row in chunk_data:
                        csv_writer.writerow([
                            upload_id,
                            row['sheet_name'],
                            row['row_index'],
                            json.dumps(row['row_data'], ensure_ascii=False),
                            0  # status
                        ])
                
                try:
                    # Use LOAD DATA LOCAL INFILE for super fast insert
                    with self._connection.cursor() as cursor:
                        query = f"""
                            LOAD DATA LOCAL INFILE '{temp_path}'
                            INTO TABLE table_data
                            FIELDS TERMINATED BY ',' 
                            ENCLOSED BY '"'
                            ESCAPED BY '\\\\'
                            LINES TERMINATED BY '\\n'
                            (upload_id, sheet_name, row_index, row_data, status)
                        """
                        cursor.execute(query)
                        rows_inserted = cursor.rowcount
                        total_inserted += rows_inserted
                    
                    elapsed = time() - chunk_start_time
                    rows_per_sec = len(chunk_data) / elapsed if elapsed > 0 else 0
                    logger.info(f"✅ Loaded rows {chunk_start+1}-{chunk_end} in {elapsed:.2f}s ({rows_per_sec:.0f} rows/s)")
                    
                finally:
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            
            total_time = time() - start_time
            avg_speed = total_inserted / total_time if total_time > 0 else 0
            logger.info(f"🚀 COMPLETED: {total_inserted} rows in {total_time:.2f}s ({avg_speed:.0f} rows/s)")
            
        except Exception as e:
            logger.error(f"Error in LOAD DATA INFILE: {e}")
            logger.warning("Falling back to regular INSERT...")
            # Fallback to regular insert
            self._insert_excel_rows_fallback(upload_id, rows_data)
    
    def _insert_excel_rows_fallback(self, upload_id: int, rows_data: List[Dict]):
        """Fallback insert method using executemany"""
        try:
            import json
            from time import time
            
            logger.info(f"Fallback insert for {len(rows_data)} rows")
            start_time = time()
            
            with self._connection.cursor() as cursor:
                query = """
                    INSERT INTO table_data (upload_id, sheet_name, row_index, row_data, status)
                    VALUES (%s, %s, %s, %s, 0)
                """
                
                values = [
                    (
                        upload_id,
                        row['sheet_name'],
                        row['row_index'],
                        json.dumps(row['row_data'], ensure_ascii=False)
                    )
                    for row in rows_data
                ]
                
                cursor.executemany(query, values)
            
            elapsed = time() - start_time
            logger.info(f"Fallback insert completed in {elapsed:.2f}s")
            
        except Exception as e:
            logger.error(f"Fallback insert failed: {e}")
            raise
    
    def get_pending_excel_rows(self, limit: int = None) -> List[Dict]:
        """
        Get Excel rows with status = 0 from database
        
        Args:
            limit: Maximum number of rows to fetch
            
        Returns:
            List of row records as dictionaries
        """
        self._ensure_connection()
        
        try:
            with self._connection.cursor() as cursor:
                query = "SELECT * FROM excel_data WHERE status = 0"
                if limit:
                    query += f" LIMIT {limit}"
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                logger.info(f"Fetched {len(results)} pending Excel rows from database")
                return results
                
        except Exception as e:
            logger.error(f"Error fetching pending Excel rows: {e}")
            raise
    
    def update_excel_row_status(self, row_id: int, status: int, updated_data: Dict = None, error_message: str = None):
        """
        Update Excel row status and optionally update row_data
        
        Args:
            row_id: ID of the Excel row to update
            status: New status value (0=pending, 1=processed, 2=error)
            updated_data: Optional updated row_data dictionary
            error_message: Optional error message if status is error
        """
        self._ensure_connection()
        
        try:
            import json
            
            with self._connection.cursor() as cursor:
                if updated_data:
                    query = """
                        UPDATE excel_data 
                        SET status = %s, row_data = %s, error_message = %s, updated_at = NOW() 
                        WHERE id = %s
                    """
                    cursor.execute(query, (
                        status,
                        json.dumps(updated_data, ensure_ascii=False),
                        error_message,
                        row_id
                    ))
                else:
                    query = """
                        UPDATE excel_data 
                        SET status = %s, error_message = %s, updated_at = NOW() 
                        WHERE id = %s
                    """
                    cursor.execute(query, (status, error_message, row_id))
                
                self._connection.commit()
                logger.info(f"Updated Excel row {row_id} status to {status}")
                
        except Exception as e:
            self._connection.rollback()
            logger.error(f"Error updating Excel row status: {e}")
            raise
    
    def get_rows_by_upload_id(self, upload_id: int) -> List[Dict]:
        """
        Get all Excel rows for a specific upload
        
        Args:
            upload_id: ID of the file upload
            
        Returns:
            List of row records
        """
        self._ensure_connection()
        
        try:
            with self._connection.cursor() as cursor:
                query = "SELECT * FROM excel_data WHERE upload_id = %s ORDER BY sheet_name, row_index"
                cursor.execute(query, (upload_id,))
                results = cursor.fetchall()
                
                logger.info(f"Fetched {len(results)} rows for upload_id {upload_id}")
                return results
                
        except Exception as e:
            logger.error(f"Error fetching rows by upload_id: {e}")
            raise

    
    def close(self):
        """Close MySQL connection"""
        if self._connection and self._connection.open:
            self._connection.close()
            logger.info("MySQL connection closed")
