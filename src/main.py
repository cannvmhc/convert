#!/usr/bin/env python3
"""
Main application entry point for file processing tool
Supports two flows:
  - Flow 1 (import): Download Excel files and import to database
  - Flow 2 (process): Process data from database
"""

import sys
import signal
import argparse
from time import sleep
from loguru import logger

from src.config.settings import settings
from src.database.mysql_client import MySQLClient
from src.database.redis_client import RedisClient
from src.processors.factory import ProcessorFactory
from src.processors import register_processors


class FileProcessorApp:
    """Main application class for file processing"""
    
    def __init__(self, flow: str = "import"):
        """
        Initialize application
        
        Args:
            flow: Processing flow - "import" or "process"
        """
        self.flow = flow
        self.mysql_client = None
        self.redis_client = None
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def initialize(self):
        """Initialize application and validate configuration"""
        logger.info(f"Initializing File Processor Application (Flow: {self.flow})...")
        
        # Validate settings
        if not settings.validate():
            logger.error("Configuration validation failed")
            return False
        
        try:
            # Initialize database clients (singleton pattern ensures single connection)
            self.mysql_client = MySQLClient()
            self.redis_client = RedisClient()
            
            # Register processors
            register_processors()
            
            logger.info("Application initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize application: {e}")
            return False
    
    def run_import_flow(self):
        """Run Flow 1: Import Excel files to database"""
        logger.info("Starting Import Flow (Flow 1)...")
        
        import_processor = ProcessorFactory.get_import_processor()
        
        while self.running:
            try:
                # Process pending files
                processed_count = import_processor.process_pending_files(
                    batch_size=settings.BATCH_SIZE
                )
                
                if processed_count == 0:
                    logger.info("No pending files, waiting...")
                    sleep(10)  # Wait before checking again
                else:
                    logger.info(f"Processed {processed_count} files")
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                break
                
            except Exception as e:
                logger.error(f"Error in import flow: {e}")
                sleep(5)  # Wait before retrying
        
        logger.info("Import flow stopped")
    
    def run_process_flow(self):
        """Run Flow 2: Process data from database"""
        logger.info("Starting Process Flow (Flow 2)...")
        
        while self.running:
            try:
                # Get pending rows grouped by upload_id to process by type
                pending_files = self.mysql_client.get_pending_files(limit=settings.BATCH_SIZE)
                
                if not pending_files:
                    logger.info("No files with pending rows, waiting...")
                    sleep(10)
                    continue
                
                total_processed = 0
                
                for file_info in pending_files:
                    if not self.running:
                        break
                    
                    file_type = file_info.get('type', 'default')
                    upload_id = file_info['id']
                    
                    # Get data processor for this file type
                    data_processor = ProcessorFactory.get_data_processor(file_type)
                    
                    # Process pending rows for this upload
                    logger.info(f"Processing rows for upload_id {upload_id} (type: {file_type})")
                    processed_count = data_processor.process_pending_rows(
                        batch_size=settings.BATCH_SIZE
                    )
                    
                    total_processed += processed_count
                
                if total_processed == 0:
                    logger.info("No pending rows to process, waiting...")
                    sleep(10)
                else:
                    logger.info(f"Processed {total_processed} rows total")
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                break
                
            except Exception as e:
                logger.error(f"Error in process flow: {e}")
                sleep(5)  # Wait before retrying
        
        logger.info("Process flow stopped")
    
    def run(self):
        """Main application loop"""
        if self.flow == "import":
            self.run_import_flow()
        elif self.flow == "process":
            self.run_process_flow()
        else:
            logger.error(f"Unknown flow: {self.flow}")
            sys.exit(1)
    
    def shutdown(self):
        """Cleanup and shutdown"""
        logger.info("Shutting down application...")
        
        try:
            if self.mysql_client:
                self.mysql_client.close()
            
            if self.redis_client:
                self.redis_client.close()
                
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        
        logger.info("Application shutdown complete")


def main():
    """Main entry point"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="File Processor Application")
    parser.add_argument(
        '--flow',
        type=str,
        choices=['import', 'process'],
        default='import',
        help='Processing flow: import (Flow 1) or process (Flow 2)'
    )
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info(f"File Processor Application Starting (Flow: {args.flow})")
    logger.info("=" * 60)
    
    app = FileProcessorApp(flow=args.flow)
    
    # Initialize application
    if not app.initialize():
        logger.error("Failed to initialize application, exiting...")
        sys.exit(1)
    
    try:
        # Run main loop
        app.run()
    finally:
        # Ensure cleanup happens
        app.shutdown()
    
    logger.info("Application exited")


if __name__ == "__main__":
    main()
