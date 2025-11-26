import os
import shutil
import requests
from pathlib import Path
from urllib.parse import urlparse
from loguru import logger


class FileDownloader:
    """Utility class for downloading files from URL or copying from local path"""
    
    def __init__(self, temp_dir: str = "/tmp/file_processor"):
        """
        Initialize file downloader
        
        Args:
            temp_dir: Directory to store temporary downloaded files
        """
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"FileDownloader initialized with temp_dir: {self.temp_dir}")
    
    def _is_url(self, path: str) -> bool:
        """
        Check if path is a URL
        
        Args:
            path: Path or URL string
            
        Returns:
            True if URL, False if local path
        """
        parsed = urlparse(path)
        return parsed.scheme in ('http', 'https', 'ftp')
    
    def _get_filename_from_path(self, path: str) -> str:
        """
        Extract filename from path or URL
        
        Args:
            path: Path or URL string
            
        Returns:
            Filename
        """
        if self._is_url(path):
            # Extract from URL
            parsed = urlparse(path)
            filename = os.path.basename(parsed.path)
        else:
            # Extract from local path
            filename = os.path.basename(path)
        
        # If no filename found, generate one
        if not filename:
            filename = f"downloaded_file_{os.getpid()}.xlsx"
        
        return filename
    
    def download_from_url(self, url: str, filename: str = None) -> str:
        """
        Download file from HTTP/HTTPS URL
        
        Args:
            url: URL to download from
            filename: Optional custom filename
            
        Returns:
            Path to downloaded file
        """
        try:
            if not filename:
                filename = self._get_filename_from_path(url)
            
            temp_path = self.temp_dir / filename
            
            logger.info(f"Downloading file from URL: {url}")
            
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"File downloaded successfully to: {temp_path}")
            return str(temp_path)
            
        except Exception as e:
            logger.error(f"Error downloading file from URL {url}: {e}")
            raise
    
    def copy_from_local(self, source_path: str, filename: str = None) -> str:
        """
        Copy file from local path to temp directory
        
        Args:
            source_path: Source file path
            filename: Optional custom filename
            
        Returns:
            Path to copied file
        """
        try:
            if not os.path.exists(source_path):
                raise FileNotFoundError(f"Source file not found: {source_path}")
            
            if not filename:
                filename = self._get_filename_from_path(source_path)
            
            temp_path = self.temp_dir / filename
            
            logger.info(f"Copying file from: {source_path} to: {temp_path}")
            shutil.copy2(source_path, temp_path)
            
            logger.info(f"File copied successfully to: {temp_path}")
            return str(temp_path)
            
        except Exception as e:
            logger.error(f"Error copying file from {source_path}: {e}")
            raise
    
    def download_file(self, path: str, filename: str = None) -> str:
        """
        Download or copy file based on path type (URL or local)
        
        Args:
            path: URL or local file path
            filename: Optional custom filename
            
        Returns:
            Path to downloaded/copied file in temp directory
        """
        path = f"http://127.0.0.1:8000/storage/{path}"
        if self._is_url(path):
            return self.download_from_url(path, filename)
        else:
            return self.copy_from_local(path, filename)
    
    def cleanup_file(self, file_path: str):
        """
        Delete temporary file
        
        Args:
            file_path: Path to file to delete
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up temporary file: {file_path}")
        except Exception as e:
            logger.warning(f"Error cleaning up file {file_path}: {e}")
    
    def cleanup_all(self):
        """Delete all files in temp directory"""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.temp_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Cleaned up all temporary files in: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Error cleaning up temp directory: {e}")
