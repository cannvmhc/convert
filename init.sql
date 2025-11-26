-- ============================================
-- Table: files (Upload tracking)
-- ============================================
CREATE TABLE IF NOT EXISTS files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path VARCHAR(500) NOT NULL COMMENT 'URL or local path to Excel file',
    type VARCHAR(50) NOT NULL DEFAULT 'default' COMMENT 'Type for processing logic',
    status INT NOT NULL DEFAULT 0 COMMENT '0=pending import, 1=imported, 2=error',
    total_rows INT DEFAULT 0 COMMENT 'Total rows imported from Excel',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_type (type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: excel_data (Imported Excel rows)
-- ============================================
CREATE TABLE IF NOT EXISTS excel_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    upload_id INT NOT NULL COMMENT 'Foreign key to files.id',
    sheet_name VARCHAR(100) NOT NULL COMMENT 'Sheet name from Excel file',
    row_index INT NOT NULL COMMENT 'Row number (1-based, excluding header)',
    row_data JSON NOT NULL COMMENT 'JSON object: {"header1": "value1", "header2": "value2", ...}',
    status INT NOT NULL DEFAULT 0 COMMENT '0=pending processing, 1=processed, 2=error',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (upload_id) REFERENCES files(id) ON DELETE CASCADE,
    INDEX idx_upload_status (upload_id, status),
    INDEX idx_sheet (sheet_name),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Sample data for testing
-- ============================================
INSERT INTO files (file_path, type, status) VALUES
    ('/app/data/sample1.xlsx', 'type1', 0),
    ('/app/data/sample2.xlsx', 'type2', 0),
    ('/app/data/sample3.xlsx', 'default', 0);
