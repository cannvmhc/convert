# Local Development Setup Guide

## Quick Start

### 1. Setup Virtual Environment

```bash
# Run setup script (tự động tạo venv và install dependencies)
./setup.sh
```

Hoặc manual:

```bash
# Tạo virtual environment
python3 -m venv venv

# Activate venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit với thông tin database của bạn
nano .env
```

**Lưu ý**: Cần có MySQL và Redis đang chạy trên localhost!

### 3. Setup Database

```bash
# Connect to MySQL
mysql -u root -p

# Tạo database và import schema
CREATE DATABASE fileprocessor;
USE fileprocessor;
source init.sql;
```

### 4. Run Application

**Option 1: Dùng scripts**

```bash
# Terminal 1: Run Flow 1 (Import)
./run_import.sh

# Terminal 2: Run Flow 2 (Process)
./run_process.sh
```

**Option 2: Manual**

```bash
# Activate venv
source venv/bin/activate

# Run Flow 1
python src/main.py --flow import

# Run Flow 2 (terminal khác)
python src/main.py --flow process
```

## Testing với Sample Data

### Tạo sample Excel file

```bash
# Tạo file test trong data/
# File cần có ít nhất 1 sheet với headers ở row đầu tiên
```

### Insert vào database

```sql
INSERT INTO files (file_path, type, status) VALUES
('/home/cannv/convert/data/test.xlsx', 'type1', 0);
```

### Xem kết quả

```sql
-- Check files đã import
SELECT * FROM files;

-- Check rows đã import
SELECT * FROM excel_data LIMIT 10;

-- Check processed rows
SELECT COUNT(*) FROM excel_data WHERE status = 1;
```

## Troubleshooting

### MySQL Connection Error

```bash
# Check MySQL đang chạy
sudo systemctl status mysql

# Start MySQL
sudo systemctl start mysql
```

### Redis Connection Error

```bash
# Check Redis đang chạy
redis-cli ping

# Start Redis
sudo systemctl start redis
```

### Import Error

```bash
# Check logs
tail -f logs/app_*.log

# Check file path trong database
SELECT id, file_path, status FROM files;
```

## Deactivate Virtual Environment

```bash
deactivate
```
