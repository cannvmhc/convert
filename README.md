# File Processor Tool

Tool Python để xử lý Excel files với 2 luồng: Import Excel vào database và xử lý data từ database.

## Tính năng

- ✅ **Flow 1 - Excel Import**: Download Excel → Parse tất cả sheets → Import vào database
- ✅ **Flow 2 - Data Processing**: Lấy data từ database → Xử lý → Update lại database
- ✅ **MySQL Client (Singleton)**: Kết nối MySQL với connection pooling
- ✅ **Redis Client (Singleton)**: Check duplicate data
- ✅ **Factory Pattern**: Xử lý các loại file khác nhau dựa trên type
- ✅ **Docker Support**: Deploy dễ dàng với Docker Compose
- ✅ **Logging**: Chi tiết với Loguru

## Kiến trúc

### Flow 1: Import Excel to Database
```
File (status=0) → Download → Parse Excel → Insert to excel_data table → Update file status
```

### Flow 2: Process Data from Database
```
excel_data (status=0) → Check duplicates → Process by type → Update excel_data status
```

## Cấu trúc Database

### Bảng `files` (Upload tracking)
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| file_path | VARCHAR(500) | URL hoặc đường dẫn file Excel |
| type | VARCHAR(50) | Loại file (type1, type2, default) |
| status | INT | 0=pending import, 1=imported, 2=error |
| total_rows | INT | Tổng số rows đã import |

### Bảng `excel_data` (Imported rows)
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| upload_id | INT | Foreign key to files.id |
| sheet_name | VARCHAR(100) | Tên sheet trong Excel |
| row_index | INT | Số thứ tự row (1-based) |
| row_data | JSON | Data của row: `{"header1": "value1", ...}` |
| status | INT | 0=pending, 1=processed, 2=error |

## Cài đặt

### 1. Setup Environment

```bash
cd /home/cannv/convert

# Copy environment template
cp .env.example .env

# Edit .env với thông tin của bạn
nano .env
```

### 2. Chạy với Docker (Recommended)

```bash
# Build và start tất cả services
docker-compose up -d

# Xem logs của Flow 1 (Import)
docker-compose logs -f app-import

# Xem logs của Flow 2 (Process)
docker-compose logs -f app-process

# Stop services
docker-compose down
```

### 3. Chạy Local (Development)

```bash
# Install dependencies
pip install -r requirements.txt

# Chạy Flow 1: Import Excel files
python src/main.py --flow import

# Chạy Flow 2: Process data (trong terminal khác)
python src/main.py --flow process
```

## Configuration

File `.env`:

```env
# MySQL Configuration
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=rootpassword
MYSQL_DATABASE=fileprocessor

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# Application Settings
LOG_LEVEL=INFO
BATCH_SIZE=10
DUPLICATE_TTL=86400
```

## Workflow Chi tiết

### Flow 1: Import Excel

1. Lấy file có `status = 0` từ bảng `files`
2. Download file từ `file_path` (hỗ trợ URL hoặc local path)
3. Parse tất cả sheets trong Excel:
   - Đọc headers từ row đầu tiên
   - Convert mỗi row thành JSON: `{"header1": "value1", "header2": "value2", ...}`
4. Insert tất cả rows vào bảng `excel_data`:
   - `upload_id`: ID của file
   - `sheet_name`: Tên sheet
   - `row_index`: Số thứ tự row
   - `row_data`: JSON data
   - `status`: 0 (pending)
5. Update `files.status = 1` và `total_rows`

### Flow 2: Process Data

1. Lấy rows có `status = 0` từ bảng `excel_data`
2. Với mỗi row:
   - Parse JSON `row_data`
   - Check duplicate với Redis
   - Nếu duplicate: Update `status = 2`
   - Nếu không duplicate:
     - Xử lý data theo logic của processor (based on file type)
     - Update `row_data` nếu cần
     - Update `status = 1`

## Thêm Processor Mới

Edit `src/processors/data_processor.py`:

```python
class Type3DataProcessor(DataProcessor):
    def process_row(self, row_data: Dict) -> Dict:
        # Custom logic cho Type 3
        processed_data = row_data.copy()
        
        # Ví dụ: Transform data
        processed_data['processed_field'] = some_transform(row_data['field'])
        
        return processed_data
```

Đăng ký trong `src/processors/__init__.py`:

```python
from src.processors.data_processor import Type3DataProcessor

def register_processors():
    ProcessorFactory.register_data_processor("type1", Type1DataProcessor)
    ProcessorFactory.register_data_processor("type2", Type2DataProcessor)
    ProcessorFactory.register_data_processor("type3", Type3DataProcessor)  # Thêm dòng này
    ProcessorFactory.register_data_processor("default", DefaultDataProcessor)
```

## Docker Services

Docker Compose chạy 4 services:

- **app-import**: Flow 1 - Import Excel files
- **app-process**: Flow 2 - Process data
- **mysql**: MySQL database
- **redis**: Redis cache

## Logs

Logs được lưu trong `logs/`:
- Format: `app_YYYY-MM-DD_HH-MM-SS.log`
- Rotation: Mỗi ngày
- Retention: 7 ngày

## Troubleshooting

### Kiểm tra services
```bash
docker-compose ps
docker-compose logs -f
```

### Restart services
```bash
docker-compose restart app-import
docker-compose restart app-process
```

### Xem database
```bash
# Connect to MySQL
docker exec -it file-processor-mysql mysql -u root -p

# Check tables
USE fileprocessor;
SELECT * FROM files;
SELECT * FROM excel_data LIMIT 10;
```

### Xem Redis
```bash
# Connect to Redis
docker exec -it file-processor-redis redis-cli

# Check keys
KEYS *
```

## License

MIT
