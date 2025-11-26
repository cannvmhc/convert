#!/bin/bash

# Setup script for local development

echo "Setting up File Processor Tool for local development..."

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create .env if not exists
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ""
    echo "⚠️  IMPORTANT: Please edit .env file with your MySQL and Redis connection details!"
    echo ""
fi

# Create necessary directories
echo "Creating directories..."
mkdir -p logs data

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your database credentials"
echo "2. Make sure MySQL and Redis are running"
echo "3. Run: source venv/bin/activate"
echo "4. Run Flow 1: python src/main.py --flow import"
echo "5. Run Flow 2: python src/main.py --flow process"
echo ""
