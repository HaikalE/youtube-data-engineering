#!/bin/bash

# Script to set up dashboard environment without dependency conflicts

# Color definitions
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}             YouTube Trending Dashboard Setup Script              ${NC}"
echo -e "${BLUE}==================================================================${NC}"

# Check for Python
echo -e "\n${YELLOW}Checking Python installation...${NC}"
if command -v python3 &>/dev/null; then
    echo -e "${GREEN}✓ Python 3 is installed${NC}"
    python_version=$(python3 --version)
    echo -e "  ${python_version}"
else
    echo -e "${RED}✗ Python 3 is not installed${NC}"
    echo -e "Please install Python 3.8 or higher"
    exit 1
fi

# Create a virtual environment for dashboard
echo -e "\n${YELLOW}Setting up virtual environment for dashboard...${NC}"
if [ -d "dashboard_env" ]; then
    echo -e "${YELLOW}⚠ Virtual environment already exists.${NC}"
    read -p "Do you want to recreate it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf dashboard_env
        python3 -m venv dashboard_env
        echo -e "${GREEN}✓ Created new virtual environment${NC}"
    fi
else
    python3 -m venv dashboard_env
    echo -e "${GREEN}✓ Created virtual environment${NC}"
fi

# Activate the virtual environment
echo -e "\n${YELLOW}Activating virtual environment...${NC}"
source dashboard_env/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Install dependencies
echo -e "\n${YELLOW}Installing dashboard dependencies...${NC}"
pip install -r dashboard/requirements.txt
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to install dependencies${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Dependencies installed successfully${NC}"
fi

# Create necessary directories
echo -e "\n${YELLOW}Setting up dashboard directories...${NC}"
mkdir -p dashboard/cache

# Initialize database
echo -e "\n${YELLOW}Initializing database...${NC}"
python3 -c "from utils.db_utils import DatabaseHandler; import yaml; config = yaml.safe_load(open('config/config.yaml')); db = DatabaseHandler(config); db.create_tables()"
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to initialize database${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Database initialized successfully${NC}"
fi

# Insert sample data if needed
echo -e "\n${YELLOW}Would you like to insert sample data for testing? (y/n)${NC}"
read -p "" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}Inserting sample data...${NC}"
    python3 insert_sample_data.py
    echo -e "${GREEN}✓ Sample data inserted${NC}"
fi

# Start dashboard
echo -e "\n${YELLOW}Starting dashboard...${NC}"
echo -e "${BLUE}Running dashboard in current terminal...${NC}"
echo -e "${GREEN}✓ Dashboard starting...${NC}"
echo -e "  Access Dashboard at: http://localhost:8050"
echo -e "${YELLOW}Press Ctrl+C to stop the dashboard${NC}"
python3 -m dashboard.app

# Deactivate virtual environment when done
deactivate