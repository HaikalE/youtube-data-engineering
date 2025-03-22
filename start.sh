#!/bin/bash

# YouTube Trending Analysis - Startup Script
# This script initializes and starts all components of the project

# Color definitions
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}             YouTube Trending Analysis Startup Script             ${NC}"
echo -e "${BLUE}==================================================================${NC}"

# Check for required environment variables
echo -e "\n${YELLOW}Checking environment variables...${NC}"

missing_vars=0
if [ -z "$YOUTUBE_API_KEY" ]; then
    echo -e "${RED}✗ YOUTUBE_API_KEY is not set${NC}"
    missing_vars=1
else
    echo -e "${GREEN}✓ YOUTUBE_API_KEY is set${NC}"
fi

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo -e "${RED}✗ AWS_ACCESS_KEY_ID is not set${NC}"
    missing_vars=1
else
    echo -e "${GREEN}✓ AWS_ACCESS_KEY_ID is set${NC}"
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo -e "${RED}✗ AWS_SECRET_ACCESS_KEY is not set${NC}"
    missing_vars=1
else
    echo -e "${GREEN}✓ AWS_SECRET_ACCESS_KEY is set${NC}"
fi

if [ -z "$DB_PASSWORD" ]; then
    echo -e "${YELLOW}⚠ DB_PASSWORD is not set. Using default if configured.${NC}"
else
    echo -e "${GREEN}✓ DB_PASSWORD is set${NC}"
fi

if [ $missing_vars -eq 1 ]; then
    echo -e "\n${RED}Error: Some required environment variables are missing.${NC}"
    echo -e "Please set them with the following commands:"
    echo -e "  export YOUTUBE_API_KEY=\"your_youtube_api_key\""
    echo -e "  export AWS_ACCESS_KEY_ID=\"your_aws_access_key\""
    echo -e "  export AWS_SECRET_ACCESS_KEY=\"your_aws_secret_key\""
    echo -e "  export DB_PASSWORD=\"your_database_password\""
    exit 1
fi

# Check for Python and required packages
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

# Check for dependencies
echo -e "\n${YELLOW}Checking for dependencies...${NC}"
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}✗ requirements.txt not found${NC}"
    exit 1
fi

echo -e "${YELLOW}Installing dependencies if needed...${NC}"
pip install -r requirements.txt
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to install dependencies${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Dependencies are installed${NC}"
fi

# Create necessary directories
echo -e "\n${YELLOW}Setting up project directories...${NC}"
mkdir -p logs
mkdir -p data
mkdir -p visualizations

# Initialize database
echo -e "\n${YELLOW}Initializing database...${NC}"
python3 -c "from utils.db_utils import DatabaseHandler; import yaml; config = yaml.safe_load(open('config/config.yaml')); db = DatabaseHandler(config); db.create_tables()"
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to initialize database${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Database initialized successfully${NC}"
fi

# Start components based on arguments
if [ "$1" == "all" ] || [ "$1" == "" ]; then
    # Start everything
    
    # Start Airflow (if installed)
    echo -e "\n${YELLOW}Starting Airflow...${NC}"
    if command -v airflow &>/dev/null; then
        # Check if Airflow is already running
        if pgrep -f "airflow scheduler" > /dev/null || pgrep -f "airflow webserver" > /dev/null; then
            echo -e "${YELLOW}⚠ Airflow already running${NC}"
        else
            echo -e "${BLUE}Starting Airflow scheduler and webserver...${NC}"
            airflow scheduler -D
            airflow webserver -D -p 8080
            echo -e "${GREEN}✓ Airflow started${NC}"
            echo -e "  Access Airflow UI at: http://localhost:8080"
        fi
    else
        echo -e "${YELLOW}⚠ Airflow is not installed. Skipping.${NC}"
        echo -e "  To install Airflow: pip install apache-airflow"
    fi
    
    # Start the dashboard
    echo -e "\n${YELLOW}Starting Dashboard...${NC}"
    nohup python3 -m dashboard.app > logs/dashboard.log 2>&1 &
    DASHBOARD_PID=$!
    echo -e "${GREEN}✓ Dashboard started with PID: $DASHBOARD_PID${NC}"
    echo -e "  Access Dashboard at: http://localhost:8050"
    
    echo -e "\n${GREEN}==================================================================${NC}"
    echo -e "${GREEN}             All components started successfully!                ${NC}"
    echo -e "${GREEN}==================================================================${NC}"
    echo -e "Dashboard: http://localhost:8050"
    echo -e "Airflow UI: http://localhost:8080"
    echo -e "\nTo stop all components: ./start.sh stop"
    
elif [ "$1" == "airflow" ]; then
    # Start only Airflow
    echo -e "\n${YELLOW}Starting Airflow...${NC}"
    if command -v airflow &>/dev/null; then
        airflow scheduler -D
        airflow webserver -D -p 8080
        echo -e "${GREEN}✓ Airflow started${NC}"
        echo -e "  Access Airflow UI at: http://localhost:8080"
    else
        echo -e "${RED}✗ Airflow is not installed${NC}"
        echo -e "  To install Airflow: pip install apache-airflow"
        exit 1
    fi
    
elif [ "$1" == "dashboard" ]; then
    # Start only the dashboard
    echo -e "\n${YELLOW}Starting Dashboard...${NC}"
    nohup python3 -m dashboard.app > logs/dashboard.log 2>&1 &
    DASHBOARD_PID=$!
    echo -e "${GREEN}✓ Dashboard started with PID: $DASHBOARD_PID${NC}"
    echo -e "  Access Dashboard at: http://localhost:8050"
    
elif [ "$1" == "pipeline" ]; then
    # Run the pipeline once without Airflow
    echo -e "\n${YELLOW}Running the pipeline manually...${NC}"
    echo -e "${BLUE}1. Extracting data...${NC}"
    python3 -m scripts.extract config/config.yaml
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Extraction failed${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}2. Transforming data...${NC}"
    python3 -m scripts.transform config/config.yaml
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Transformation failed${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}3. Loading data...${NC}"
    python3 -m scripts.load config/config.yaml
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Loading failed${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}4. Analyzing data...${NC}"
    python3 -m scripts.analyze config/config.yaml
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Analysis failed${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Pipeline execution completed successfully${NC}"
    
elif [ "$1" == "stop" ]; then
    # Stop all running components
    echo -e "\n${YELLOW}Stopping all components...${NC}"
    
    # Stop Airflow
    if command -v airflow &>/dev/null; then
        echo -e "${BLUE}Stopping Airflow...${NC}"
        pkill -f "airflow scheduler" || true
        pkill -f "airflow webserver" || true
        echo -e "${GREEN}✓ Airflow stopped${NC}"
    fi
    
    # Stop Dashboard
    echo -e "${BLUE}Stopping Dashboard...${NC}"
    pkill -f "python3 -m dashboard.app" || true
    echo -e "${GREEN}✓ Dashboard stopped${NC}"
    
    echo -e "\n${GREEN}All components stopped successfully!${NC}"
    
else
    echo -e "${RED}Invalid argument: $1${NC}"
    echo -e "Usage: ./start.sh [option]"
    echo -e "Options:"
    echo -e "  all (default) - Start all components"
    echo -e "  airflow       - Start only Airflow"
    echo -e "  dashboard     - Start only the dashboard"
    echo -e "  pipeline      - Run the pipeline once without Airflow"
    echo -e "  stop          - Stop all running components"
    exit 1
fi