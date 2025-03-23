# Cricket API

Real-time cricket score scraper aur API jo automatically Cricbuzz se live matches detect aur track karta hai.


## Features

- **Real-time Score Updates**: Cricket scores ko adaptive frequencies ke saath continuously scrape karta hai
- **Smart Match Classification**: Alag-alag match formats (IPL, Test, ODI) aur statuses (Live, Upcoming, Completed) ko detect karta hai
- **Enhanced Status Tracking**: Match status transitions ko track karta hai aur changes ko database mein store karta hai
- **IPL Format Detection**: Special patterns ki madad se IPL matches ko accurately identify karta hai
- **Advanced Error Handling**: Automatic retries, connection recovery aur backoff strategy
- **Performance Optimized**: Database indexes, connection pooling aur memory management
- **Monitoring Ready**: Prometheus metrics endpoint aur health checks
- **Secure Logging**: Passwords aur sensitive data ko logs mein censor karta hai
- **Flask API Endpoints**: Ready-to-use API endpoints jo match data provide karte hain
- **Render URL Status Check**: Auto-detect feature jo Render deployment ki status check karta hai


## Setup

### Requirements

- Python 3.8+
- PostgreSQL database

### Environment Variables

Ek `.env` file create karein aur in variables ko set karein:

```
# Database Configuration
DB_HOST=localhost
DB_USER=yourdbuser
DB_PASS=yourdbpassword
DB_NAME=cricket_db

# Port Configuration
FLASK_PORT=5000          # Flask API server port
PROMETHEUS_PORT=8000     # Prometheus metrics server port
HEALTH_PORT=8100         # Health check endpoint port
CHECK_PORT=true          # Check if ports are already in use

# Render Configuration
RENDER_URL=https://your-render-deployment-url.com
```

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/cricket-api.git
cd cricket-api

# Install dependencies
pip install -r requirements.txt

# Run the application
python main.py
```

## API Endpoints

### Render Status Endpoints
- **URL**: `/`
- **Method**: GET
- **Description**: Render deployment URL ki live status check karta hai (maximum 10 attempts tak)

- **URL**: `/check-render`
- **Method**: GET 
- **Description**: Render URL ki immediate status check karta hai (no waiting)
- **Response**: JSON format mein status information return karta hai

### Matches Endpoint
- **URL**: `/api/matches`
- **Method**: GET
- **Parameters**: 
  - `type` (optional): Filter matches by type (Live, Upcoming, Completed, Test)
- **Description**: Database se match data retrieve karta hai

Example:
```
/api/matches?type=Live
```

## Ports

Application teen alag-alag ports use karta hai:

1. **Flask API Server (Default: 5000)**
   - Cricket match data aur render status ke liye REST API endpoints
   - `.env` file mein `FLASK_PORT` se configure karein

2. **Prometheus Metrics Server (Default: 8000)**
   - Application metrics (requests, errors, etc.) ke liye Prometheus endpoint
   - `.env` file mein `PROMETHEUS_PORT` se configure karein

3. **Health Check Endpoint (Default: 8100)**
   - Simple health check endpoint (for monitoring systems)
   - `.env` file mein `HEALTH_PORT` se configure karein
   - `0` value set karne par automatically `PROMETHEUS_PORT + 100` use karega

All ports are auto-configured - agar koi port already in use hai, toh system automatically dusra available port use karega.

## Monitoring

- Prometheus metrics: `http://localhost:8000`
- Health check endpoint: `http://localhost:8100/health`

