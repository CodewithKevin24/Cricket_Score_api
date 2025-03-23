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
- **Single Port Operation**: Sabhi services (API, metrics, health) ek hi port pe run hoti hain
- **Auto-Ping System**: Random intervals (1-2.5 minutes) par Render URL ko automatically ping karta hai, real user traffic simulate karke service ko always active rakhne ke liye


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
PORT=5000               # Single port for all services (API, metrics, health)

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

### Deployment on Render

Render par deploy karte samay in baaton ka dhyan rakhein:

1. **Flask Async Support**: Flask async routes ke liye `flask[async]` package zaruri hai
   - Render dashboard par environment variables mein `PYTHON_VERSION=3.9` ya usse latest set karein
   - `requirements.txt` mein exact versions specify karna important hai

2. **Deploy Settings**:
   - Build Command: `pip install -U pip && pip install -r requirements.txt`
   - Start Command: `python main.py`

3. **Environment Variables**:
   - `.env` file mein `RENDER_URL` ko actual deployment URL se replace karein
   - Database credentials ko secure environment variables ke through set karein

4. **Troubleshooting**:
   - Agar async routes par error aaye toh Render dashboard se manual redeploy karein
   - Logs mein check karein ki "Flask async support is available" message aaya hai

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

### Monitoring Endpoints

- **Health Check URL**: `/health`
  - **Method**: GET
  - **Description**: Simple health check jo server status verify karta hai
  - **Response**: `{"status": "OK", "timestamp": "..."}`

- **Metrics URL**: `/metrics`
  - **Method**: GET
  - **Description**: Prometheus metrics format mein application metrics provide karta hai
  - **Response**: Standard Prometheus metrics format

## Server

Application sirf ek hi port (default: 5000) pe sabhi services run karta hai:

1. **API Endpoints**: `/`, `/check-render`, `/api/matches`
2. **Monitoring Endpoints**: `/health`, `/metrics`

Port `.env` file mein `PORT` variable se configure kar sakte hain.

