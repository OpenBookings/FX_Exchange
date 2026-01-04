# FX Exchange Rate Service

A Flask-based microservice for fetching and converting foreign exchange rates using data from the European Central Bank (ECB).

## Features

- Fetch latest exchange rates from ECB API
- Store exchange rates in Cloud SQL (PostgreSQL)
- Convert amounts between currencies
- Health check endpoint with database connectivity verification
- Production-ready logging and error handling

## Architecture

- **Framework**: Flask with Gunicorn
- **Database**: Google Cloud SQL (PostgreSQL)
- **Deployment**: Google Cloud Run
- **Authentication**: Cloud SQL Python Connector (supports cross-project access)

## Project Structure

```
FX_Exchange/
├── main.py              # Flask application and API endpoints
├── db.py                # Database connection management
├── update.py            # ECB data fetching and database operations
├── conversion.py        # Currency conversion logic
├── requirements.txt     # Python dependencies
├── Dockerfile           # Container image definition
├── .dockerignore        # Docker build exclusions
├── cloudbuild.yaml      # GCP Cloud Build configuration
└── DEPLOYMENT.md        # Detailed deployment guide
```

## Quick Start

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export INSTANCE_CONNECTION_NAME="project:region:instance"
export DB_NAME="postgres"
export DB_USER="exchange_rates_db"
export DB_PASSWORD="your-password"
export IP_TYPE="PUBLIC"
```

3. Run the application:
```bash
python main.py
```

### Docker

```bash
docker build -t fx-exchange .
docker run -p 8080:8080 \
    -e INSTANCE_CONNECTION_NAME="project:region:instance" \
    -e DB_NAME="postgres" \
    -e DB_USER="exchange_rates_db" \
    -e DB_PASSWORD="your-password" \
    -e IP_TYPE="PUBLIC" \
    fx-exchange
```

## API Endpoints

### Health Check
```bash
GET /health
```
Returns service health status and database connectivity.

### Test Database
```bash
GET /test-db
```
Tests database connection and returns sample data.

### Update Exchange Rates
```bash
GET /update
```
Fetches latest exchange rates from ECB and updates the database.

### Convert Currency
```bash
GET /convert?amount=100&from=USD&to=EUR
```
Converts an amount from one currency to another.

**Parameters:**
- `amount` (required): Amount to convert
- `from` (required): Source currency code (e.g., USD)
- `to` (required): Target currency code (e.g., EUR)

**Example Response:**
```json
{
  "amount": 92.5,
  "from": "USD",
  "to": "EUR",
  "original_amount": 100
}
```

## Production Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions to GCP Cloud Run.

### Key Production Features

- ✅ No hardcoded credentials (password required via environment/Secret Manager)
- ✅ Comprehensive logging across all modules
- ✅ Error handling and input validation
- ✅ Connection pooling with health checks
- ✅ Cross-project Cloud SQL support
- ✅ Production-ready container configuration

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `INSTANCE_CONNECTION_NAME` | Cloud SQL connection string | Yes |
| `DB_NAME` | Database name | Yes |
| `DB_USER` | Database user | Yes |
| `DB_PASSWORD` | Database password | Yes |
| `IP_TYPE` | `PUBLIC` or `PRIVATE` | No (default: `PUBLIC`) |
| `PORT` | Application port | No (default: `8080`) |
| `FLASK_DEBUG` | Enable debug mode | No (default: `false`) |

## Logging

The application uses Python's standard `logging` module with structured logging:
- **INFO**: General application flow and successful operations
- **WARNING**: Non-critical issues (missing data, invalid input)
- **ERROR**: Failures and exceptions
- **DEBUG**: Detailed diagnostic information

Logs are formatted with timestamps and module names for easy debugging in Cloud Run.

## Security

- Database password must be provided via environment variable or Secret Manager
- No hardcoded credentials in code
- Cloud SQL authentication handled securely via Python connector
- Input validation on all API endpoints
- Error messages don't expose sensitive information

## License

[Add your license here]

