# Deployment Guide for GCP Cloud Run

This guide covers deploying the FX Exchange application to Google Cloud Platform Cloud Run with cross-project Cloud SQL access.

## Prerequisites

1. GCP projects set up:
   - **Application Project**: Where Cloud Run service will be deployed
   - **Database Project**: Where Cloud SQL instance is hosted

2. Required GCP services enabled:
   - Cloud Run API
   - Cloud SQL Admin API
   - Cloud Build API (if using Cloud Build)
   - Secret Manager API (for storing database password)

3. Docker installed locally (for local testing)

## Setup Steps

### 1. Create Secret in Secret Manager

Store the database password securely:

```bash
# In the application project
echo -n "your-db-password" | gcloud secrets create db-password \
    --data-file=- \
    --replication-policy="automatic"
```

### 2. Grant Permissions

#### Grant Cloud SQL Client Role (Cross-Project Access)

The Cloud Run service account needs permission to connect to Cloud SQL in a different project:

```bash
# Get the Cloud Run service account email
SERVICE_ACCOUNT=$(gcloud iam service-accounts list \
    --filter="displayName:Compute Engine default service account" \
    --format="value(email)")

# Grant Cloud SQL Client role in the DATABASE project
gcloud projects add-iam-policy-binding DATABASE_PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/cloudsql.client"
```

#### Grant Secret Accessor Role

```bash
# Grant secret accessor role in the APPLICATION project
gcloud projects add-iam-policy-binding APPLICATION_PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor"
```

### 3. Configure Cloud SQL Instance

#### For Public IP:
- Ensure authorized networks are configured (or use Cloud SQL Proxy which handles this)
- The Cloud SQL Python Connector handles authentication automatically

#### For Private IP:
- Set up VPC peering between projects
- Or use Shared VPC
- Ensure Cloud Run service is in the same VPC network

### 4. Deploy Using Cloud Build

#### Option A: Using cloudbuild.yaml

```bash
# Set substitution variables
gcloud builds submit --config=cloudbuild.yaml \
    --substitutions=_CLOUD_SQL_INSTANCE="PROJECT_ID:REGION:INSTANCE_NAME",\
_DB_NAME="postgres",\
_DB_USER="exchange_rates_db",\
_IP_TYPE="PUBLIC",\
_DB_PASSWORD_SECRET="db-password"
```

#### Option B: Manual Deployment

```bash
# Build and push image
gcloud builds submit --tag gcr.io/PROJECT_ID/fx-exchange

# Deploy to Cloud Run
gcloud run deploy fx-exchange \
    --image gcr.io/PROJECT_ID/fx-exchange \
    --platform managed \
    --region europe-west1 \
    --allow-unauthenticated \
    --add-cloudsql-instances PROJECT_ID:REGION:INSTANCE_NAME \
    --set-env-vars INSTANCE_CONNECTION_NAME=PROJECT_ID:REGION:INSTANCE_NAME,\
DB_NAME=postgres,\
DB_USER=exchange_rates_db,\
IP_TYPE=PUBLIC \
    --set-secrets DB_PASSWORD=db-password:latest
```

### 5. Local Testing with Docker

```bash
# Build image
docker build -t fx-exchange .

# Run container (set environment variables)
docker run -p 8080:8080 \
    -e INSTANCE_CONNECTION_NAME="PROJECT_ID:REGION:INSTANCE_NAME" \
    -e DB_NAME="postgres" \
    -e DB_USER="exchange_rates_db" \
    -e DB_PASSWORD="your-password" \
    -e IP_TYPE="PUBLIC" \
    fx-exchange
```

## Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `INSTANCE_CONNECTION_NAME` | Cloud SQL instance connection name (format: `PROJECT:REGION:INSTANCE`) | Yes | `openbookings:europe-west1:openbookings-db` |
| `DB_NAME` | Database name | Yes | `postgres` |
| `DB_USER` | Database user | Yes | `exchange_rates_db` |
| `DB_PASSWORD` | Database password (use Secret Manager in production) | Yes | None |
| `IP_TYPE` | Connection type: `PUBLIC` or `PRIVATE` | No | `PUBLIC` |
| `PORT` | Application port | No | `8080` |
| `FLASK_DEBUG` | Enable Flask debug mode | No | `false` |

## API Endpoints

- `GET /health` - Health check (includes database connectivity test)
- `GET /test-db` - Test database connection and return sample data
- `GET /update` - Fetch and update exchange rates from ECB
- `GET /convert?amount=100&from=USD&to=EUR` - Convert currency

## Troubleshooting

### Connection Issues

1. **Authentication Failed**:
   - Verify service account has `roles/cloudsql.client` in database project
   - Check database credentials are correct
   - Verify secret is accessible

2. **Connection Timeout**:
   - For Private IP: Check VPC peering/Shared VPC configuration
   - For Public IP: Check authorized networks (if not using Cloud SQL Proxy)
   - Verify Cloud SQL instance is running

3. **Permission Denied**:
   - Check IAM roles on both projects
   - Verify Cloud Run service account has necessary permissions

### Logs

View Cloud Run logs:
```bash
gcloud run services logs read fx-exchange --region europe-west1
```

## Security Best Practices

1. ✅ Never commit passwords to code (now enforced)
2. ✅ Use Secret Manager for sensitive data
3. ✅ Enable Cloud SQL Auth Proxy (handled by Python connector)
4. ✅ Use least privilege IAM roles
5. ✅ Enable Cloud Run authentication if needed (currently public)
6. ✅ Monitor logs for suspicious activity

## Cost Optimization

- Cloud Run scales to zero when not in use
- Connection pooling is configured (pool_size=2)
- Consider using Private IP to avoid egress charges

