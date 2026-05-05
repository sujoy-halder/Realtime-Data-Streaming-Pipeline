#!/bin/bash
set -e

echo "🚀 Starting Airflow setup..."

# ------------------ Install Dependencies ------------------

if [ -f "/opt/airflow/requirements.txt" ]; then
  echo "📦 Installing Python dependencies..."
  pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

# ------------------ Wait for Postgres ------------------

echo "⏳ Waiting for Postgres..."
sleep 10

# ------------------ Initialize DB ------------------

echo "🗄️ Initializing Airflow DB..."
airflow db migrate

# ------------------ Create Admin User (Safe) ------------------

echo "👤 Creating admin user (if not exists)..."

airflow users list | grep -q "admin" || airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin

# ------------------ Start Webserver ------------------

echo "🌐 Starting Airflow Webserver..."
exec airflow webserver