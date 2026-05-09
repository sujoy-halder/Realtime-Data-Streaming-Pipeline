#!/bin/bash
set -e

echo "🚀 Starting Airflow setup..."

# ------------------ Install Dependencies ------------------

if [ -f "/opt/airflow/requirements.txt" ]; then
  echo "📦 Installing Python dependencies..."
  pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

# ------------------ Wait For Postgres ------------------

echo "⏳ Waiting for Postgres connection..."

until airflow db check; do
  echo "Postgres not ready yet..."
  sleep 5
done

echo "✅ Postgres is ready!"

# ------------------ Initialize Airflow DB ------------------

echo "🗄️ Initializing Airflow DB..."
airflow db migrate

# ------------------ Create Admin User ------------------

echo "👤 Creating admin user if not exists..."

airflow users list | grep -q "admin" || airflow users create \
    --username admin \
    --firstname Sujoy \
    --lastname Halder \
    --role Admin \
    --email admin@example.com \
    --password admin

# ------------------ Start Scheduler ------------------

echo "📅 Starting Airflow Scheduler..."
airflow scheduler &

# ------------------ Start Webserver ------------------

echo "🌐 Starting Airflow Webserver..."
exec airflow webserver