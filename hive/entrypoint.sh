#!/bin/bash
set -e

# 1. Skip envsubst
# We assume hive-site.xml and core-site.xml are already correctly 
# configured and copied into the image during the Docker build.
echo "⚠️ Skipping environment substitution (using hardcoded config files)..."

# 2. Wait for PostgreSQL (Hardcoded to 'metastore-db' on port 5432)
echo "💬 Waiting for metastore-db:5432 to be ready..."
until timeout 1s bash -c "true < /dev/tcp/metastore-db/5432" 2>/dev/null; do
  echo "⏳ Waiting for Postgres..."
  sleep 2
done

echo "✅ PostgreSQL is up!"

# 3. Check and Initialize Schema (Mandatory for Hive to function)
echo "💬 Checking if Hive schema is already in Postgres..."

if ! /opt/hive/bin/schematool -dbType postgres -info > /dev/null 2>&1; then
  echo "📦 Initializing schema (Postgres)..."
  /opt/hive/bin/schematool -dbType postgres -initSchema --verbose
else
  echo "✅ Schema already exists"
fi

# 4. Start Hive Metastore
echo "🚀 Starting Hive Metastore Thrift server on port 9083..."
exec hive --service metastore