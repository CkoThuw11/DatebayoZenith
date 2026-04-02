#!/usr/bin/env pwsh

Write-Host "Waiting for Kafka Connect to be ready..." -ForegroundColor Yellow

# Loop until the Kafka Connect API responds with 200 OK
do {
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 5
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8083/connectors" -UseBasicParsing -ErrorAction Stop
        $statusCode = $response.StatusCode
    } catch {
        $statusCode = 0
    }
} while ($statusCode -ne 200)

Write-Host "`nKafka Connect is ready!" -ForegroundColor Green

Write-Host "Registering connector..." -ForegroundColor Yellow

# Submit the configuration file
$body = Get-Content -Path "connectors\debezium-postgres.json" -Raw
Invoke-RestMethod -Uri "http://localhost:8083/connectors" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body

Write-Host "`nCurrent Connectors:" -ForegroundColor Green
Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Get

Write-Host "`nConnector status:" -ForegroundColor Green
Invoke-RestMethod -Uri "http://localhost:8083/connectors/inventory-connector/status" -Method Get