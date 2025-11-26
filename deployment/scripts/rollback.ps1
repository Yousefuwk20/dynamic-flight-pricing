# Rollback Script - Revert to previous task definition
$ErrorActionPreference = "Stop"

$AWS_REGION = "us-east-1"
$ECS_CLUSTER = "flight-pricing-cluster"
$ECS_SERVICE = "flight-pricing-service"

Write-Host "========================================" -ForegroundColor Red
Write-Host "Rolling Back Deployment" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red
Write-Host ""

# Get the previous task definition
Write-Host "Finding previous task definition..." -ForegroundColor Yellow
$taskDefs = aws ecs list-task-definitions --family-prefix flight-pricing-task --sort DESC --region $AWS_REGION --query 'taskDefinitionArns' --output json | ConvertFrom-Json

if ($taskDefs.Count -lt 2) {
    Write-Host "✗ No previous task definition found" -ForegroundColor Red
    exit 1
}

$previousTaskDef = $taskDefs[1]
Write-Host "Previous task definition: $previousTaskDef" -ForegroundColor Cyan
Write-Host ""

# Update service to use previous task definition
Write-Host "Rolling back to previous version..." -ForegroundColor Yellow
aws ecs update-service --cluster $ECS_CLUSTER --service $ECS_SERVICE --task-definition $previousTaskDef --force-new-deployment --region $AWS_REGION | Out-Null

Write-Host "✓ Rollback initiated" -ForegroundColor Green
Write-Host ""
Write-Host "Waiting for service to stabilize..." -ForegroundColor Yellow
aws ecs wait services-stable --cluster $ECS_CLUSTER --services $ECS_SERVICE --region $AWS_REGION

Write-Host ""
Write-Host "✓ Rollback complete" -ForegroundColor Green
