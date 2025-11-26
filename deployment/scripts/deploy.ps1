# Flight Pricing API - AWS ECS Deployment Script
# This script builds, pushes, and deploys the updated ML model to AWS ECS

$ErrorActionPreference = "Stop"

# Configuration
$AWS_REGION = "us-east-1"
$AWS_ACCOUNT_ID = "590183820535"
$ECR_REPOSITORY = "flight-pricing-api"
$ECS_CLUSTER = "flight-pricing-cluster"
$ECS_SERVICE = "flight-pricing-service"
$TASK_FAMILY = "flight-pricing-task"

# Get the ml root directory (parent of deployment/scripts)
$ML_ROOT = (Get-Item $PSScriptRoot).Parent.Parent.FullName

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Flight Pricing API Deployment" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "ML Root: $ML_ROOT" -ForegroundColor Gray
Write-Host ""

# Step 1: Stop and remove old container if running locally
Write-Host "[1/8] Stopping local container..." -ForegroundColor Yellow
$process = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
if ($process) {
    Stop-Process -Id $process.OwningProcess -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Seconds 2
Write-Host "[OK] Local server stopped" -ForegroundColor Green
Write-Host ""

# Step 2: Build Docker image (from ml root, using deployment/dockerfile)
Write-Host "[2/8] Building Docker image..." -ForegroundColor Yellow
Push-Location $ML_ROOT
docker build -t flight-pricing-api:latest -f deployment/dockerfile .
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Docker build failed" -ForegroundColor Red
    Pop-Location
    exit 1
}
Pop-Location
Write-Host "[OK] Docker image built successfully" -ForegroundColor Green
Write-Host ""

# Step 3: Test Docker image locally
Write-Host "[3/8] Testing Docker image locally..." -ForegroundColor Yellow
docker run -d --name flight-pricing-test -p 8001:8000 flight-pricing-api:latest
Write-Host "Waiting for container to start..." -ForegroundColor Gray
Start-Sleep -Seconds 20

try {
    $testResult = Invoke-RestMethod -Uri "http://localhost:8001/health" -Method Get -ErrorAction Stop
    if ($testResult.status -eq "healthy") {
        Write-Host "[OK] Local Docker test passed" -ForegroundColor Green
    }
    else {
        Write-Host "[FAIL] Local Docker test failed" -ForegroundColor Red
        docker stop flight-pricing-test
        docker rm flight-pricing-test
        exit 1
    }
}
catch {
    Write-Host "[FAIL] Could not connect to test container" -ForegroundColor Red
    docker stop flight-pricing-test
    docker rm flight-pricing-test
    exit 1
}

docker stop flight-pricing-test
docker rm flight-pricing-test
Write-Host ""

# Step 4: Login to ECR
Write-Host "[4/8] Logging into AWS ECR..." -ForegroundColor Yellow
$ECR_URI = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
$password = aws ecr get-login-password --region $AWS_REGION
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Failed to get ECR password" -ForegroundColor Red
    exit 1
}
$password | docker login --username AWS --password-stdin $ECR_URI
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] ECR login failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Logged into ECR" -ForegroundColor Green
Write-Host ""

# Step 5: Tag and push image to ECR
Write-Host "[5/8] Pushing image to ECR..." -ForegroundColor Yellow
$IMAGE_URI = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:latest"
docker tag flight-pricing-api:latest $IMAGE_URI
docker push $IMAGE_URI
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Docker push failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Image pushed to ECR" -ForegroundColor Green
Write-Host ""

# Step 6: Stop old ECS service (scale to 0)
Write-Host "[6/8] Stopping old ECS service..." -ForegroundColor Yellow
aws ecs update-service --cluster $ECS_CLUSTER --service $ECS_SERVICE --desired-count 0 --region $AWS_REGION | Out-Null
Write-Host "Waiting for old tasks to stop..."
Start-Sleep -Seconds 30
Write-Host "[OK] Old service stopped" -ForegroundColor Green
Write-Host ""

# Step 7: Register new task definition
Write-Host "[7/8] Registering new task definition..." -ForegroundColor Yellow
$taskDefPath = Join-Path $ML_ROOT "deployment\aws\task-definition.json"
$taskDefArn = aws ecs register-task-definition --cli-input-json file://$taskDefPath --region $AWS_REGION --query 'taskDefinition.taskDefinitionArn' --output text
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Task definition registration failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Task definition registered: $taskDefArn" -ForegroundColor Green
Write-Host ""

# Step 8: Update service with new task definition
Write-Host "[8/8] Deploying new service..." -ForegroundColor Yellow
aws ecs update-service --cluster $ECS_CLUSTER --service $ECS_SERVICE --task-definition $taskDefArn --desired-count 1 --force-new-deployment --region $AWS_REGION | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Service update failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Service updated successfully" -ForegroundColor Green
Write-Host ""

# Wait for deployment to stabilize
Write-Host "Waiting for service to stabilize (this may take 2-3 minutes)..." -ForegroundColor Yellow
aws ecs wait services-stable --cluster $ECS_CLUSTER --services $ECS_SERVICE --region $AWS_REGION

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "The updated ML model is now live on AWS ECS." -ForegroundColor Cyan
Write-Host "Access your API via the load balancer endpoint." -ForegroundColor Cyan
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Yellow
Write-Host "  Check service status: aws ecs describe-services --cluster $ECS_CLUSTER --services $ECS_SERVICE --region $AWS_REGION" -ForegroundColor Gray
Write-Host "  View logs: aws logs tail /ecs/flight-pricing --follow --region $AWS_REGION" -ForegroundColor Gray
Write-Host "  Get load balancer URL: aws elbv2 describe-load-balancers --region $AWS_REGION --query 'LoadBalancers[0].DNSName' --output text" -ForegroundColor Gray
