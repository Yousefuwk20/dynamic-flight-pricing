# Flight Pricing API
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app.py .
COPY pricing_system.py .
COPY flight_pricing_model.json .
COPY label_encoders.pkl .
COPY index.html .

# Expose port
EXPOSE 8000

# Run the API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
