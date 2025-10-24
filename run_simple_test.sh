#!/bin/bash

# Script simple para test multi-cliente
# Uso: ./run_simple_test.sh

echo "🚀 Ejecutando test con 2 clientes..."

# Generar compose con 2 clientes
echo "📝 Generando docker-compose.yaml..."
cp compose_generator.py compose_generator_temp.py
sed -i 's/^CLIENTS = .*/CLIENTS = 2/' compose_generator_temp.py
python3 compose_generator_temp.py
rm compose_generator_temp.py

# Limpiar Docker
echo "🧹 Limpiando Docker..."
docker compose down --remove-orphans > /dev/null 2>&1 || true

# Ejecutar Docker Compose
echo "🐳 Iniciando servicios..."
docker compose up --build -d

echo "⏰ Esperando a que terminen los servicios..."
sleep 150

# Ejecutar comparación
echo "📊 Ejecutando comparación CSV..."
python3 compare_csv_multi.py

echo "✅ Completado!"