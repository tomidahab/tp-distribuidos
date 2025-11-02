#!/usr/bin/env bash
set -euo pipefail

# test_filter_by_hour_kill.sh
# Script para probar la tolerancia a fallos matando un worker de filter_by_hour
# y luego levantándolo de nuevo

# Configuración
WORKER_NAME="${1:-filter_by_hour_worker_1}"  # Worker a matar (por defecto el 0)
DOWN_TIME="${2:-15}"                           # Tiempo que permanece caído (segundos)
LOG_WAIT_TIMEOUT=60                           # Timeout para esperar logs de startup

echo "[kill-test] Configuración:"
echo "[kill-test] Worker a matar: $WORKER_NAME"
echo "[kill-test] Tiempo caído: ${DOWN_TIME}s"
echo ""

# Verificar que el worker existe y está corriendo
if ! docker ps --format '{{.Names}}' | grep -q "^${WORKER_NAME}$"; then
    echo "[kill-test] ERROR: Worker $WORKER_NAME no encontrado o no está corriendo"
    echo "[kill-test] Workers disponibles:"
    docker ps --format "table {{.Names}}	{{.Status}}" | grep filter_by_hour || echo "Ningún worker filter_by_hour corriendo"
    exit 1
fi

echo "[kill-test] Worker $WORKER_NAME encontrado y corriendo"

# Obtener información antes del kill
echo "[kill-test] Estado antes del kill:"
docker ps --format "table {{.Names}}	{{.Status}}" | grep filter_by_hour

echo ""
echo "[kill-test] ⚠️  MATANDO worker $WORKER_NAME..."
docker kill "$WORKER_NAME"

echo "[kill-test] ✅ Worker $WORKER_NAME eliminado"
echo "[kill-test] Estado después del kill:"
docker ps --format "table {{.Names}}	{{.Status}}" | grep filter_by_hour || echo "Ningún worker filter_by_hour corriendo"

echo ""
echo "[kill-test] ⏳ Esperando ${DOWN_TIME} segundos..."
sleep "$DOWN_TIME"

echo ""
echo "[kill-test] 🔄 Levantando worker $WORKER_NAME..."

# Necesitamos extraer el índice del worker para saber qué servicio levantar
if [[ $WORKER_NAME =~ filter_by_hour_worker_([0-9]+) ]]; then
    WORKER_INDEX="${BASH_REMATCH[1]}"
    SERVICE_NAME="filter_by_hour_worker_${WORKER_INDEX}"
    
    # Levantar el servicio específico
    docker compose up -d "$SERVICE_NAME"
    
    echo "[kill-test] ✅ Comando docker compose up ejecutado para $SERVICE_NAME"
    
    # Esperar a que el worker aparezca en docker ps
    echo "[kill-test] Esperando que $WORKER_NAME aparezca en la lista de containers..."
    timeout=30
    while [ $timeout -gt 0 ]; do
        if docker ps --format '{{.Names}}' | grep -q "^${WORKER_NAME}$"; then
            echo "[kill-test] ✅ Container $WORKER_NAME detectado"
            break
        fi
        sleep 1
        timeout=$((timeout - 1))
    done
    
    if [ $timeout -le 0 ]; then
        echo "[kill-test] ⚠️  Timeout esperando que aparezca $WORKER_NAME"
        echo "[kill-test] Containers actuales:"
        docker ps --format "table {{.Names}}	{{.Status}}"
        exit 1
    fi
    
    # Esperar log de reconexión
    echo "[kill-test] Esperando log de startup de $WORKER_NAME..."
    start_time=$SECONDS
    while [ $((SECONDS - start_time)) -lt $LOG_WAIT_TIMEOUT ]; do
        if docker logs "$WORKER_NAME" 2>&1 | tail -n 20 | grep -q "connecting to RabbitMQ"; then
            echo "[kill-test] ✅ $WORKER_NAME se reconectó exitosamente"
            break
        fi
        sleep 1
    done
    
    if [ $((SECONDS - start_time)) -ge $LOG_WAIT_TIMEOUT ]; then
        echo "[kill-test] ⚠️  Timeout esperando log de reconexión de $WORKER_NAME"
        echo "[kill-test] Últimos logs de $WORKER_NAME:"
        docker logs "$WORKER_NAME" 2>&1 | tail -n 10
    fi
    
else
    echo "[kill-test] ERROR: No se pudo extraer índice del worker name: $WORKER_NAME"
    exit 1
fi

echo ""
echo "[kill-test] Estado final:"
docker ps --format "table {{.Names}}	{{.Status}}" | grep filter_by_hour

echo ""
echo "[kill-test] 🎉 Test completado!"
echo "[kill-test] - Worker matado: $WORKER_NAME"
echo "[kill-test] - Tiempo caído: ${DOWN_TIME}s"
echo "[kill-test] - Worker recuperado: $(docker ps --format '{{.Names}}' | grep "^${WORKER_NAME}$" && echo "SÍ" || echo "NO")"

# Mostrar resumen de logs recientes del worker recuperado
echo ""
echo "[kill-test] Últimos logs de $WORKER_NAME:"
docker logs "$WORKER_NAME" 2>&1 | tail -n 5