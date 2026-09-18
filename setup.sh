#!/bin/bash
# =============================================================================
# setup.sh — Instala dependencias y levanta el simulador de mercado
# =============================================================================
set -e  # Si cualquier comando falla, el script se detiene

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║      SIMULADOR DE MERCADO — Setup Automático         ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. Verificar que Docker esté instalado ─────────────────────────────────
echo "▶  Verificando Docker..."
if ! command -v docker &> /dev/null; then
    echo "✗  Docker no está instalado."
    echo "   Instálalo desde: https://www.docker.com/products/docker-desktop"
    exit 1
fi
echo "✓  Docker encontrado: $(docker --version)"

# ── 2. Verificar que Docker Compose esté disponible ───────────────────────
echo "▶  Verificando Docker Compose..."
if ! docker compose version &> /dev/null; then
    echo "✗  Docker Compose no está disponible."
    echo "   Asegúrate de tener Docker Desktop actualizado."
    exit 1
fi
echo "✓  Docker Compose encontrado: $(docker compose version)"

# ── 3. Construir y levantar todos los contenedores ────────────────────────
# Los go.sum ya están incluidos en el repo, Docker los usa directamente.
echo ""
echo "▶  Construyendo imágenes Docker (puede tomar unos minutos la primera vez)..."
docker compose build

echo ""
echo "▶  Levantando todos los contenedores..."
docker compose up -d

# ── 4. Esperar a que los servicios estén listos ────────────────────────────
echo ""
echo "▶  Esperando que los servicios inicien..."
sleep 5

# ── 5. Verificar el health check ──────────────────────────────────────────
echo ""
echo "▶  Verificando estado de los servicios..."
HEALTH=$(curl -s http://localhost:8080/health 2>/dev/null || echo "error")

if echo "$HEALTH" | grep -q "UP"; then
    echo "✓  Todos los servicios están UP"
else
    echo "⚠  Algunos servicios pueden tardar un poco más en arrancar."
    echo "   Verifica con: docker compose logs"
fi

# ── 6. Mostrar resumen ─────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║               🚀 Sistema listo                       ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Frontend   →  http://localhost:3000                 ║"
echo "║  Gateway    →  http://localhost:8080                 ║"
echo "║  Health     →  http://localhost:8080/health          ║"
echo "║  Servicios  →  http://localhost:8080/services        ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Microservicios (acceso directo para pruebas):       ║"
echo "║  Orders  →  http://localhost:8081/orders             ║"
echo "║  Prices  →  http://localhost:8082/prices             ║"
echo "║  Users   →  http://localhost:8083/users              ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Para apagar:  docker compose down                   ║"
echo "║  Para logs:    docker compose logs -f                ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
