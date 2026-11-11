#!/usr/bin/env bash
# ==============================================================================
# Environment Teardown and Cleanup Script
# Stops containers, removes networks, cleans orphaned resources, and checks
# for lingering containers.
#
# Usage:
#   ./scripts/cleanup.sh [--volumes]
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

DOCKER_COMPOSE_CMD="docker compose"
if ! docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
fi

DOWN_ARGS="--remove-orphans"
if [ "${1:-}" == "--volumes" ] || [ "${1:-}" == "-v" ]; then
    echo "[CLEANUP] Flag detected: removing persistent volumes as well..."
    DOWN_ARGS="${DOWN_ARGS} --volumes"
fi

echo "============================================================"
echo "[CLEANUP] Stopping and tearing down environment..."
echo "============================================================"

$DOCKER_COMPOSE_CMD down ${DOWN_ARGS}

echo "------------------------------------------------------------"
echo "[CLEANUP] Checking for any lingering containers..."
echo "------------------------------------------------------------"

LINGERING=$(docker ps -a --filter "name=de_base_workspace" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}")

if [ -n "$LINGERING" ] && [ "$(echo "$LINGERING" | wc -l)" -gt 1 ]; then
    echo "[WARN] Lingering containers found for project 'de_base_workspace':"
    echo "$LINGERING"
    echo "To force remove lingering containers run:"
    echo "  docker rm -f \$(docker ps -a -q --filter 'name=de_base_workspace')"
else
    echo "[INFO] No lingering project containers detected. Environment is clean!"
fi

echo "============================================================"
echo "[CLEANUP] Teardown complete."
echo "============================================================"

