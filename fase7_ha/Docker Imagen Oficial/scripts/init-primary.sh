#!/bin/bash
# =============================================================================
# Script de inicialización del nodo PRIMARY
# Se ejecuta automáticamente en el primer arranque
# =============================================================================

set -e

echo "=========================================="
echo "Configurando nodo PRIMARY para replicación"
echo "=========================================="

# Crear usuario de replicación
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Crear usuario de replicación si no existe
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
            CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator123';
            RAISE NOTICE 'Usuario replicator creado';
        END IF;
    END
    \$\$;
EOSQL

# Configurar pg_hba.conf para permitir replicación
echo "Configurando pg_hba.conf..."
cat >> "$PGDATA/pg_hba.conf" <<EOF

# Configuración de replicación - Fase 7 Alta Disponibilidad
host    replication     replicator      0.0.0.0/0               md5
host    all             all             0.0.0.0/0               md5
EOF

echo "=========================================="
echo "PRIMARY configurado correctamente"
echo "=========================================="
