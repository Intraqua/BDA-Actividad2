#!/bin/bash
# =============================================================================
# Script de inicialización del nodo STANDBY
# Configura la replicación desde el PRIMARY
# =============================================================================

set -e

PGDATA="/var/lib/postgresql/data"
PRIMARY_HOST="pg-primary"
PRIMARY_PORT="5432"
REPLICATION_USER="replicator"
REPLICATION_PASSWORD="replicator123"

echo "=========================================="
echo "Inicializando nodo STANDBY"
echo "=========================================="

# Función para esperar al PRIMARY
wait_for_primary() {
    echo "Esperando a que el PRIMARY esté disponible..."
    until PGPASSWORD=$REPLICATION_PASSWORD pg_isready -h $PRIMARY_HOST -p $PRIMARY_PORT -U $REPLICATION_USER; do
        echo "PRIMARY no disponible, reintentando en 2 segundos..."
        sleep 2
    done
    echo "PRIMARY disponible!"
}

# Función para verificar si ya está inicializado
is_initialized() {
    if [ -f "$PGDATA/PG_VERSION" ]; then
        return 0
    else
        return 1
    fi
}

# Función para hacer backup base del PRIMARY
do_base_backup() {
    echo "Realizando pg_basebackup desde PRIMARY..."
    
    # Limpiar directorio de datos si existe
    rm -rf "$PGDATA"/*
    
    # Crear archivo .pgpass para autenticación
    echo "$PRIMARY_HOST:$PRIMARY_PORT:replication:$REPLICATION_USER:$REPLICATION_PASSWORD" > ~/.pgpass
    chmod 600 ~/.pgpass
    
    # Ejecutar pg_basebackup
    PGPASSWORD=$REPLICATION_PASSWORD pg_basebackup \
        -h $PRIMARY_HOST \
        -p $PRIMARY_PORT \
        -U $REPLICATION_USER \
        -D $PGDATA \
        -Fp \
        -Xs \
        -P \
        -R \
        -W
    
    echo "pg_basebackup completado!"
}

# Función para configurar el standby
configure_standby() {
    echo "Configurando parámetros de standby..."
    
    # Crear archivo standby.signal (indica que es un standby)
    touch "$PGDATA/standby.signal"
    
    # Configurar conexión al primary en postgresql.auto.conf
    cat >> "$PGDATA/postgresql.auto.conf" <<EOF

# Configuración de replicación - Standby
primary_conninfo = 'host=$PRIMARY_HOST port=$PRIMARY_PORT user=$REPLICATION_USER password=$REPLICATION_PASSWORD application_name=standby1'
primary_slot_name = 'standby_slot'
hot_standby = on
EOF
    
    # Asegurar permisos correctos
    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
    
    echo "Standby configurado!"
}

# Función para crear slot de replicación en el PRIMARY
create_replication_slot() {
    echo "Creando slot de replicación en PRIMARY..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $PRIMARY_HOST -p $PRIMARY_PORT -U postgres -d marketplace_ha -c \
        "SELECT pg_create_physical_replication_slot('standby_slot', true);" 2>/dev/null || \
        echo "Slot ya existe o no se pudo crear (continuando...)"
}

# =============================================================================
# PROCESO PRINCIPAL
# =============================================================================

# Esperar al PRIMARY
wait_for_primary

# Verificar si ya está inicializado
if is_initialized; then
    echo "STANDBY ya inicializado, verificando configuración..."
    
    # Verificar que tiene standby.signal
    if [ ! -f "$PGDATA/standby.signal" ]; then
        echo "Creando archivo standby.signal..."
        touch "$PGDATA/standby.signal"
    fi
else
    echo "Primera inicialización del STANDBY..."
    
    # Crear slot de replicación
    create_replication_slot
    
    # Hacer backup base
    do_base_backup
    
    # Configurar standby
    configure_standby
fi

echo "=========================================="
echo "Iniciando PostgreSQL en modo STANDBY..."
echo "=========================================="

# Iniciar PostgreSQL
exec postgres \
    -c hot_standby=on \
    -c wal_level=replica \
    -c max_wal_senders=10 \
    -c max_replication_slots=10
