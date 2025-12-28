-- =============================================================================
-- FASE 7 - VERIFICACION RAPIDA DEL CLUSTER
-- Sistema Marketplace Digital - Actividad 2
-- Autor: David Valbuena Segura
-- =============================================================================
-- Archivo: fase7_verificacion_rapida.sql
-- Propósito: Verificación rápida del estado del cluster HA
-- Ejecutar en: Cualquier nodo
-- =============================================================================

SET search_path TO marketplace_ha, public;

-- =============================================================================
-- 1. IDENTIFICACION DEL NODO
-- =============================================================================

SELECT 
    '=== IDENTIFICACION DEL NODO ===' AS seccion;

SELECT 
    CASE 
        WHEN pg_is_in_recovery() THEN 'STANDBY (Solo Lectura)'
        ELSE 'PRIMARY (Lectura/Escritura)'
    END AS rol_actual,
    inet_server_port() AS puerto,
    current_database() AS base_datos,
    NOW() AS timestamp;

-- =============================================================================
-- 2. ESTADO DE REPLICACION (Solo muestra datos en PRIMARY)
-- =============================================================================

SELECT 
    '=== ESTADO DE REPLICACION ===' AS seccion;

SELECT 
    application_name AS replica,
    state AS estado,
    sync_state AS modo,
    pg_size_pretty(pg_wal_lsn_diff(sent_lsn, replay_lsn)) AS lag
FROM pg_stat_replication;

-- =============================================================================
-- 3. CONTEO DE DATOS
-- =============================================================================

SELECT 
    '=== DATOS EN EL SISTEMA ===' AS seccion;

SELECT 
    'Clientes' AS tabla, COUNT(*) AS registros FROM Cliente
UNION ALL
SELECT 'Vendedores', COUNT(*) FROM Vendedor
UNION ALL
SELECT 'Productos', COUNT(*) FROM Producto
UNION ALL
SELECT 'Pedidos', COUNT(*) FROM Pedido
UNION ALL
SELECT 'Eventos HA', COUNT(*) FROM Log_Alta_Disponibilidad;

-- =============================================================================
-- 4. SALUD DEL CLUSTER
-- =============================================================================

SELECT 
    '=== SALUD DEL CLUSTER ===' AS seccion;

SELECT * FROM fn_salud_cluster();

-- =============================================================================
-- 5. RESUMEN
-- =============================================================================

SELECT 
    '=== RESUMEN ===' AS seccion;

SELECT * FROM vw_resumen_cluster;
