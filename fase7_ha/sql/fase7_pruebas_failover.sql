-- =============================================================================
-- FASE 7 - PRUEBAS DE FAILOVER Y RECUPERACION
-- Sistema Marketplace Digital - Actividad 2
-- Autor: David Valbuena Segura
-- =============================================================================
-- Archivo: fase7_pruebas_failover.sql
-- Propósito: Verificar y documentar el proceso de failover automático
-- =============================================================================

-- =============================================================================
-- SECCION 1: VERIFICACION PRE-FAILOVER (Ejecutar en PRIMARY)
-- =============================================================================

\echo ''
\echo '============================================================'
\echo 'SECCION 1: ESTADO INICIAL DEL CLUSTER'
\echo '============================================================'
\echo ''

-- 1.1 Verificar rol del nodo
SELECT 
    '1.1 ROL DEL NODO' AS test,
    CASE WHEN pg_is_in_recovery() THEN 'STANDBY' ELSE 'PRIMARY' END AS resultado;

-- 1.2 Estado de réplicas conectadas
SELECT 
    '1.2 REPLICAS CONECTADAS' AS test,
    application_name AS replica,
    state AS estado,
    sync_state AS modo_sync,
    pg_size_pretty(pg_wal_lsn_diff(sent_lsn, replay_lsn)) AS lag
FROM pg_stat_replication;

-- 1.3 Conteo de datos actuales
SELECT 
    '1.3 DATOS EN EL SISTEMA' AS test,
    (SELECT COUNT(*) FROM marketplace_ha.Cliente) AS clientes,
    (SELECT COUNT(*) FROM marketplace_ha.Producto) AS productos,
    (SELECT COUNT(*) FROM marketplace_ha.Pedido) AS pedidos;

-- =============================================================================
-- SECCION 2: INSERTAR DATOS DE PRUEBA ANTES DEL FAILOVER
-- =============================================================================

\echo ''
\echo '============================================================'
\echo 'SECCION 2: INSERTAR DATOS DE PRUEBA'
\echo '============================================================'
\echo ''

-- Solo ejecutar si es PRIMARY
DO $$
DECLARE
    v_cliente_id INTEGER;
    v_timestamp TEXT;
BEGIN
    IF NOT pg_is_in_recovery() THEN
        v_timestamp := TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS');
        
        -- Insertar cliente de prueba
        INSERT INTO marketplace_ha.Cliente (email, nombre)
        VALUES ('test_failover_' || v_timestamp || '@test.com', 
                'Cliente Test Failover ' || v_timestamp)
        RETURNING cliente_id INTO v_cliente_id;
        
        -- Registrar en log de HA
        INSERT INTO marketplace_ha.Log_Alta_Disponibilidad 
            (evento, descripcion, nodo_origen, datos_adicionales)
        VALUES 
            ('TEST_PRE_FAILOVER', 
             'Datos insertados antes de prueba de failover',
             'PRIMARY',
             jsonb_build_object('cliente_id', v_cliente_id, 'timestamp', v_timestamp));
        
        RAISE NOTICE 'Datos de prueba insertados: cliente_id = %', v_cliente_id;
        RAISE NOTICE 'Email: test_failover_%@test.com', v_timestamp;
    ELSE
        RAISE NOTICE 'Este nodo es STANDBY - No se pueden insertar datos';
    END IF;
END $$;

-- Verificar el último cliente insertado
SELECT 
    '2.1 ULTIMO CLIENTE INSERTADO' AS test,
    cliente_id,
    email,
    nombre,
    fecha_registro
FROM marketplace_ha.Cliente
ORDER BY cliente_id DESC
LIMIT 1;

-- =============================================================================
-- SECCION 3: VERIFICAR SINCRONIZACION (Ejecutar en STANDBY)
-- =============================================================================

\echo ''
\echo '============================================================'
\echo 'SECCION 3: VERIFICAR SINCRONIZACION EN STANDBY'
\echo '============================================================'
\echo ''

-- 3.1 Verificar rol (debe ser STANDBY)
SELECT 
    '3.1 ROL DEL NODO' AS test,
    CASE WHEN pg_is_in_recovery() THEN 'STANDBY - OK' ELSE 'PRIMARY - ERROR' END AS resultado;

-- 3.2 Verificar que los datos se replicaron
SELECT 
    '3.2 DATOS REPLICADOS' AS test,
    (SELECT COUNT(*) FROM marketplace_ha.Cliente) AS clientes,
    (SELECT COUNT(*) FROM marketplace_ha.Producto) AS productos,
    (SELECT COUNT(*) FROM marketplace_ha.Pedido) AS pedidos;

-- 3.3 Verificar último cliente (debe coincidir con PRIMARY)
SELECT 
    '3.3 ULTIMO CLIENTE' AS test,
    cliente_id,
    email,
    nombre
FROM marketplace_ha.Cliente
ORDER BY cliente_id DESC
LIMIT 1;

-- 3.4 Estado del WAL receiver
SELECT 
    '3.4 WAL RECEIVER' AS test,
    status AS estado,
    sender_host AS primary_host,
    pg_size_pretty(pg_wal_lsn_diff(latest_end_lsn, received_lsn)) AS lag
FROM pg_stat_wal_receiver;

-- =============================================================================
-- SECCION 4: VERIFICAR CAPACIDAD DE FAILOVER (Ejecutar en STANDBY)
-- =============================================================================

\echo ''
\echo '============================================================'
\echo 'SECCION 4: VERIFICAR CAPACIDAD DE FAILOVER'
\echo '============================================================'
\echo ''

-- 4.1 Verificar que el failover es posible
SELECT * FROM fn_verificar_failover_posible();

-- =============================================================================
-- SECCION 5: SIMULACION DE FAILOVER
-- =============================================================================
-- IMPORTANTE: Esta sección describe el proceso de failover.
-- El failover automático es ejecutado por el failover_manager cuando
-- detecta que el PRIMARY no está disponible.
-- =============================================================================

\echo ''
\echo '============================================================'
\echo 'SECCION 5: PROCEDIMIENTO DE FAILOVER'
\echo '============================================================'
\echo ''
\echo 'Para simular un failover automático:'
\echo ''
\echo '1. Verificar que el failover-manager está activo:'
\echo '   docker logs marketplace_failover_manager'
\echo ''
\echo '2. Detener el PRIMARY para simular fallo:'
\echo '   docker stop marketplace_primary'
\echo ''
\echo '3. Observar los logs del failover-manager:'
\echo '   docker logs -f marketplace_failover_manager'
\echo ''
\echo '4. El manager detectará el fallo y ejecutará:'
\echo '   SELECT pg_promote();'
\echo ''
\echo '5. El STANDBY se convertirá en nuevo PRIMARY'
\echo '============================================================'

-- =============================================================================
-- SECCION 6: VERIFICACION POST-FAILOVER (Ejecutar en nuevo PRIMARY)
-- =============================================================================

\echo ''
\echo '============================================================'
\echo 'SECCION 6: VERIFICACION POST-FAILOVER'
\echo '============================================================'
\echo ''

-- 6.1 Verificar que ahora es PRIMARY
SELECT 
    '6.1 NUEVO ROL' AS test,
    CASE 
        WHEN pg_is_in_recovery() THEN 'STANDBY - Failover NO completado'
        ELSE 'PRIMARY - Failover EXITOSO'
    END AS resultado;

-- 6.2 Verificar integridad de datos
SELECT 
    '6.2 INTEGRIDAD DE DATOS' AS test,
    (SELECT COUNT(*) FROM marketplace_ha.Cliente) AS clientes,
    (SELECT COUNT(*) FROM marketplace_ha.Producto) AS productos,
    (SELECT COUNT(*) FROM marketplace_ha.Pedido) AS pedidos,
    'Verificar que coinciden con pre-failover' AS nota;

-- 6.3 Verificar que se pueden insertar datos
DO $$
DECLARE
    v_new_id INTEGER;
BEGIN
    IF NOT pg_is_in_recovery() THEN
        INSERT INTO marketplace_ha.Cliente (email, nombre)
        VALUES ('post_failover_' || TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS') || '@test.com',
                'Cliente Post-Failover')
        RETURNING cliente_id INTO v_new_id;
        
        -- Registrar el failover
        INSERT INTO marketplace_ha.Log_Alta_Disponibilidad 
            (evento, descripcion, nodo_origen)
        VALUES 
            ('POST_FAILOVER', 
             'Primer INSERT después de failover exitoso',
             'NUEVO_PRIMARY');
        
        RAISE NOTICE 'EXITO: Nuevo PRIMARY acepta escrituras (cliente_id = %)', v_new_id;
    ELSE
        RAISE NOTICE 'ERROR: El nodo sigue siendo STANDBY';
    END IF;
END $$;

-- 6.4 Consultar historial de eventos HA
SELECT 
    '6.4 ULTIMOS EVENTOS HA' AS seccion,
    evento,
    descripcion,
    nodo_origen,
    timestamp_evento
FROM marketplace_ha.Log_Alta_Disponibilidad
ORDER BY timestamp_evento DESC
LIMIT 5;

-- =============================================================================
-- SECCION 7: RESUMEN DE PRUEBAS
-- =============================================================================

\echo ''
\echo '============================================================'
\echo 'SECCION 7: RESUMEN DE VERIFICACION'
\echo '============================================================'
\echo ''

SELECT 
    'RESUMEN FINAL' AS seccion,
    CASE WHEN pg_is_in_recovery() THEN 'STANDBY' ELSE 'PRIMARY' END AS rol_actual,
    (SELECT COUNT(*) FROM marketplace_ha.Cliente) AS total_clientes,
    (SELECT COUNT(*) FROM marketplace_ha.Log_Alta_Disponibilidad) AS eventos_ha,
    NOW() AS timestamp_verificacion;

\echo ''
\echo '============================================================'
\echo 'PRUEBAS DE FAILOVER COMPLETADAS'
\echo '============================================================'
