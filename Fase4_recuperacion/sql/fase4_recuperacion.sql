-- ========================================
-- ACTIVIDAD 2 - FASE 4: RECUPERACION Y TOLERANCIA A FALLOS
-- Mecanismos WAL, Checkpoints y PITR
-- Autor: David Valbuena Segura
-- Universidad: UNIPRO
-- ========================================

-- ========================================
-- 1. VERIFICACION DEL ENTORNO
-- ========================================

\c marketplace_indices;

SELECT '=== FASE 4: RECUPERACION ===' AS fase;
SELECT '=== CONFIGURACION WAL ACTUAL ===' AS seccion;

-- Verificar configuracion WAL actual
SELECT name, setting, unit, short_desc
FROM pg_settings
WHERE name IN (
    'wal_level',
    'archive_mode', 
    'archive_command',
    'checkpoint_timeout',
    'checkpoint_completion_target',
    'max_wal_size',
    'min_wal_size',
    'wal_buffers'
)
ORDER BY name;

-- ========================================
-- 2. CICLO DE VIDA DE TRANSACCION REGISTRADA
-- ========================================

SELECT '=== CICLO DE VIDA DE TRANSACCION ===' AS seccion;

-- Crear tabla de auditoria para demostrar el ciclo
DROP TABLE IF EXISTS log_transacciones;
CREATE TABLE log_transacciones (
    id SERIAL PRIMARY KEY,
    lsn_inicio pg_lsn,
    lsn_fin pg_lsn,
    xid BIGINT,
    operacion VARCHAR(50),
    tabla_afectada VARCHAR(50),
    datos_anteriores JSONB,
    datos_nuevos JSONB,
    timestamp_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Funcion para registrar el ciclo de vida
CREATE OR REPLACE FUNCTION registrar_ciclo_transaccion(
    p_operacion VARCHAR(50),
    p_tabla VARCHAR(50),
    p_datos_ant JSONB DEFAULT NULL,
    p_datos_new JSONB DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_lsn pg_lsn;
    v_xid BIGINT;
BEGIN
    -- Obtener LSN actual (posicion en WAL)
    v_lsn := pg_current_wal_lsn();
    -- Obtener ID de transaccion actual
    v_xid := txid_current();
    
    INSERT INTO log_transacciones (lsn_inicio, xid, operacion, tabla_afectada, datos_anteriores, datos_nuevos)
    VALUES (v_lsn, v_xid, p_operacion, p_tabla, p_datos_ant, p_datos_new);
END;
$$ LANGUAGE plpgsql;

-- Demostrar ciclo de vida con transaccion de compra
SELECT 'Demostracion: Ciclo de vida de transaccion' AS info;

-- Guardar estado inicial
SELECT pg_current_wal_lsn() AS lsn_antes_transaccion;


-- PASO A: Ver LSN antes de la transacción
SELECT pg_current_wal_lsn() AS lsn_antes_transaccion;

-- PASO B: Ejecutar transacción completa
BEGIN;
    -- Fase 1: BEGIN registrado
    SELECT registrar_ciclo_transaccion('BEGIN', NULL);
    
    -- Fase 2: Modificación (cambios en shared buffers)
    UPDATE Producto SET stock = stock - 1 WHERE id_producto = 1;
    SELECT registrar_ciclo_transaccion('UPDATE', 'Producto', 
        '{"stock": "antes"}'::jsonb, 
        '{"stock": "despues"}'::jsonb);
    
    -- Fase 3: Los cambios están en WAL buffer
    -- Fase 4: COMMIT - fsync del WAL
COMMIT;

-- PASO C: Ver LSN después de la transacción
SELECT pg_current_wal_lsn() AS lsn_despues_transaccion;


-- Mostrar registro del ciclo
SELECT 'Registro del ciclo de transaccion' AS info;
SELECT xid, operacion, tabla_afectada, lsn_inicio, timestamp_registro
FROM log_transacciones
ORDER BY id DESC LIMIT 5;

-- ========================================
-- 3. SIMULACION DE FALLO Y RECUPERACION
-- ========================================

SELECT '=== SIMULACION DE FALLO ===' AS seccion;

-- Crear tabla para simular transacciones en diferentes estados
DROP TABLE IF EXISTS transacciones_simuladas;
CREATE TABLE transacciones_simuladas (
    id SERIAL PRIMARY KEY,
    descripcion VARCHAR(100),
    estado VARCHAR(20), -- 'committed', 'in_progress', 'aborted'
    valor_antes INTEGER,
    valor_despues INTEGER,
    timestamp_op TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Preparar producto para simulacion
UPDATE Producto SET stock = 100 WHERE id_producto = 1;

-- Transaccion T1: Completada (necesitara REDO si no llego a disco)
BEGIN;
    INSERT INTO transacciones_simuladas (descripcion, estado, valor_antes, valor_despues)
    VALUES ('T1: Compra completada', 'committed', 100, 97);
    
    UPDATE Producto SET stock = 97 WHERE id_producto = 1;
COMMIT;

SELECT 'T1 COMMITTED - Si fallo antes de checkpoint, requiere REDO' AS estado_t1;

-- Transaccion T2: En progreso al momento del "fallo" (necesitara UNDO)
-- Nota: En un fallo real, esta transaccion quedaria sin confirmar
BEGIN;
    INSERT INTO transacciones_simuladas (descripcion, estado, valor_antes, valor_despues)
    VALUES ('T2: Compra interrumpida', 'in_progress', 97, 94);
    
    UPDATE Producto SET stock = 94 WHERE id_producto = 1;
    
    -- Simular punto de fallo (no hacemos commit)
    SELECT 'T2 EN PROGRESO - Simulando fallo antes de COMMIT' AS estado_t2;
    SELECT stock AS stock_durante_t2 FROM Producto WHERE id_producto = 1;
ROLLBACK; -- En fallo real, el sistema haria UNDO automaticamente

SELECT 'T2 ROLLBACK (simula UNDO automatico tras fallo)' AS resultado_recuperacion;

-- Verificar estado despues de "recuperacion"
SELECT 'Estado despues de recuperacion simulada' AS info;
SELECT id_producto, stock AS stock_recuperado FROM Producto WHERE id_producto = 1;

-- ========================================
-- 4. DEMOSTRACION UNDO/REDO CON SAVEPOINTS
-- ========================================

SELECT '=== DEMOSTRACION UNDO/REDO ===' AS seccion;

-- Restaurar stock inicial
UPDATE Producto SET stock = 100 WHERE id_producto = 1;

-- Crear tabla de log de operaciones
DROP TABLE IF EXISTS log_undo_redo;
CREATE TABLE log_undo_redo (
    id SERIAL PRIMARY KEY,
    operacion VARCHAR(20),
    tabla_obj VARCHAR(50),
    valor_undo TEXT,
    valor_redo TEXT,
    lsn pg_lsn,
    aplicado BOOLEAN DEFAULT FALSE
);

-- Funcion que simula registro para UNDO/REDO
CREATE OR REPLACE FUNCTION compra_con_log_recuperacion(
    p_id_producto INTEGER,
    p_cantidad INTEGER
) RETURNS TABLE(exito BOOLEAN, mensaje TEXT, stock_final INTEGER) AS $$
DECLARE
    v_stock_antes INTEGER;
    v_stock_despues INTEGER;
    v_lsn pg_lsn;
BEGIN
    -- Obtener LSN actual
    v_lsn := pg_current_wal_lsn();
    
    -- Leer valor actual (para UNDO)
    SELECT stock INTO v_stock_antes FROM Producto WHERE id_producto = p_id_producto;
    
    -- Calcular nuevo valor (para REDO)
    v_stock_despues := v_stock_antes - p_cantidad;
    
    -- Registrar en log (antes de modificar - Write-Ahead!)
    INSERT INTO log_undo_redo (operacion, tabla_obj, valor_undo, valor_redo, lsn)
    VALUES ('UPDATE', 'Producto.stock', 
            format('stock=%s', v_stock_antes),
            format('stock=%s', v_stock_despues),
            v_lsn);
    
    -- Verificar stock suficiente
    IF v_stock_antes < p_cantidad THEN
        RETURN QUERY SELECT FALSE, 'Stock insuficiente'::TEXT, v_stock_antes;
        RETURN;
    END IF;
    
    -- Aplicar cambio
    UPDATE Producto SET stock = v_stock_despues WHERE id_producto = p_id_producto;
    
    -- Marcar como aplicado
    UPDATE log_undo_redo SET aplicado = TRUE WHERE lsn = v_lsn;
    
    RETURN QUERY SELECT TRUE, 'Compra registrada con log de recuperacion'::TEXT, v_stock_despues;
END;
$$ LANGUAGE plpgsql;

-- Ejecutar compras con logging
SELECT 'Ejecutando compras con registro de recuperacion' AS info;
SELECT * FROM compra_con_log_recuperacion(1, 5);
SELECT * FROM compra_con_log_recuperacion(1, 10);
SELECT * FROM compra_con_log_recuperacion(1, 3);

-- Mostrar log de operaciones
SELECT 'Log de operaciones (base para UNDO/REDO)' AS info;
SELECT id, operacion, valor_undo, valor_redo, aplicado, lsn
FROM log_undo_redo ORDER BY id;

-- ========================================
-- 5. ESTRATEGIA DE CHECKPOINTING
-- ========================================

SELECT '=== CONFIGURACION DE CHECKPOINTS ===' AS seccion;

-- Mostrar configuracion actual de checkpoints
SELECT 'Configuracion actual de checkpoints' AS info;
SELECT name, setting, unit, 
       CASE name 
           WHEN 'checkpoint_timeout' THEN 'Intervalo maximo entre checkpoints'
           WHEN 'checkpoint_completion_target' THEN 'Fraccion del intervalo para completar'
           WHEN 'max_wal_size' THEN 'Tamano WAL que fuerza checkpoint'
           WHEN 'min_wal_size' THEN 'Tamano WAL minimo a mantener'
       END AS descripcion
FROM pg_settings
WHERE name IN ('checkpoint_timeout', 'checkpoint_completion_target', 
               'max_wal_size', 'min_wal_size');

-- Informacion del ultimo checkpoint
SELECT 'Informacion del ultimo checkpoint' AS info;
SELECT checkpoint_lsn, redo_lsn, checkpoint_time
FROM pg_control_checkpoint();

-- Estadisticas de checkpoints
SELECT 'Estadisticas de checkpoints (desde inicio del servidor)' AS info;
SELECT checkpoints_timed, checkpoints_req, 
       checkpoint_write_time, checkpoint_sync_time,
       buffers_checkpoint
FROM pg_stat_bgwriter;

-- Forzar checkpoint manual (para demostracion)
SELECT 'Ejecutando CHECKPOINT manual' AS info;
CHECKPOINT;

-- Verificar nuevo estado
SELECT 'Estado post-checkpoint' AS info;
SELECT checkpoint_lsn AS nuevo_checkpoint_lsn, 
       checkpoint_time AS timestamp_checkpoint
FROM pg_control_checkpoint();

-- ========================================
-- 6. POLITICA DE RETENCION DE LOGS
-- ========================================

SELECT '=== POLITICA DE RETENCION WAL ===' AS seccion;

-- Verificar configuracion de archivado
SELECT 'Configuracion de archivado WAL' AS info;
SELECT name, setting,
       CASE name
           WHEN 'archive_mode' THEN 'Modo de archivado (on/off)'
           WHEN 'archive_command' THEN 'Comando para archivar segmentos'
           WHEN 'archive_timeout' THEN 'Forzar archivado tras N segundos'
           WHEN 'wal_level' THEN 'Nivel de detalle WAL'
       END AS descripcion
FROM pg_settings
WHERE name IN ('archive_mode', 'archive_command', 'archive_timeout', 'wal_level');

-- Crear tabla de politicas de backup
DROP TABLE IF EXISTS politica_backup;
CREATE TABLE politica_backup (
    id SERIAL PRIMARY KEY,
    tipo_backup VARCHAR(50),
    frecuencia VARCHAR(50),
    retencion VARCHAR(50),
    descripcion TEXT
);

INSERT INTO politica_backup (tipo_backup, frecuencia, retencion, descripcion) VALUES
('Backup Base Completo', 'Semanal (Domingo 02:00)', '4 semanas', 'pg_basebackup completo del cluster'),
('WAL Archivado', 'Continuo', '7 dias', 'Segmentos WAL copiados a /backup/wal/'),
('Backup Logico', 'Diario (03:00)', '7 dias', 'pg_dump de tablas criticas'),
('Snapshot Almacenamiento', 'Cada 6 horas', '48 horas', 'Snapshot a nivel de almacenamiento');

SELECT 'Politica de backup implementada' AS info;
SELECT tipo_backup, frecuencia, retencion FROM politica_backup;

-- ========================================
-- 7. FUNCION DE RECUPERACION POINT-IN-TIME
-- ========================================

SELECT '=== PROCEDIMIENTO PITR ===' AS seccion;

-- Documentar procedimiento PITR
DROP TABLE IF EXISTS procedimiento_pitr;
CREATE TABLE procedimiento_pitr (
    paso INTEGER PRIMARY KEY,
    accion VARCHAR(100),
    comando TEXT,
    notas TEXT
);

INSERT INTO procedimiento_pitr VALUES
(1, 'Detener PostgreSQL', 'pg_ctl stop -D $PGDATA', 'Asegurar que no hay conexiones activas'),
(2, 'Respaldar datos actuales', 'mv $PGDATA $PGDATA.failed', 'Preservar para analisis forense'),
(3, 'Restaurar backup base', 'tar -xf backup_base.tar -C $PGDATA', 'Usar backup mas reciente anterior al fallo'),
(4, 'Crear recovery.signal', 'touch $PGDATA/recovery.signal', 'Indica a PostgreSQL modo recuperacion'),
(5, 'Configurar recuperacion', 'Editar postgresql.conf', 'restore_command y recovery_target_time'),
(6, 'Iniciar PostgreSQL', 'pg_ctl start -D $PGDATA', 'Inicia replay de WAL hasta target_time'),
(7, 'Verificar recuperacion', 'SELECT pg_is_in_recovery();', 'Debe retornar false al completar');

SELECT 'Procedimiento de recuperacion PITR' AS info;
SELECT paso, accion, comando FROM procedimiento_pitr ORDER BY paso;

-- ========================================
-- 8. VERIFICACION DE INTEGRIDAD
-- ========================================

SELECT '=== VERIFICACION DE INTEGRIDAD ===' AS seccion;

-- Verificar consistencia de datos despues de operaciones
SELECT 'Verificacion de integridad de datos' AS info;

-- Contar registros en tablas principales
SELECT 'Producto' AS tabla, COUNT(*) AS registros FROM Producto
UNION ALL
SELECT 'Pedido', COUNT(*) FROM Pedido
UNION ALL
SELECT 'Comprobante', COUNT(*) FROM Comprobante
UNION ALL
SELECT 'Cliente', COUNT(*) FROM Cliente;

-- Verificar que no hay transacciones huerfanas
SELECT 'Pedidos sin comprobante (integridad referencial)' AS verificacion;
SELECT COUNT(*) AS pedidos_sin_comprobante
FROM Pedido p
LEFT JOIN Comprobante c ON p.id_pedido = c.id_pedido
WHERE c.id_comprobante IS NULL
  AND p.fecha_pedido < CURRENT_DATE; -- Solo pedidos antiguos

-- Verificar consistencia de stock
SELECT 'Productos con stock negativo (anomalia)' AS verificacion;
SELECT COUNT(*) AS productos_stock_negativo
FROM Producto WHERE stock < 0;

-- ========================================
-- 9. ESTADISTICAS WAL
-- ========================================

SELECT '=== ESTADISTICAS WAL ===' AS seccion;

-- Posicion actual en WAL
SELECT 'Posicion actual en WAL' AS info;
SELECT pg_current_wal_lsn() AS wal_lsn_actual,
       pg_walfile_name(pg_current_wal_lsn()) AS archivo_wal_actual;

-- Tamano WAL generado (aproximado)
SELECT 'Actividad WAL reciente' AS info;
SELECT 
    pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') / (1024*1024) AS wal_total_mb,
    (SELECT setting FROM pg_settings WHERE name = 'wal_segment_size')::bigint / (1024*1024) AS segmento_mb;

-- ========================================
-- 10. LIMPIEZA Y RESTAURACION
-- ========================================

SELECT '=== RESTAURACION PARA SIGUIENTE FASE ===' AS seccion;

-- Restaurar stock a valores normales
UPDATE Producto SET stock = 100 WHERE id_producto IN (1, 2, 3);

-- Resumen final
SELECT 'Resumen de objetos creados en Fase 4' AS info;
SELECT 'log_transacciones' AS objeto, 'Tabla' AS tipo, 'Registro de ciclo de vida' AS proposito
UNION ALL
SELECT 'log_undo_redo', 'Tabla', 'Simulacion de log para recuperacion'
UNION ALL
SELECT 'politica_backup', 'Tabla', 'Documentacion de politica de respaldos'
UNION ALL
SELECT 'procedimiento_pitr', 'Tabla', 'Pasos para recuperacion PITR'
UNION ALL
SELECT 'registrar_ciclo_transaccion', 'Funcion', 'Registra ciclo de vida transaccional'
UNION ALL
SELECT 'compra_con_log_recuperacion', 'Funcion', 'Compra con logging tipo WAL';

SELECT '=== FASE 4 COMPLETADA ===' AS resultado;
