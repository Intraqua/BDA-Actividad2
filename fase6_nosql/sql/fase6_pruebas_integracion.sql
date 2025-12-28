-- ============================================================
-- FASE 6: PRUEBAS DE INTEGRACIÓN HÍBRIDA
-- Sistema de Marketplace Digital
-- Bases de Datos Avanzadas - UNIPRO
-- Autor: David Valbuena Segura
-- Ejecutar DESPUÉS del script fase6_integracion_hibrida.sql
-- ============================================================

-- ============================================================
-- PRUEBA 1: Verificar que sync_queue está vacía inicialmente
-- ============================================================
SELECT 'PRUEBA 1: Estado inicial de la cola' AS test;
SELECT COUNT(*) AS registros_en_cola FROM sync_queue;

-- ============================================================
-- PRUEBA 2: INSERT - Insertar un cliente de prueba
-- ============================================================
SELECT 'PRUEBA 2: INSERT de cliente' AS test;

INSERT INTO Cliente (nombre_cliente)
VALUES ('Cliente Prueba Sincronización');

-- Verificar que se registró en la cola
SELECT 
    id,
    tabla_origen,
    coleccion_destino,
    operacion,
    id_registro,
    datos->>'nombre_cliente' AS nombre,
    procesado,
    fecha_creacion
FROM sync_queue 
WHERE tabla_origen = 'cliente'
ORDER BY id DESC 
LIMIT 1;

-- ============================================================
-- PRUEBA 3: UPDATE - Modificar el cliente
-- ============================================================
SELECT 'PRUEBA 3: UPDATE de cliente' AS test;

UPDATE Cliente 
SET nombre_cliente = 'Cliente Prueba MODIFICADO'
WHERE nombre_cliente = 'Cliente Prueba Sincronización';

-- Verificar que se registró el UPDATE
SELECT 
    id,
    operacion,
    datos->>'nombre_cliente' AS nombre_nuevo,
    fecha_creacion
FROM sync_queue 
WHERE tabla_origen = 'cliente'
ORDER BY id DESC 
LIMIT 2;

-- ============================================================
-- PRUEBA 4: DELETE - Eliminar el cliente de prueba
-- ============================================================
SELECT 'PRUEBA 4: DELETE de cliente' AS test;

DELETE FROM Cliente 
WHERE nombre_cliente = 'Cliente Prueba MODIFICADO';

-- Verificar que se registró el DELETE
SELECT 
    id,
    operacion,
    datos->>'nombre_cliente' AS nombre_eliminado,
    fecha_creacion
FROM sync_queue 
WHERE tabla_origen = 'cliente'
ORDER BY id DESC 
LIMIT 3;

-- ============================================================
-- PRUEBA 5: Probar con Producto
-- ============================================================
SELECT 'PRUEBA 5: INSERT de producto' AS test;

INSERT INTO Producto (nombre, precio, stock, id_categoria, id_vendedor)
VALUES ('Producto Prueba Sync', 99.99, 10, 1, 1);

-- Verificar en la cola
SELECT 
    id,
    tabla_origen,
    coleccion_destino,
    operacion,
    datos->>'nombre' AS producto,
    datos->>'precio' AS precio
FROM sync_queue 
WHERE tabla_origen = 'producto'
ORDER BY id DESC 
LIMIT 1;

-- Limpiar producto de prueba
DELETE FROM Producto WHERE nombre = 'Producto Prueba Sync';

-- ============================================================
-- PRUEBA 6: Vista de estado de sincronización
-- ============================================================
SELECT 'PRUEBA 6: Estado de sincronización' AS test;
SELECT * FROM v_estado_sincronizacion;

-- ============================================================
-- PRUEBA 7: Obtener pendientes (simula lo que haría la app)
-- ============================================================
SELECT 'PRUEBA 7: Obtener pendientes para procesar' AS test;
SELECT * FROM fn_obtener_pendientes(10);

-- ============================================================
-- PRUEBA 8: Marcar como procesado (simula sincronización exitosa)
-- ============================================================
SELECT 'PRUEBA 8: Marcar registros como procesados' AS test;

-- Marcar todos los pendientes como procesados
UPDATE sync_queue SET procesado = TRUE, fecha_procesado = NOW() WHERE procesado = FALSE;

-- Verificar estado final
SELECT 
    COUNT(*) FILTER (WHERE procesado = FALSE) AS pendientes,
    COUNT(*) FILTER (WHERE procesado = TRUE) AS procesados
FROM sync_queue;

-- ============================================================
-- PRUEBA 9: Consulta híbrida de ejemplo
-- ============================================================
SELECT 'PRUEBA 9: Consulta híbrida' AS test;
SELECT * FROM fn_info_cliente_hibrida(1);

-- ============================================================
-- RESUMEN FINAL
-- ============================================================
SELECT 'RESUMEN: Registros en cola de sincronización' AS test;
SELECT 
    tabla_origen,
    operacion,
    COUNT(*) AS total,
    SUM(CASE WHEN procesado THEN 1 ELSE 0 END) AS procesados
FROM sync_queue
GROUP BY tabla_origen, operacion
ORDER BY tabla_origen, operacion;

-- ============================================================
-- LIMPIAR DATOS DE PRUEBA (OPCIONAL)
-- ============================================================
-- DELETE FROM sync_queue WHERE datos->>'nombre_cliente' LIKE '%Prueba%';
-- DELETE FROM sync_queue WHERE datos->>'nombre' = 'Producto Prueba Sync';
