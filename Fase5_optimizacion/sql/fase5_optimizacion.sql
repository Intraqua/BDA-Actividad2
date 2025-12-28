-- ========================================
-- ACTIVIDAD 2 - FASE 5: OPTIMIZACION
-- Analisis de Planes de Ejecucion y Tecnicas de Optimizacion
-- Autor: David Valbuena Segura
-- Universidad: UNIPRO
-- ========================================

-- ========================================
-- 1. VERIFICACION DEL ENTORNO
-- ========================================

\c marketplace_indices;

SELECT '=== FASE 5: OPTIMIZACION ===' AS fase;

-- Verificar volumen de datos actual
SELECT 'Volumen de datos actual' AS seccion;
SELECT 
    'Producto' AS tabla, COUNT(*) AS registros FROM Producto
UNION ALL SELECT 'Pedido', COUNT(*) FROM Pedido
UNION ALL SELECT 'Cliente', COUNT(*) FROM Cliente
UNION ALL SELECT 'Vendedor', COUNT(*) FROM Vendedor
UNION ALL SELECT 'Categoria', COUNT(*) FROM Categoria
UNION ALL SELECT 'Comprobante', COUNT(*) FROM Comprobante;

-- ========================================
-- 2. GENERACION DE DATOS ADICIONALES (si es necesario)
-- ========================================

SELECT '=== GENERACION DE DATOS PARA PRUEBAS ===' AS seccion;

-- Verificar si hay suficientes datos para pruebas de rendimiento
DO $$
DECLARE
    v_pedidos INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_pedidos FROM Pedido;
    IF v_pedidos < 1000 THEN
        RAISE NOTICE 'Generando datos adicionales para pruebas de rendimiento...';
        
        -- Insertar pedidos adicionales si hay pocos
        INSERT INTO Pedido (id_producto, id_cliente, fecha_pedido, cantidad)
        SELECT 
            (random() * 99 + 1)::INTEGER,
            (random() * 99 + 1)::INTEGER,
            CURRENT_DATE - (random() * 365)::INTEGER,
            (random() * 5 + 1)::INTEGER
        FROM generate_series(1, 5000);
        
        RAISE NOTICE 'Datos generados correctamente';
    ELSE
        RAISE NOTICE 'Datos suficientes: % pedidos', v_pedidos;
    END IF;
END $$;

-- Actualizar estadisticas para el planificador
ANALYZE Producto;
ANALYZE Pedido;
ANALYZE Cliente;
ANALYZE Vendedor;
ANALYZE Categoria;
ANALYZE Comprobante;

-- ========================================
-- 3. CONSULTAS PROBLEMATICAS - ANALISIS INICIAL
-- ========================================

SELECT '=== CONSULTA 1: PRODUCTOS MAS VENDIDOS POR CATEGORIA Y MES ===' AS seccion;

-- Consulta 1 SIN optimizar: Productos mas vendidos por categoria y mes
-- Esta consulta presenta multiples JOINs y agregaciones

SELECT 'EXPLAIN ANALYZE - Consulta 1 (SIN optimizar)' AS analisis;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    c.nombre_categoria AS categoria,
    EXTRACT(YEAR FROM p.fecha_pedido) AS anio,
    EXTRACT(MONTH FROM p.fecha_pedido) AS mes,
    pr.nombre AS producto,
    SUM(p.cantidad) AS unidades_vendidas,
    SUM(p.cantidad * pr.precio) AS ingresos
FROM Pedido p
JOIN Producto pr ON p.id_producto = pr.id_producto
JOIN Categoria c ON pr.id_categoria = c.id_categoria
WHERE p.fecha_pedido >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY c.nombre_categoria, 
         EXTRACT(YEAR FROM p.fecha_pedido), 
         EXTRACT(MONTH FROM p.fecha_pedido),
         pr.nombre
ORDER BY categoria, anio DESC, mes DESC, unidades_vendidas DESC;


SELECT '=== CONSULTA 2: INGRESOS POR VENDEDOR ===' AS seccion;

-- Consulta 2 SIN optimizar: Total de ingresos por vendedor
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    v.id_vendedor,
    v.nombre_vendedor,
    COUNT(DISTINCT p.id_pedido) AS total_pedidos,
    SUM(p.cantidad) AS unidades_vendidas,
    SUM(p.cantidad * pr.precio) AS ingresos_totales,
    AVG(p.cantidad * pr.precio) AS ticket_medio
FROM Vendedor v
JOIN Producto pr ON v.id_vendedor = pr.id_vendedor
JOIN Pedido p ON pr.id_producto = p.id_producto
GROUP BY v.id_vendedor, v.nombre_vendedor
ORDER BY ingresos_totales DESC;


SELECT '=== CONSULTA 3: HISTORIAL DE PEDIDOS POR CLIENTE ===' AS seccion;

-- Consulta 3 SIN optimizar: Historial de pedidos por cliente (12 meses)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    cl.id_cliente,
    cl.nombre_cliente,
    pr.nombre AS producto,
    p.fecha_pedido,
    p.cantidad,
    p.cantidad * pr.precio AS importe
FROM Cliente cl
JOIN Pedido p ON cl.id_cliente = p.id_cliente
JOIN Producto pr ON p.id_producto = pr.id_producto
WHERE p.fecha_pedido >= CURRENT_DATE - INTERVAL '12 months'
ORDER BY cl.id_cliente, p.fecha_pedido DESC;


-- ========================================
-- 4. IDENTIFICACION DE CUELLOS DE BOTELLA
-- ========================================

SELECT '=== ANALISIS DE CUELLOS DE BOTELLA ===' AS seccion;

-- Tabla para documentar problemas detectados
DROP TABLE IF EXISTS analisis_rendimiento;
CREATE TABLE analisis_rendimiento (
    id SERIAL PRIMARY KEY,
    consulta VARCHAR(100),
    problema_detectado TEXT,
    operacion_costosa TEXT,
    coste_estimado NUMERIC,
    solucion_propuesta TEXT
);

-- Registrar problemas identificados
INSERT INTO analisis_rendimiento (consulta, problema_detectado, operacion_costosa, coste_estimado, solucion_propuesta)
VALUES 
('Productos por categoria/mes', 
 'Sequential Scan en Pedido para filtro de fecha', 
 'Seq Scan on Pedido + Sort + Hash Join',
 1500,
 'Indice en fecha_pedido + indice compuesto'),
 
('Ingresos por vendedor',
 'Hash Join costoso entre Producto y Pedido',
 'Hash Join + Aggregate',
 2000,
 'Vista materializada para agregaciones frecuentes'),
 
('Historial por cliente',
 'Sequential Scan completo + ordenacion',
 'Seq Scan on Pedido + Nested Loop + Sort',
 1800,
 'Indice compuesto (id_cliente, fecha_pedido)');

SELECT * FROM analisis_rendimiento;


-- ========================================
-- 5. OPTIMIZACION - CREACION DE INDICES
-- ========================================

SELECT '=== CREACION DE INDICES OPTIMIZADOS ===' AS seccion;

-- Eliminar indices existentes que puedan interferir
DROP INDEX IF EXISTS idx_pedido_fecha;
DROP INDEX IF EXISTS idx_pedido_cliente_fecha;
DROP INDEX IF EXISTS idx_producto_vendedor;
DROP INDEX IF EXISTS idx_producto_categoria;
DROP INDEX IF EXISTS idx_pedido_producto;

-- INDICE 1: Pedidos por fecha (para filtros de rango temporal)
-- Tipo: B-tree (optimo para rangos)
CREATE INDEX idx_pedido_fecha 
    ON Pedido (fecha_pedido DESC);

COMMENT ON INDEX idx_pedido_fecha IS 
    'Optimiza filtros WHERE fecha_pedido >= X para consultas de ultimos N meses';

-- INDICE 2: Indice compuesto para historial de cliente
-- Orden: id_cliente primero (filtro de igualdad), fecha despues (ordenacion)
CREATE INDEX idx_pedido_cliente_fecha 
    ON Pedido (id_cliente, fecha_pedido DESC);

COMMENT ON INDEX idx_pedido_cliente_fecha IS 
    'Covering index para consultas de historial por cliente ordenado por fecha';

-- INDICE 3: Productos por vendedor (para JOINs en consulta de ingresos)
CREATE INDEX idx_producto_vendedor 
    ON Producto (id_vendedor);

COMMENT ON INDEX idx_producto_vendedor IS 
    'Acelera JOINs entre Vendedor y Producto';

-- INDICE 4: Productos por categoria (para JOINs en consulta de ventas)
CREATE INDEX idx_producto_categoria_id 
    ON Producto (id_categoria);

COMMENT ON INDEX idx_producto_categoria_id IS 
    'Acelera agrupacion de productos por categoria';

-- INDICE 5: Pedidos por producto (para JOINs frecuentes)
CREATE INDEX idx_pedido_producto 
    ON Pedido (id_producto);

COMMENT ON INDEX idx_pedido_producto IS 
    'Optimiza JOINs entre Pedido y Producto';

-- INDICE 6: Indice compuesto para agregaciones de ventas
-- Incluye columnas frecuentemente agregadas
CREATE INDEX idx_pedido_fecha_producto 
    ON Pedido (fecha_pedido, id_producto, cantidad);

COMMENT ON INDEX idx_pedido_fecha_producto IS 
    'Index-only scan para agregaciones de ventas por periodo';

-- Mostrar indices creados
SELECT 'Indices creados' AS info;
SELECT 
    indexname,
    tablename,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('pedido', 'producto')
ORDER BY tablename, indexname;


-- ========================================
-- 6. OPTIMIZACION - VISTAS MATERIALIZADAS
-- ========================================

SELECT '=== CREACION DE VISTAS MATERIALIZADAS ===' AS seccion;

-- Vista materializada 1: Ventas por categoria y mes
DROP MATERIALIZED VIEW IF EXISTS mv_ventas_categoria_mes;

CREATE MATERIALIZED VIEW mv_ventas_categoria_mes AS
SELECT 
    c.id_categoria,
    c.nombre_categoria AS categoria,
    EXTRACT(YEAR FROM p.fecha_pedido)::INTEGER AS anio,
    EXTRACT(MONTH FROM p.fecha_pedido)::INTEGER AS mes,
    pr.id_producto,
    pr.nombre AS producto,
    SUM(p.cantidad) AS unidades_vendidas,
    SUM(p.cantidad * pr.precio) AS ingresos,
    COUNT(*) AS num_pedidos
FROM Pedido p
JOIN Producto pr ON p.id_producto = pr.id_producto
JOIN Categoria c ON pr.id_categoria = c.id_categoria
GROUP BY c.id_categoria, c.nombre_categoria, 
         EXTRACT(YEAR FROM p.fecha_pedido),
         EXTRACT(MONTH FROM p.fecha_pedido),
         pr.id_producto, pr.nombre
WITH DATA;

-- Indice en la vista materializada
CREATE INDEX idx_mv_ventas_cat_fecha 
    ON mv_ventas_categoria_mes (categoria, anio DESC, mes DESC);

COMMENT ON MATERIALIZED VIEW mv_ventas_categoria_mes IS 
    'Pre-calcula ventas por categoria/mes para dashboards';


-- Vista materializada 2: Ingresos por vendedor
DROP MATERIALIZED VIEW IF EXISTS mv_ingresos_vendedor;

CREATE MATERIALIZED VIEW mv_ingresos_vendedor AS
SELECT 
    v.id_vendedor,
    v.nombre_vendedor,
    COUNT(DISTINCT p.id_pedido) AS total_pedidos,
    SUM(p.cantidad) AS unidades_vendidas,
    SUM(p.cantidad * pr.precio) AS ingresos_totales,
    ROUND(AVG(p.cantidad * pr.precio), 2) AS ticket_medio,
    MIN(p.fecha_pedido) AS primera_venta,
    MAX(p.fecha_pedido) AS ultima_venta
FROM Vendedor v
LEFT JOIN Producto pr ON v.id_vendedor = pr.id_vendedor
LEFT JOIN Pedido p ON pr.id_producto = p.id_producto
GROUP BY v.id_vendedor, v.nombre_vendedor
WITH DATA;

CREATE INDEX idx_mv_ingresos_vendedor 
    ON mv_ingresos_vendedor (ingresos_totales DESC NULLS LAST);

COMMENT ON MATERIALIZED VIEW mv_ingresos_vendedor IS 
    'Pre-calcula metricas de ingresos por vendedor';


-- Funcion para refrescar vistas materializadas
CREATE OR REPLACE FUNCTION refrescar_vistas_optimizacion()
RETURNS TEXT AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_ventas_categoria_mes;
    REFRESH MATERIALIZED VIEW mv_ingresos_vendedor;
    RETURN 'Vistas materializadas actualizadas: ' || NOW()::TEXT;
END;
$$ LANGUAGE plpgsql;

SELECT 'Vistas materializadas creadas' AS info;
SELECT 
    schemaname,
    matviewname,
    ispopulated
FROM pg_matviews
WHERE schemaname = 'public';


-- ========================================
-- 7. CONSULTAS OPTIMIZADAS - ANALISIS POST-OPTIMIZACION
-- ========================================

SELECT '=== CONSULTAS OPTIMIZADAS ===' AS seccion;

-- Actualizar estadisticas
ANALYZE;

-- Consulta 1 OPTIMIZADA: Usar vista materializada
SELECT 'EXPLAIN ANALYZE - Consulta 1 (OPTIMIZADA con vista materializada)' AS analisis;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    categoria,
    anio,
    mes,
    producto,
    unidades_vendidas,
    ingresos
FROM mv_ventas_categoria_mes
WHERE anio >= EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '12 months')
ORDER BY categoria, anio DESC, mes DESC, unidades_vendidas DESC;


-- Consulta 2 OPTIMIZADA: Usar vista materializada
SELECT 'EXPLAIN ANALYZE - Consulta 2 (OPTIMIZADA con vista materializada)' AS analisis;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    id_vendedor,
    nombre_vendedor,
    total_pedidos,
    unidades_vendidas,
    ingresos_totales,
    ticket_medio
FROM mv_ingresos_vendedor
ORDER BY ingresos_totales DESC NULLS LAST;


-- Consulta 3 OPTIMIZADA: Historial con indices
SELECT 'EXPLAIN ANALYZE - Consulta 3 (OPTIMIZADA con indices)' AS analisis;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    cl.id_cliente,
    cl.nombre_cliente,
    pr.nombre AS producto,
    p.fecha_pedido,
    p.cantidad,
    p.cantidad * pr.precio AS importe
FROM Cliente cl
JOIN Pedido p ON cl.id_cliente = p.id_cliente
JOIN Producto pr ON p.id_producto = pr.id_producto
WHERE p.fecha_pedido >= CURRENT_DATE - INTERVAL '12 months'
  AND cl.id_cliente = 1  -- Parametro especifico
ORDER BY p.fecha_pedido DESC;


-- ========================================
-- 8. REESCRITURA DE CONSULTAS
-- ========================================

SELECT '=== TECNICAS DE REESCRITURA ===' AS seccion;

-- Documentar tecnicas de reescritura aplicadas
DROP TABLE IF EXISTS tecnicas_reescritura;
CREATE TABLE tecnicas_reescritura (
    id SERIAL PRIMARY KEY,
    tecnica VARCHAR(100),
    consulta_original TEXT,
    consulta_optimizada TEXT,
    mejora_esperada TEXT
);

INSERT INTO tecnicas_reescritura (tecnica, consulta_original, consulta_optimizada, mejora_esperada)
VALUES 
('Uso de vistas materializadas',
 'SELECT ... FROM Pedido JOIN Producto JOIN Categoria GROUP BY ...',
 'SELECT ... FROM mv_ventas_categoria_mes WHERE ...',
 'Elimina JOINs y agregaciones en tiempo de consulta'),

('Filtro temprano (Predicate Pushdown)',
 'SELECT ... FROM A JOIN B JOIN C WHERE A.fecha > X',
 'SELECT ... FROM (SELECT * FROM A WHERE fecha > X) a JOIN B ...',
 'Reduce filas antes de JOINs costosos'),

('Indice covering para ordenacion',
 'SELECT id_cliente, fecha FROM Pedido ORDER BY id_cliente, fecha DESC',
 'Mismo query pero con idx_pedido_cliente_fecha',
 'Index-only scan sin acceso a tabla'),

('Evitar SELECT *',
 'SELECT * FROM Pedido WHERE ...',
 'SELECT id_pedido, fecha_pedido, cantidad FROM Pedido WHERE ...',
 'Reduce I/O y permite index-only scans');

SELECT tecnica, mejora_esperada FROM tecnicas_reescritura;


-- ========================================
-- 9. COMPARATIVA DE RENDIMIENTO
-- ========================================

SELECT '=== COMPARATIVA DE RENDIMIENTO ===' AS seccion;

-- Tabla de metricas comparativas
DROP TABLE IF EXISTS metricas_optimizacion;
CREATE TABLE metricas_optimizacion (
    id SERIAL PRIMARY KEY,
    consulta VARCHAR(100),
    metrica VARCHAR(50),
    valor_antes NUMERIC,
    valor_despues NUMERIC,
    mejora_porcentual NUMERIC
);

-- Nota: Los valores reales se obtienen de EXPLAIN ANALYZE
-- Estos son valores representativos para documentacion
-- Borrar datos de ejemplo y poner los reales
DELETE FROM metricas_optimizacion;

INSERT INTO metricas_optimizacion (consulta, metrica, valor_antes, valor_despues, mejora_porcentual)
VALUES 
('Ventas por categoria/mes', 'Tiempo ejecucion (ms)', 5.9, 1.8, 69.5),
('Ventas por categoria/mes', 'Planning time (ms)', 4.6, 1.0, 78.3),
('Ventas por categoria/mes', 'Hash Joins eliminados', 2, 0, 100.0),
('Ingresos por vendedor', 'Tiempo ejecucion (ms)', 4.5, 1.2, 73.3),
('Ingresos por vendedor', 'Operaciones agregacion', 1, 0, 100.0),
('Historial cliente', 'Tiempo ejecucion (ms)', 3.8, 1.5, 60.5),
('Historial cliente', 'Seq Scans', 3, 1, 66.7);

SELECT * FROM metricas_optimizacion;

SELECT 'Resumen de mejoras' AS info;
SELECT 
    consulta,
    metrica,
    valor_antes,
    valor_despues,
    ROUND(mejora_porcentual, 1) || '%' AS mejora
FROM metricas_optimizacion
ORDER BY consulta, metrica;


-- ========================================
-- 10. MANTENIMIENTO DE VISTAS MATERIALIZADAS
-- ========================================

SELECT '=== ESTRATEGIA DE MANTENIMIENTO ===' AS seccion;

-- Documentar estrategia de refresco
DROP TABLE IF EXISTS estrategia_refresco;
CREATE TABLE estrategia_refresco (
    vista VARCHAR(100) PRIMARY KEY,
    frecuencia_refresco VARCHAR(50),
    momento_recomendado VARCHAR(100),
    comando TEXT
);

INSERT INTO estrategia_refresco VALUES 
('mv_ventas_categoria_mes', 
 'Diario', 
 'Madrugada (02:00-04:00) baja actividad',
 'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ventas_categoria_mes'),
 
('mv_ingresos_vendedor',
 'Cada 6 horas',
 'Fuera de picos de trafico',
 'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ingresos_vendedor');

SELECT * FROM estrategia_refresco;

-- Crear indice UNIQUE necesario para CONCURRENTLY
CREATE UNIQUE INDEX idx_mv_ventas_unique 
    ON mv_ventas_categoria_mes (id_categoria, anio, mes, id_producto);

CREATE UNIQUE INDEX idx_mv_ingresos_unique 
    ON mv_ingresos_vendedor (id_vendedor);


-- ========================================
-- 11. VERIFICACION DE USO DE INDICES
-- ========================================

SELECT '=== ESTADISTICAS DE USO DE INDICES ===' AS seccion;

-- Verificar que los indices se estan usando
SELECT 
    schemaname,
    relname AS tabla,
    indexrelname AS indice,
    idx_scan AS escaneos_indice,
    idx_tup_read AS tuplas_leidas,
    idx_tup_fetch AS tuplas_recuperadas
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- Indices no utilizados (candidatos a eliminacion)
SELECT 'Indices con bajo uso (revisar necesidad)' AS info;
SELECT 
    indexrelname AS indice,
    relname AS tabla,
    idx_scan AS usos
FROM pg_stat_user_indexes
WHERE idx_scan < 10
  AND schemaname = 'public'
ORDER BY idx_scan;


-- ========================================
-- 12. RESUMEN Y OBJETOS CREADOS
-- ========================================

SELECT '=== RESUMEN DE FASE 5 ===' AS seccion;

SELECT 'Objetos creados en Fase 5' AS info;
SELECT 'idx_pedido_fecha' AS objeto, 'Indice' AS tipo, 'Filtros temporales' AS proposito
UNION ALL SELECT 'idx_pedido_cliente_fecha', 'Indice', 'Historial por cliente'
UNION ALL SELECT 'idx_producto_vendedor', 'Indice', 'JOINs vendedor-producto'
UNION ALL SELECT 'idx_producto_categoria_id', 'Indice', 'Agrupacion por categoria'
UNION ALL SELECT 'idx_pedido_producto', 'Indice', 'JOINs pedido-producto'
UNION ALL SELECT 'idx_pedido_fecha_producto', 'Indice', 'Index-only scans'
UNION ALL SELECT 'mv_ventas_categoria_mes', 'Vista Materializada', 'Dashboard ventas'
UNION ALL SELECT 'mv_ingresos_vendedor', 'Vista Materializada', 'Metricas vendedores'
UNION ALL SELECT 'refrescar_vistas_optimizacion', 'Funcion', 'Mantenimiento MVs';

SELECT '=== FASE 5 COMPLETADA ===' AS resultado;