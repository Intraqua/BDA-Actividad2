-- ============================================================
-- ACTIVIDAD 2 - FASE 1: VERIFICACIÓN DE ÍNDICES
-- Sistema de Marketplace Digital
-- Bases de Datos Avanzadas - UNIPRO
-- Autor: David Valbuena Segura
-- ============================================================

-- ============================================================
-- 1. CONSULTAS FRECUENTES CON EXPLAIN ANALYZE
-- ============================================================

-- Consulta 1: Productos por nombre (búsqueda parcial)
-- Debe usar idx_producto_nombre con Index Scan
\echo '=== Q1: Búsqueda parcial por nombre ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id_producto, nombre, precio
FROM Producto
WHERE nombre LIKE 'Producto Premium%'
ORDER BY nombre;

-- Consulta 2: Productos por rango de precios
-- Debe usar idx_producto_precio con Index Scan o Bitmap Index Scan
\echo '=== Q2: Rango de precios ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id_producto, nombre, precio
FROM Producto
WHERE precio BETWEEN 50 AND 150
ORDER BY precio;

-- Consulta 3: Productos por categoría
-- Debe usar idx_producto_categoria
\echo '=== Q3: Filtro por categoría ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT p.id_producto, p.nombre, p.precio, c.nombre_categoria
FROM Producto p
JOIN Categoria c ON p.id_categoria = c.id_categoria
WHERE p.id_categoria = 6
ORDER BY p.precio;

-- Consulta 4: Pedidos por cliente
-- Debe usar idx_pedido_cliente
\echo '=== Q4: Pedidos por cliente ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id_pedido, id_producto, fecha_pedido, cantidad
FROM Pedido
WHERE id_cliente = 100
ORDER BY fecha_pedido DESC;

-- ============================================================
-- 2. VERIFICACIÓN DE ÍNDICES COMPUESTOS
-- ============================================================

-- Consulta 5: Categoría + Precio (usa índice compuesto)
\echo '=== Q5: Índice compuesto categoría + precio ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT nombre, precio
FROM Producto
WHERE id_categoria = 9 AND precio BETWEEN 100 AND 300
ORDER BY precio;

-- Consulta 6: Cliente + Fecha (usa índice compuesto)
\echo '=== Q6: Índice compuesto cliente + fecha ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id_pedido, fecha_pedido, cantidad
FROM Pedido
WHERE id_cliente = 50
ORDER BY fecha_pedido DESC
LIMIT 10;

-- Consulta 7: Vendedor + Categoría (usa índice compuesto)
\echo '=== Q7: Índice compuesto vendedor + categoría ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COUNT(*) AS total, AVG(precio) AS precio_medio
FROM Producto
WHERE id_vendedor = 3 AND id_categoria = 6;

-- ============================================================
-- 3. VERIFICACIÓN DEL CLUSTERING
-- ============================================================

-- Comprobar correlación del clustering por categoría
\echo '=== Correlación de clustering ==='
SELECT 
    schemaname,
    tablename,
    attname AS columna,
    correlation AS correlacion
FROM pg_stats
WHERE tablename = 'producto' 
AND attname = 'id_categoria';

-- ============================================================
-- 4. COMPARATIVA CON Y SIN ÍNDICES
-- ============================================================

-- Desactivar índices temporalmente
\echo '=== Comparativa SIN índices ==='
SET enable_indexscan = off;
SET enable_bitmapscan = off;

EXPLAIN (ANALYZE, COSTS, FORMAT TEXT)
SELECT nombre, precio
FROM Producto
WHERE id_categoria = 6
ORDER BY precio;

-- Reactivar índices
\echo '=== Comparativa CON índices ==='
SET enable_indexscan = on;
SET enable_bitmapscan = on;

EXPLAIN (ANALYZE, COSTS, FORMAT TEXT)
SELECT nombre, precio
FROM Producto
WHERE id_categoria = 6
ORDER BY precio;

-- ============================================================
-- 5. ESTADÍSTICAS DE USO DE ÍNDICES
-- ============================================================

\echo '=== Estadísticas de uso de índices ==='
SELECT 
    schemaname,
    relname AS tabla,
    indexrelname AS indice,
    idx_scan AS escaneos,
    idx_tup_read AS tuplas_leidas,
    idx_tup_fetch AS tuplas_obtenidas
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- ============================================================
-- 6. TAMAÑO DE ÍNDICES
-- ============================================================

\echo '=== Tamaño de índices ==='
SELECT 
    tablename AS tabla,
    indexname AS indice,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS tamanio
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY pg_relation_size(indexname::regclass) DESC;

-- ============================================================
-- 7. ÍNDICES NO UTILIZADOS
-- ============================================================

\echo '=== Índices no utilizados (candidatos a eliminar) ==='
SELECT 
    relname AS tabla,
    indexrelname AS indice,
    idx_scan AS escaneos,
    pg_size_pretty(pg_relation_size(indexrelid)) AS tamanio
FROM pg_stat_user_indexes
WHERE idx_scan = 0
AND schemaname = 'public'
ORDER BY pg_relation_size(indexrelid) DESC;

-- ============================================================
-- 8. RESUMEN DE ÍNDICES CREADOS
-- ============================================================

\echo '=== Resumen de índices creados ==='
SELECT 
    tablename AS tabla,
    indexname AS indice,
    indexdef AS definicion
FROM pg_indexes 
WHERE schemaname = 'public'
AND indexname NOT LIKE '%_pkey'
ORDER BY tablename, indexname;
