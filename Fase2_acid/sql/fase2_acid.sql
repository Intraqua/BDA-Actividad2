-- ========================================
-- ACTIVIDAD 2 - FASE 2: PROPIEDADES ACID
-- Procesamiento Transaccional
-- Autor: David Valbuena Segura
-- Universidad: UNIPRO
-- ========================================

-- ========================================
-- 1. EXTENSIÓN DEL MODELO DE DATOS
-- ========================================

-- Conectar a la base de datos de la Fase 1
\c marketplace_indices;

-- Añadir columna stock a Producto
ALTER TABLE Producto ADD COLUMN IF NOT EXISTS stock INTEGER DEFAULT 100;
ALTER TABLE Producto ADD CONSTRAINT chk_stock_positivo CHECK (stock >= 0);

-- Crear tabla Comprobante
DROP TABLE IF EXISTS Comprobante CASCADE;
CREATE TABLE Comprobante (
    id_comprobante SERIAL PRIMARY KEY,
    id_pedido INTEGER NOT NULL REFERENCES Pedido(id_pedido),
    fecha_emision TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total DECIMAL(12,2) NOT NULL CHECK (total > 0)
);

-- Índice para búsquedas por pedido
CREATE INDEX idx_comprobante_pedido ON Comprobante(id_pedido);

-- ========================================
-- 2. PREPARACIÓN DE DATOS DE PRUEBA
-- ========================================

-- Asegurar que hay productos con stock suficiente para pruebas
UPDATE Producto SET stock = 100 WHERE id_producto = 1;
UPDATE Producto SET stock = 5 WHERE id_producto = 2;  -- Stock limitado para prueba de fallo

-- Verificar estado inicial
SELECT 'Estado inicial de productos' AS info;
SELECT id_producto, nombre, precio, stock 
FROM Producto 
WHERE id_producto IN (1, 2);

-- ========================================
-- 3. ESCENARIO 1: TRANSACCIÓN EXITOSA
-- ========================================

SELECT '=== ESCENARIO 1: TRANSACCIÓN EXITOSA ===' AS escenario;

-- Guardar estado antes de la transacción
SELECT 'Stock antes de compra' AS info, stock FROM Producto WHERE id_producto = 1;

BEGIN;
    -- Paso 1: Registrar el pedido
    INSERT INTO Pedido (id_producto, id_cliente, fecha_pedido, cantidad)
    VALUES (1, 1, CURRENT_DATE, 2)
    RETURNING id_pedido, id_producto, cantidad;
    
    -- Paso 2: Descontar stock del producto
    UPDATE Producto 
    SET stock = stock - 2
    WHERE id_producto = 1 AND stock >= 2
    RETURNING id_producto, nombre, stock AS stock_resultante;
    
    -- Paso 3: Generar comprobante vinculado
    INSERT INTO Comprobante (id_pedido, total)
    SELECT 
        currval('pedido_id_pedido_seq'),
        precio * 2
    FROM Producto 
    WHERE id_producto = 1
    RETURNING id_comprobante, id_pedido, total;
COMMIT;

-- Verificar resultado exitoso
SELECT 'Verificación post-transacción exitosa' AS info;
SELECT 'Pedido creado' AS resultado, p.id_pedido, p.cantidad, pr.nombre
FROM Pedido p
JOIN Producto pr ON p.id_producto = pr.id_producto
WHERE p.id_pedido = (SELECT MAX(id_pedido) FROM Pedido);

SELECT 'Comprobante generado' AS resultado, c.id_comprobante, c.id_pedido, c.total
FROM Comprobante c
WHERE c.id_pedido = (SELECT MAX(id_pedido) FROM Pedido);

SELECT 'Stock actualizado' AS resultado, id_producto, nombre, stock
FROM Producto WHERE id_producto = 1;

-- ========================================
-- 4. ESCENARIO 2: FALLO CON ROLLBACK
-- ========================================

SELECT '=== ESCENARIO 2: FALLO POR STOCK INSUFICIENTE ===' AS escenario;

-- Estado antes del intento
SELECT 'Stock antes de intento fallido' AS info, 
       id_producto, nombre, stock 
FROM Producto WHERE id_producto = 2;

-- Contar pedidos antes del intento
SELECT 'Pedidos antes del intento' AS info, COUNT(*) AS total FROM Pedido;

BEGIN;
    -- Paso 1: Registrar el pedido (se ejecuta)
    INSERT INTO Pedido (id_producto, id_cliente, fecha_pedido, cantidad)
    VALUES (2, 2, CURRENT_DATE, 50);  -- 50 unidades cuando solo hay 5
    
    -- Paso 2: Intentar descontar stock (fallará la condición)
    UPDATE Producto 
    SET stock = stock - 50
    WHERE id_producto = 2 AND stock >= 50;  -- Condición NO se cumple
    
    -- En una aplicación real, aquí verificaríamos ROW_COUNT
    -- y haríamos ROLLBACK si es 0. Simulamos el fallo:
ROLLBACK;  -- Reversión completa

-- Verificar que NO se persistió nada
SELECT 'Verificación post-rollback' AS info;
SELECT 'Pedidos después del rollback' AS resultado, COUNT(*) AS total FROM Pedido;
SELECT 'Stock sin cambios' AS resultado, id_producto, nombre, stock
FROM Producto WHERE id_producto = 2;

-- ========================================
-- 5. FUNCIÓN TRANSACCIONAL COMPLETA
-- ========================================

-- Función que implementa la transacción con manejo de errores
CREATE OR REPLACE FUNCTION realizar_compra(
    p_id_producto INTEGER,
    p_id_cliente INTEGER,
    p_cantidad INTEGER
) RETURNS TABLE(
    exito BOOLEAN,
    mensaje TEXT,
    id_pedido_nuevo INTEGER,
    id_comprobante_nuevo INTEGER
) AS $$
DECLARE
    v_id_pedido INTEGER;
    v_id_comprobante INTEGER;
    v_precio DECIMAL(10,2);
    v_stock INTEGER;
    v_total DECIMAL(12,2);
    v_filas_actualizadas INTEGER;
BEGIN
    -- Verificar stock disponible con bloqueo pesimista
    SELECT stock, precio INTO v_stock, v_precio
    FROM Producto
    WHERE id_producto = p_id_producto
    FOR UPDATE;  -- Bloqueo para evitar condiciones de carrera
    
    -- Validación: producto existe
    IF v_stock IS NULL THEN
        RETURN QUERY SELECT FALSE, 
            'ERROR: Producto no encontrado'::TEXT, 
            NULL::INTEGER, 
            NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Validación: stock suficiente
    IF v_stock < p_cantidad THEN
        RETURN QUERY SELECT FALSE, 
            format('ERROR: Stock insuficiente. Disponible: %s, Solicitado: %s', v_stock, p_cantidad)::TEXT,
            NULL::INTEGER, 
            NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Calcular total
    v_total := v_precio * p_cantidad;
    
    -- Paso 1: Crear pedido
    INSERT INTO Pedido (id_producto, id_cliente, fecha_pedido, cantidad)
    VALUES (p_id_producto, p_id_cliente, CURRENT_DATE, p_cantidad)
    RETURNING id_pedido INTO v_id_pedido;
    
    -- Paso 2: Descontar stock
    UPDATE Producto
    SET stock = stock - p_cantidad
    WHERE id_producto = p_id_producto;
    
    GET DIAGNOSTICS v_filas_actualizadas = ROW_COUNT;
    
    IF v_filas_actualizadas = 0 THEN
        RAISE EXCEPTION 'Error al actualizar stock';
    END IF;
    
    -- Paso 3: Crear comprobante
    INSERT INTO Comprobante (id_pedido, total)
    VALUES (v_id_pedido, v_total)
    RETURNING id_comprobante INTO v_id_comprobante;
    
    -- Retornar éxito
    RETURN QUERY SELECT TRUE, 
        'Compra realizada exitosamente'::TEXT, 
        v_id_pedido, 
        v_id_comprobante;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- 6. PRUEBAS DE LA FUNCIÓN
-- ========================================

SELECT '=== PRUEBAS DE FUNCIÓN TRANSACCIONAL ===' AS pruebas;

-- Preparar producto para pruebas
UPDATE Producto SET stock = 20 WHERE id_producto = 3;

-- Prueba exitosa
SELECT 'Prueba 1: Compra válida' AS test;
SELECT * FROM realizar_compra(3, 1, 5);

-- Prueba con stock insuficiente
SELECT 'Prueba 2: Stock insuficiente' AS test;
SELECT * FROM realizar_compra(2, 1, 100);

-- Prueba con producto inexistente
SELECT 'Prueba 3: Producto inexistente' AS test;
SELECT * FROM realizar_compra(99999, 1, 1);

-- ========================================
-- 7. VERIFICACIÓN DE PROPIEDADES ACID
-- ========================================

SELECT '=== VERIFICACIÓN DE PROPIEDADES ACID ===' AS verificacion;

-- ATOMICIDAD: Verificar que transacciones fallidas no dejan rastro
SELECT 'ATOMICIDAD: Pedidos con cantidad > stock inicial' AS propiedad;
SELECT COUNT(*) AS pedidos_invalidos 
FROM Pedido p
JOIN Producto pr ON p.id_producto = pr.id_producto
WHERE p.cantidad > 100;  -- Ninguno debería existir

-- CONSISTENCIA: No hay referencias huérfanas
SELECT 'CONSISTENCIA: Comprobantes sin pedido válido' AS propiedad;
SELECT COUNT(*) AS huerfanos
FROM Comprobante c
LEFT JOIN Pedido p ON c.id_pedido = p.id_pedido
WHERE p.id_pedido IS NULL;

-- CONSISTENCIA: No hay stock negativo
SELECT 'CONSISTENCIA: Productos con stock negativo' AS propiedad;
SELECT COUNT(*) AS stock_negativo
FROM Producto
WHERE stock < 0;

-- AISLAMIENTO: Nivel actual
SELECT 'AISLAMIENTO: Nivel de aislamiento' AS propiedad;
SHOW transaction_isolation;

-- DURABILIDAD: Los datos confirmados existen
SELECT 'DURABILIDAD: Comprobantes emitidos' AS propiedad;
SELECT COUNT(*) AS comprobantes_totales FROM Comprobante;

-- ========================================
-- 8. RESUMEN FINAL
-- ========================================

SELECT '=== RESUMEN DE DATOS ===' AS resumen;

SELECT 'Total de pedidos' AS metrica, COUNT(*) AS valor FROM Pedido
UNION ALL
SELECT 'Total de comprobantes', COUNT(*) FROM Comprobante
UNION ALL
SELECT 'Productos con stock > 0', COUNT(*) FROM Producto WHERE stock > 0;

-- Últimos pedidos con sus comprobantes
SELECT 'Últimos pedidos con comprobantes' AS info;
SELECT 
    p.id_pedido,
    p.id_producto,
    p.cantidad,
    p.fecha_pedido,
    c.id_comprobante,
    c.total,
    c.fecha_emision
FROM Pedido p
LEFT JOIN Comprobante c ON p.id_pedido = c.id_pedido
ORDER BY p.id_pedido DESC
LIMIT 5;
