-- ========================================
-- ACTIVIDAD 2 - FASE 3: CONCURRENCIA Y AISLAMIENTO
-- Control de Transacciones Concurrentes
-- Autor: David Valbuena Segura
-- Universidad: UNIPRO
-- ========================================

-- ========================================
-- 1. PREPARACION DEL ENTORNO
-- ========================================

-- Conectar a la base de datos
\c marketplace_indices;

-- Verificar nivel de aislamiento actual
SELECT '=== CONFIGURACION INICIAL ===' AS seccion;
SHOW transaction_isolation;
SHOW default_transaction_isolation;

-- Preparar producto de prueba con stock limitado
UPDATE Producto SET stock = 5 WHERE id_producto = 1;

SELECT 'Estado inicial del producto de prueba' AS info;
SELECT id_producto, nombre, stock, precio
FROM Producto 
WHERE id_producto = 1;

-- ========================================
-- 2. DEMOSTRACION DE NIVELES DE AISLAMIENTO
-- ========================================

-- Nota: Las simulaciones de concurrencia requieren dos sesiones separadas.
-- Este script documenta los comandos a ejecutar en cada sesion.

SELECT '=== NIVEL READ COMMITTED ===' AS seccion;

-- SESION A: Ejecutar primero
-- BEGIN;
-- SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
-- SELECT stock FROM Producto WHERE id_producto = 1;
-- -- Resultado: 5
-- -- PAUSA: Esperar a que Sesion B actualice

-- SESION B: Ejecutar durante pausa de A
-- BEGIN;
-- UPDATE Producto SET stock = 2 WHERE id_producto = 1;
-- COMMIT;

-- SESION A: Continuar
-- SELECT stock FROM Producto WHERE id_producto = 1;
-- -- Resultado: 2 (CAMBIO - Non-repeatable read)
-- COMMIT;

-- Restaurar stock para siguiente prueba
UPDATE Producto SET stock = 5 WHERE id_producto = 1;

SELECT '=== NIVEL REPEATABLE READ ===' AS seccion;

-- SESION A: Ejecutar primero
-- BEGIN;
-- SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
-- SELECT stock FROM Producto WHERE id_producto = 1;
-- -- Resultado: 5
-- -- PAUSA: Esperar a que Sesion B actualice

-- SESION B: Ejecutar durante pausa de A
-- BEGIN;
-- UPDATE Producto SET stock = 2 WHERE id_producto = 1;
-- COMMIT;

-- SESION A: Continuar
-- SELECT stock FROM Producto WHERE id_producto = 1;
-- -- Resultado: 5 (CONSISTENTE - snapshot isolation)
-- UPDATE Producto SET stock = stock - 3 WHERE id_producto = 1;
-- -- ERROR: could not serialize access due to concurrent update
-- ROLLBACK;

-- Restaurar stock
UPDATE Producto SET stock = 5 WHERE id_producto = 1;

SELECT '=== NIVEL SERIALIZABLE ===' AS seccion;

-- Similar a REPEATABLE READ pero con deteccion ampliada de conflictos
-- mediante Serializable Snapshot Isolation (SSI)

-- ========================================
-- 3. SIMULACION AUTOMATIZADA DE CONFLICTO
-- ========================================

SELECT '=== SIMULACION DE CONFLICTO (Escenario Controlado) ===' AS seccion;

-- Crear tabla de log para registrar resultados de pruebas
DROP TABLE IF EXISTS log_concurrencia;
CREATE TABLE log_concurrencia (
    id SERIAL PRIMARY KEY,
    sesion VARCHAR(20),
    operacion VARCHAR(100),
    resultado TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Funcion para simular compra con logging
CREATE OR REPLACE FUNCTION simular_compra_log(
    p_sesion VARCHAR(20),
    p_id_producto INTEGER,
    p_cantidad INTEGER
) RETURNS TEXT AS $$
DECLARE
    v_stock INTEGER;
    v_resultado TEXT;
BEGIN
    -- Leer stock actual
    SELECT stock INTO v_stock FROM Producto WHERE id_producto = p_id_producto;
    
    INSERT INTO log_concurrencia (sesion, operacion, resultado)
    VALUES (p_sesion, 'SELECT stock', format('Stock leido: %s', v_stock));
    
    IF v_stock >= p_cantidad THEN
        UPDATE Producto SET stock = stock - p_cantidad WHERE id_producto = p_id_producto;
        v_resultado := format('EXITO: Compra de %s unidades realizada', p_cantidad);
    ELSE
        v_resultado := format('FALLO: Stock insuficiente (%s < %s)', v_stock, p_cantidad);
    END IF;
    
    INSERT INTO log_concurrencia (sesion, operacion, resultado)
    VALUES (p_sesion, 'UPDATE stock', v_resultado);
    
    RETURN v_resultado;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- 4. ESTRATEGIA DE BLOQUEO PESIMISTA
-- ========================================

SELECT '=== FUNCION CON BLOQUEO PESIMISTA ===' AS seccion;

-- Funcion de compra segura con SELECT FOR UPDATE
CREATE OR REPLACE FUNCTION comprar_producto_seguro(
    p_id_producto INTEGER,
    p_id_cliente INTEGER,
    p_cantidad INTEGER
) RETURNS TABLE(
    exito BOOLEAN,
    mensaje TEXT,
    stock_restante INTEGER
) AS $$
DECLARE
    v_stock INTEGER;
    v_precio DECIMAL(10,2);
    v_id_pedido INTEGER;
BEGIN
    -- BLOQUEO PESIMISTA: Adquiere bloqueo exclusivo en la fila
    SELECT stock, precio INTO v_stock, v_precio
    FROM Producto
    WHERE id_producto = p_id_producto
    FOR UPDATE;  -- Otras transacciones esperan aqui
    
    -- Validacion: Producto existe
    IF v_stock IS NULL THEN
        RETURN QUERY SELECT FALSE, 
            'ERROR: Producto no encontrado'::TEXT,
            NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Validacion: Stock suficiente
    IF v_stock < p_cantidad THEN
        RETURN QUERY SELECT FALSE, 
            format('ERROR: Stock insuficiente. Disponible: %s, Solicitado: %s', v_stock, p_cantidad)::TEXT,
            v_stock;
        RETURN;
    END IF;
    
    -- PASO 1: Descontar stock
    UPDATE Producto 
    SET stock = stock - p_cantidad 
    WHERE id_producto = p_id_producto;
    
    -- PASO 2: Crear pedido
    INSERT INTO Pedido (id_producto, id_cliente, fecha_pedido, cantidad)
    VALUES (p_id_producto, p_id_cliente, CURRENT_DATE, p_cantidad)
    RETURNING id_pedido INTO v_id_pedido;
    
    -- PASO 3: Crear comprobante
    INSERT INTO Comprobante (id_pedido, total)
    VALUES (v_id_pedido, v_precio * p_cantidad);
    
    -- Retornar exito con stock restante
    RETURN QUERY SELECT TRUE, 
        format('Compra realizada. Pedido: %s', v_id_pedido)::TEXT,
        (v_stock - p_cantidad);
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- 5. PRUEBAS DE LA FUNCION SEGURA
-- ========================================

SELECT '=== PRUEBAS DE COMPRA SEGURA ===' AS seccion;

-- Restaurar stock para pruebas
UPDATE Producto SET stock = 5 WHERE id_producto = 1;

-- Prueba 1: Compra exitosa
SELECT 'Prueba 1: Compra valida (3 de 5 unidades)' AS test;
SELECT * FROM comprar_producto_seguro(1, 1, 3);

-- Verificar estado
SELECT 'Stock despues de compra exitosa' AS info;
SELECT id_producto, nombre, stock FROM Producto WHERE id_producto = 1;

-- Prueba 2: Segunda compra con stock insuficiente
SELECT 'Prueba 2: Compra excesiva (3 de 2 restantes)' AS test;
SELECT * FROM comprar_producto_seguro(1, 2, 3);

-- Prueba 3: Compra que agota stock
SELECT 'Prueba 3: Compra que agota stock (2 de 2)' AS test;
SELECT * FROM comprar_producto_seguro(1, 2, 2);

-- Verificar estado final
SELECT 'Stock despues de todas las pruebas' AS info;
SELECT id_producto, nombre, stock FROM Producto WHERE id_producto = 1;

-- ========================================
-- 6. BLOQUEO OPTIMISTA (ALTERNATIVA)
-- ========================================

SELECT '=== BLOQUEO OPTIMISTA (Referencia) ===' AS seccion;

-- Agregar columna de version para bloqueo optimista
ALTER TABLE Producto ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1;

-- Funcion de compra con bloqueo optimista
CREATE OR REPLACE FUNCTION comprar_producto_optimista(
    p_id_producto INTEGER,
    p_id_cliente INTEGER,
    p_cantidad INTEGER,
    p_version_esperada INTEGER
) RETURNS TABLE(
    exito BOOLEAN,
    mensaje TEXT,
    nueva_version INTEGER
) AS $$
DECLARE
    v_filas_actualizadas INTEGER;
BEGIN
    -- Intentar actualizar solo si la version coincide
    UPDATE Producto 
    SET stock = stock - p_cantidad,
        version = version + 1
    WHERE id_producto = p_id_producto 
      AND stock >= p_cantidad
      AND version = p_version_esperada;
    
    GET DIAGNOSTICS v_filas_actualizadas = ROW_COUNT;
    
    IF v_filas_actualizadas = 0 THEN
        -- Conflicto detectado: version cambio o stock insuficiente
        RETURN QUERY SELECT FALSE, 
            'CONFLICTO: Datos modificados por otra transaccion. Reintentar.'::TEXT,
            NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Crear pedido
    INSERT INTO Pedido (id_producto, id_cliente, fecha_pedido, cantidad)
    VALUES (p_id_producto, p_id_cliente, CURRENT_DATE, p_cantidad);
    
    -- Retornar nueva version
    RETURN QUERY SELECT TRUE, 
        'Compra realizada con bloqueo optimista'::TEXT,
        p_version_esperada + 1;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- 7. COMPARACION DE ESTRATEGIAS
-- ========================================

SELECT '=== COMPARACION DE ESTRATEGIAS ===' AS seccion;

-- Tabla comparativa
SELECT 'Estrategia' AS estrategia, 'Ventaja' AS ventaja, 'Desventaja' AS desventaja
UNION ALL
SELECT 'Pesimista (FOR UPDATE)', 
       'Previene conflictos', 
       'Reduce concurrencia'
UNION ALL
SELECT 'Optimista (Version)', 
       'Mayor concurrencia', 
       'Requiere reintentos'
UNION ALL
SELECT 'SERIALIZABLE', 
       'Maxima seguridad', 
       'Errores frecuentes';

-- ========================================
-- 8. INSTRUCCIONES PARA PRUEBA MANUAL
-- ========================================

SELECT '=== INSTRUCCIONES PARA PRUEBA MANUAL ===' AS seccion;

-- Para probar concurrencia real, abrir dos terminales psql:
--
-- TERMINAL 1:
-- \c marketplace_indices
-- BEGIN;
-- SELECT * FROM comprar_producto_seguro(1, 1, 3);
-- -- NO hacer COMMIT todavia
--
-- TERMINAL 2 (simultaneamente):
-- \c marketplace_indices
-- BEGIN;
-- SELECT * FROM comprar_producto_seguro(1, 2, 3);
-- -- Esta sesion ESPERARA hasta que Terminal 1 haga COMMIT o ROLLBACK
--
-- TERMINAL 1:
-- COMMIT;
--
-- TERMINAL 2:
-- -- Ahora se ejecuta y deberia fallar por stock insuficiente

-- ========================================
-- 9. VERIFICACION FINAL
-- ========================================

SELECT '=== VERIFICACION FINAL ===' AS seccion;

-- Restaurar datos para siguiente fase
UPDATE Producto SET stock = 100 WHERE id_producto = 1;
UPDATE Producto SET stock = 100 WHERE id_producto = 2;

-- Verificar funciones creadas
SELECT 'Funciones de concurrencia creadas' AS info;
SELECT proname, prorettype::regtype 
FROM pg_proc 
WHERE proname IN ('comprar_producto_seguro', 'comprar_producto_optimista', 'simular_compra_log');

-- Resumen de pedidos generados en pruebas
SELECT 'Pedidos generados en pruebas de concurrencia' AS info;
SELECT COUNT(*) AS total_pedidos FROM Pedido;

-- Log de concurrencia (si existe)
SELECT 'Log de simulaciones' AS info;
SELECT * FROM log_concurrencia ORDER BY timestamp DESC LIMIT 10;
