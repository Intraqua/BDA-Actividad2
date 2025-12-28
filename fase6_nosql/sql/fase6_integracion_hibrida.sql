-- ============================================================
-- FASE 6: INTEGRACIÓN HÍBRIDA PostgreSQL - MongoDB
-- Sistema de Marketplace Digital
-- Bases de Datos Avanzadas - UNIPRO
-- Autor: David Valbuena Segura
-- ============================================================

-- ============================================================
-- 1. TABLA DE COLA DE SINCRONIZACIÓN
-- ============================================================
-- Esta tabla actúa como intermediario para registrar todos los
-- cambios que deben propagarse a MongoDB. Un proceso externo
-- (aplicación, cron job, etc.) lee esta cola y actualiza MongoDB.

DROP TABLE IF EXISTS sync_queue CASCADE;

CREATE TABLE sync_queue (
    id SERIAL PRIMARY KEY,
    tabla_origen VARCHAR(50) NOT NULL,           -- Tabla PostgreSQL modificada
    coleccion_destino VARCHAR(50) NOT NULL,      -- Colección MongoDB destino
    operacion VARCHAR(10) NOT NULL               -- INSERT, UPDATE, DELETE
        CHECK (operacion IN ('INSERT', 'UPDATE', 'DELETE')),
    id_registro INTEGER NOT NULL,                -- ID del registro afectado
    datos JSONB,                                 -- Datos en formato JSON
    procesado BOOLEAN DEFAULT FALSE,             -- ¿Ya se sincronizó?
    fecha_creacion TIMESTAMP DEFAULT NOW(),      -- Cuándo se registró el cambio
    fecha_procesado TIMESTAMP,                   -- Cuándo se sincronizó
    intentos INTEGER DEFAULT 0,                  -- Reintentos en caso de fallo
    error_mensaje TEXT                           -- Mensaje de error si falló
);

-- Índices para optimizar consultas de la cola
CREATE INDEX idx_sync_queue_pendientes ON sync_queue (procesado, fecha_creacion) 
    WHERE procesado = FALSE;
CREATE INDEX idx_sync_queue_tabla ON sync_queue (tabla_origen);

COMMENT ON TABLE sync_queue IS 'Cola de sincronización para propagar cambios a MongoDB';

-- ============================================================
-- 2. FUNCIÓN GENÉRICA DE CAPTURA DE CAMBIOS
-- ============================================================
-- Esta función se ejecuta con cada trigger y registra el cambio
-- en la cola de sincronización.

CREATE OR REPLACE FUNCTION fn_capturar_cambio()
RETURNS TRIGGER AS $$
DECLARE
    v_coleccion VARCHAR(50);
    v_datos JSONB;
    v_id INTEGER;
BEGIN
    -- Determinar colección destino según la tabla
    CASE TG_TABLE_NAME
        WHEN 'cliente' THEN v_coleccion := 'perfiles_usuario';
        WHEN 'producto' THEN v_coleccion := 'catalogo_productos';
        WHEN 'valoracion' THEN v_coleccion := 'reviews_extendidas';
        ELSE v_coleccion := TG_TABLE_NAME;
    END CASE;
    
    -- Según la operación, capturar datos apropiados
    IF TG_OP = 'DELETE' THEN
        v_datos := row_to_json(OLD)::JSONB;
        -- Obtener ID según la tabla
        CASE TG_TABLE_NAME
            WHEN 'cliente' THEN v_id := OLD.id_cliente;
            WHEN 'producto' THEN v_id := OLD.id_producto;
            WHEN 'valoracion' THEN v_id := OLD.id_valoracion;
            ELSE v_id := 0;
        END CASE;
    ELSE
        v_datos := row_to_json(NEW)::JSONB;
        CASE TG_TABLE_NAME
            WHEN 'cliente' THEN v_id := NEW.id_cliente;
            WHEN 'producto' THEN v_id := NEW.id_producto;
            WHEN 'valoracion' THEN v_id := NEW.id_valoracion;
            ELSE v_id := 0;
        END CASE;
    END IF;
    
    -- Insertar en la cola de sincronización
    INSERT INTO sync_queue (tabla_origen, coleccion_destino, operacion, id_registro, datos)
    VALUES (TG_TABLE_NAME, v_coleccion, TG_OP, v_id, v_datos);
    
    -- Retornar el registro apropiado
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION fn_capturar_cambio() IS 'Función genérica para capturar cambios y encolarlos para sincronización con MongoDB';

-- ============================================================
-- 3. TRIGGERS EN TABLAS RELEVANTES
-- ============================================================

-- Trigger para tabla Cliente
DROP TRIGGER IF EXISTS tr_sync_cliente ON Cliente;
CREATE TRIGGER tr_sync_cliente
    AFTER INSERT OR UPDATE OR DELETE ON Cliente
    FOR EACH ROW
    EXECUTE FUNCTION fn_capturar_cambio();

-- Trigger para tabla Producto
DROP TRIGGER IF EXISTS tr_sync_producto ON Producto;
CREATE TRIGGER tr_sync_producto
    AFTER INSERT OR UPDATE OR DELETE ON Producto
    FOR EACH ROW
    EXECUTE FUNCTION fn_capturar_cambio();

-- ============================================================
-- 4. FUNCIONES DE GESTIÓN DE LA COLA
-- ============================================================

-- Función para obtener cambios pendientes
CREATE OR REPLACE FUNCTION fn_obtener_pendientes(p_limite INTEGER DEFAULT 100)
RETURNS TABLE (
    id INTEGER,
    tabla_origen VARCHAR(50),
    coleccion_destino VARCHAR(50),
    operacion VARCHAR(10),
    id_registro INTEGER,
    datos JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        sq.id,
        sq.tabla_origen,
        sq.coleccion_destino,
        sq.operacion,
        sq.id_registro,
        sq.datos
    FROM sync_queue sq
    WHERE sq.procesado = FALSE
    ORDER BY sq.fecha_creacion ASC
    LIMIT p_limite;
END;
$$ LANGUAGE plpgsql;

-- Función para marcar como procesado
CREATE OR REPLACE FUNCTION fn_marcar_procesado(p_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE sync_queue
    SET procesado = TRUE,
        fecha_procesado = NOW()
    WHERE id = p_id;
END;
$$ LANGUAGE plpgsql;

-- Función para marcar error
CREATE OR REPLACE FUNCTION fn_marcar_error(p_id INTEGER, p_mensaje TEXT)
RETURNS VOID AS $$
BEGIN
    UPDATE sync_queue
    SET intentos = intentos + 1,
        error_mensaje = p_mensaje
    WHERE id = p_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 5. VISTA DE ESTADO DE SINCRONIZACIÓN
-- ============================================================

CREATE OR REPLACE VIEW v_estado_sincronizacion AS
SELECT 
    tabla_origen,
    coleccion_destino,
    operacion,
    COUNT(*) FILTER (WHERE procesado = FALSE) AS pendientes,
    COUNT(*) FILTER (WHERE procesado = TRUE) AS procesados,
    COUNT(*) FILTER (WHERE intentos > 0 AND procesado = FALSE) AS con_errores,
    MAX(fecha_creacion) FILTER (WHERE procesado = FALSE) AS ultimo_pendiente,
    MAX(fecha_procesado) AS ultima_sincronizacion
FROM sync_queue
GROUP BY tabla_origen, coleccion_destino, operacion
ORDER BY tabla_origen, operacion;

COMMENT ON VIEW v_estado_sincronizacion IS 'Resumen del estado de sincronización con MongoDB';

-- ============================================================
-- 6. FUNCIÓN DE CONSULTA HÍBRIDA (EJEMPLO)
-- ============================================================
-- Esta función demuestra cómo una aplicación consultaría datos
-- combinando PostgreSQL (transaccional) con MongoDB (perfil extendido)

CREATE OR REPLACE FUNCTION fn_info_cliente_hibrida(p_id_cliente INTEGER)
RETURNS TABLE (
    id_cliente INTEGER,
    nombre VARCHAR,
    total_pedidos BIGINT,
    total_gastado NUMERIC,
    ultimo_pedido TIMESTAMP,
    mongodb_usuario_id INTEGER,
    instrucciones TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id_cliente,
        c.nombre_cliente,
        COUNT(p.id_pedido)::BIGINT AS total_pedidos,
        COALESCE(SUM(p.cantidad * pr.precio), 0) AS total_gastado,
        MAX(p.fecha_pedido)::TIMESTAMP AS ultimo_pedido,
        c.id_cliente AS mongodb_usuario_id,
        'Consultar MongoDB: db.perfiles_usuario.findOne({usuario_id: ' || c.id_cliente || '}) para obtener preferencias'::TEXT AS instrucciones
    FROM Cliente c
    LEFT JOIN Pedido p ON c.id_cliente = p.id_cliente
    LEFT JOIN Producto pr ON p.id_producto = pr.id_producto
    WHERE c.id_cliente = p_id_cliente
    GROUP BY c.id_cliente, c.nombre_cliente;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION fn_info_cliente_hibrida(INTEGER) IS 
'Ejemplo de consulta híbrida: datos transaccionales de PostgreSQL + referencia a MongoDB';

-- ============================================================
-- 7. PROCEDIMIENTO DE LIMPIEZA
-- ============================================================

CREATE OR REPLACE FUNCTION fn_limpiar_cola_antigua(p_dias INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    v_eliminados INTEGER;
BEGIN
    DELETE FROM sync_queue
    WHERE procesado = TRUE
    AND fecha_procesado < NOW() - (p_dias || ' days')::INTERVAL;
    
    GET DIAGNOSTICS v_eliminados = ROW_COUNT;
    
    RETURN v_eliminados;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION fn_limpiar_cola_antigua(INTEGER) IS 
'Elimina registros procesados con más de N días de antigüedad';

-- ============================================================
-- 8. VERIFICACIÓN DE LA INSTALACIÓN
-- ============================================================

-- Mostrar triggers creados
SELECT 
    trigger_name,
    event_object_table AS tabla,
    action_timing || ' ' || event_manipulation AS evento
FROM information_schema.triggers
WHERE trigger_schema = 'public'
AND trigger_name LIKE 'tr_sync%'
ORDER BY event_object_table;

-- Mostrar estructura de sync_queue
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'sync_queue'
ORDER BY ordinal_position;

-- ============================================================
-- FIN DEL SCRIPT DE INTEGRACIÓN HÍBRIDA
-- ============================================================
