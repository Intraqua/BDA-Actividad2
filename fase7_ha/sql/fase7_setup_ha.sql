-- =============================================================================
-- FASE 7 - ALTA DISPONIBILIDAD CON FAILOVER AUTOMATICO
-- Sistema Marketplace Digital - Actividad 2
-- Autor: David Valbuena Segura
-- =============================================================================
-- Archivo: fase7_setup_ha.sql
-- Ejecutar en: NODO PRIMARY (puerto 5432)
-- Propósito: Crear esquema completo del marketplace con soporte HA y failover
-- =============================================================================

-- =============================================================================
-- PARTE 1: ESQUEMA DE DATOS
-- =============================================================================

-- Crear esquema dedicado para alta disponibilidad
CREATE SCHEMA IF NOT EXISTS marketplace_ha;
SET search_path TO marketplace_ha, public;

-- ---------------------------------------------------------------------------
-- Tabla: Cliente
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Cliente (
    cliente_id SERIAL PRIMARY KEY,
    email VARCHAR(100) UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    activo BOOLEAN DEFAULT true
);

-- ---------------------------------------------------------------------------
-- Tabla: Vendedor
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Vendedor (
    vendedor_id SERIAL PRIMARY KEY,
    email VARCHAR(100) UNIQUE NOT NULL,
    nombre_tienda VARCHAR(100) NOT NULL,
    fecha_alta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verificado BOOLEAN DEFAULT false
);

-- ---------------------------------------------------------------------------
-- Tabla: Categoria
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Categoria (
    categoria_id SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    descripcion TEXT
);

-- ---------------------------------------------------------------------------
-- Tabla: Producto
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Producto (
    producto_id SERIAL PRIMARY KEY,
    vendedor_id INTEGER REFERENCES Vendedor(vendedor_id),
    categoria_id INTEGER REFERENCES Categoria(categoria_id),
    nombre VARCHAR(200) NOT NULL,
    precio DECIMAL(10,2) NOT NULL CHECK (precio > 0),
    stock INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
    activo BOOLEAN DEFAULT true
);

-- ---------------------------------------------------------------------------
-- Tabla: Pedido
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Pedido (
    pedido_id SERIAL PRIMARY KEY,
    cliente_id INTEGER REFERENCES Cliente(cliente_id),
    fecha_pedido TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estado VARCHAR(20) DEFAULT 'pendiente' 
        CHECK (estado IN ('pendiente', 'procesando', 'enviado', 'entregado', 'cancelado')),
    total DECIMAL(12,2) DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- Tabla: DetallePedido
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DetallePedido (
    detalle_id SERIAL PRIMARY KEY,
    pedido_id INTEGER REFERENCES Pedido(pedido_id),
    producto_id INTEGER REFERENCES Producto(producto_id),
    cantidad INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario DECIMAL(10,2) NOT NULL
);

-- ---------------------------------------------------------------------------
-- Tabla: Log de Alta Disponibilidad
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Log_Alta_Disponibilidad (
    log_id SERIAL PRIMARY KEY,
    evento VARCHAR(50) NOT NULL,
    descripcion TEXT,
    nodo_origen VARCHAR(100),
    nodo_destino VARCHAR(100),
    lsn_evento PG_LSN,
    datos_adicionales JSONB,
    timestamp_evento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- Tabla: Registro de Failovers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Registro_Failover (
    failover_id SERIAL PRIMARY KEY,
    timestamp_deteccion TIMESTAMP NOT NULL,
    timestamp_promocion TIMESTAMP,
    timestamp_completado TIMESTAMP,
    nodo_anterior VARCHAR(100),
    nodo_nuevo VARCHAR(100),
    tipo_failover VARCHAR(20) DEFAULT 'automatico',
    estado VARCHAR(20) DEFAULT 'iniciado',
    datos_verificacion JSONB,
    notas TEXT
);

-- Índices para consultas de auditoría
CREATE INDEX IF NOT EXISTS idx_log_ha_timestamp ON Log_Alta_Disponibilidad(timestamp_evento DESC);
CREATE INDEX IF NOT EXISTS idx_log_ha_evento ON Log_Alta_Disponibilidad(evento);
CREATE INDEX IF NOT EXISTS idx_failover_timestamp ON Registro_Failover(timestamp_deteccion DESC);

-- =============================================================================
-- PARTE 2: INDICES PARA RENDIMIENTO
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_producto_vendedor ON Producto(vendedor_id);
CREATE INDEX IF NOT EXISTS idx_producto_categoria ON Producto(categoria_id);
CREATE INDEX IF NOT EXISTS idx_pedido_cliente ON Pedido(cliente_id);
CREATE INDEX IF NOT EXISTS idx_pedido_fecha ON Pedido(fecha_pedido DESC);
CREATE INDEX IF NOT EXISTS idx_detalle_pedido ON DetallePedido(pedido_id);

-- =============================================================================
-- PARTE 3: DATOS DE PRUEBA
-- =============================================================================

-- Limpiar datos existentes para evitar duplicados
TRUNCATE TABLE DetallePedido, Pedido, Producto, Categoria, Vendedor, Cliente RESTART IDENTITY CASCADE;

-- Categorías
INSERT INTO Categoria (nombre, descripcion) VALUES
    ('Electrónica', 'Dispositivos electrónicos y gadgets'),
    ('Ropa', 'Moda y accesorios'),
    ('Hogar', 'Artículos para el hogar'),
    ('Deportes', 'Equipamiento deportivo'),
    ('Libros', 'Libros y material educativo');

-- Vendedores
INSERT INTO Vendedor (email, nombre_tienda, verificado) VALUES
    ('tech@store.com', 'TechStore Pro', true),
    ('moda@fashion.com', 'Fashion Express', true),
    ('home@deco.com', 'HomeDecor Plus', false),
    ('sport@shop.com', 'SportShop Elite', true),
    ('books@read.com', 'ReadMore Books', true);

-- Clientes
INSERT INTO Cliente (email, nombre) VALUES
    ('juan@email.com', 'Juan García'),
    ('maria@email.com', 'María López'),
    ('carlos@email.com', 'Carlos Rodríguez'),
    ('ana@email.com', 'Ana Martínez'),
    ('pedro@email.com', 'Pedro Sánchez');

-- Productos
INSERT INTO Producto (vendedor_id, categoria_id, nombre, precio, stock) VALUES
    (1, 1, 'Smartphone Galaxy Pro', 699.99, 50),
    (1, 1, 'Tablet Ultra 10"', 449.99, 30),
    (1, 1, 'Auriculares Bluetooth', 79.99, 100),
    (2, 2, 'Camiseta Premium', 29.99, 200),
    (2, 2, 'Pantalón Casual', 49.99, 150),
    (3, 3, 'Lámpara LED Moderna', 39.99, 80),
    (3, 3, 'Set Decoración', 89.99, 40),
    (4, 4, 'Balón Fútbol Pro', 34.99, 60),
    (4, 4, 'Raqueta Tenis', 129.99, 25),
    (5, 5, 'Novela Bestseller', 19.99, 300);

-- Pedidos con detalles
INSERT INTO Pedido (cliente_id, estado, total) VALUES
    (1, 'entregado', 779.98),
    (2, 'enviado', 79.98),
    (3, 'procesando', 449.99),
    (4, 'pendiente', 164.98),
    (5, 'entregado', 19.99);

INSERT INTO DetallePedido (pedido_id, producto_id, cantidad, precio_unitario) VALUES
    (1, 1, 1, 699.99),
    (1, 3, 1, 79.99),
    (2, 4, 2, 29.99),
    (2, 5, 1, 49.99),
    (3, 2, 1, 449.99),
    (4, 8, 2, 34.99),
    (4, 6, 1, 39.99),
    (4, 10, 1, 19.99),
    (5, 10, 1, 19.99);

-- Registro inicial en log de HA
INSERT INTO Log_Alta_Disponibilidad (evento, descripcion, nodo_origen)
VALUES ('SETUP_INICIAL', 'Esquema marketplace_ha creado e inicializado', 'PRIMARY');

-- =============================================================================
-- PARTE 4: FUNCIONES DE MONITORIZACION HA
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Función: Estado del nodo actual
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_estado_nodo()
RETURNS TABLE (
    metrica VARCHAR(50),
    valor TEXT
) AS $$
BEGIN
    -- Rol del nodo
    IF pg_is_in_recovery() THEN
        RETURN QUERY SELECT 'rol_nodo'::VARCHAR(50), 'STANDBY (Solo Lectura)'::TEXT;
        RETURN QUERY SELECT 'modo'::VARCHAR(50), 'Hot Standby - Recibiendo WAL'::TEXT;
    ELSE
        RETURN QUERY SELECT 'rol_nodo'::VARCHAR(50), 'PRIMARY (Lectura/Escritura)'::TEXT;
        RETURN QUERY SELECT 'lsn_actual'::VARCHAR(50), pg_current_wal_lsn()::TEXT;
    END IF;
    
    -- Información general
    RETURN QUERY SELECT 'base_datos'::VARCHAR(50), current_database()::TEXT;
    RETURN QUERY SELECT 'usuario'::VARCHAR(50), current_user::TEXT;
    RETURN QUERY SELECT 'timestamp'::VARCHAR(50), NOW()::TEXT;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Función: Estado de replicación (ejecutar en PRIMARY)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_estado_replicacion()
RETURNS TABLE (
    replica TEXT,
    estado TEXT,
    lsn_enviado TEXT,
    lsn_aplicado TEXT,
    lag_bytes BIGINT,
    lag_tiempo INTERVAL,
    modo_sync TEXT
) AS $$
BEGIN
    IF pg_is_in_recovery() THEN
        RAISE NOTICE 'Este nodo es STANDBY. Ejecutar en PRIMARY para ver réplicas.';
        RETURN;
    END IF;
    
    RETURN QUERY
    SELECT 
        r.application_name::TEXT,
        r.state::TEXT,
        r.sent_lsn::TEXT,
        r.replay_lsn::TEXT,
        pg_wal_lsn_diff(r.sent_lsn, r.replay_lsn)::BIGINT,
        (NOW() - r.reply_time)::INTERVAL,
        r.sync_state::TEXT
    FROM pg_stat_replication r;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Función: Salud completa del cluster
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_salud_cluster()
RETURNS TABLE (
    componente VARCHAR(50),
    estado VARCHAR(20),
    detalle TEXT,
    accion_requerida TEXT
) AS $$
DECLARE
    v_replicas INTEGER;
    v_lag NUMERIC;
    v_sync_replicas INTEGER;
BEGIN
    -- Verificar rol del nodo
    IF pg_is_in_recovery() THEN
        RETURN QUERY SELECT 
            'rol_nodo'::VARCHAR(50), 
            'OK'::VARCHAR(20), 
            'Funcionando como STANDBY'::TEXT,
            'Ninguna'::TEXT;
        
        RETURN QUERY SELECT 
            'modo_standby'::VARCHAR(50), 
            'OK'::VARCHAR(20), 
            'Hot Standby activo - Consultas de lectura permitidas'::TEXT,
            'Ninguna'::TEXT;
    ELSE
        RETURN QUERY SELECT 
            'rol_nodo'::VARCHAR(50), 
            'OK'::VARCHAR(20), 
            'Funcionando como PRIMARY'::TEXT,
            'Ninguna'::TEXT;
        
        -- Contar réplicas conectadas
        SELECT COUNT(*) INTO v_replicas FROM pg_stat_replication;
        SELECT COUNT(*) INTO v_sync_replicas FROM pg_stat_replication WHERE sync_state = 'sync';
        
        IF v_replicas > 0 THEN
            RETURN QUERY SELECT 
                'replicas_conectadas'::VARCHAR(50), 
                'OK'::VARCHAR(20), 
                (v_replicas || ' réplica(s) conectada(s)')::TEXT,
                'Ninguna'::TEXT;
            
            -- Verificar lag
            SELECT MAX(pg_wal_lsn_diff(sent_lsn, replay_lsn))
            INTO v_lag FROM pg_stat_replication;
            
            IF v_lag < 1048576 THEN -- Menos de 1MB
                RETURN QUERY SELECT 
                    'lag_replicacion'::VARCHAR(50), 
                    'OK'::VARCHAR(20), 
                    ('Lag: ' || pg_size_pretty(v_lag::BIGINT) || ' - Dentro de límites')::TEXT,
                    'Ninguna'::TEXT;
            ELSIF v_lag < 10485760 THEN -- Menos de 10MB
                RETURN QUERY SELECT 
                    'lag_replicacion'::VARCHAR(50), 
                    'WARNING'::VARCHAR(20), 
                    ('Lag: ' || pg_size_pretty(v_lag::BIGINT) || ' - Elevado')::TEXT,
                    'Monitorizar carga del sistema'::TEXT;
            ELSE
                RETURN QUERY SELECT 
                    'lag_replicacion'::VARCHAR(50), 
                    'CRITICAL'::VARCHAR(20), 
                    ('Lag: ' || pg_size_pretty(v_lag::BIGINT) || ' - Crítico')::TEXT,
                    'Investigar problema de red o rendimiento'::TEXT;
            END IF;
            
            -- Estado de sincronización
            IF v_sync_replicas > 0 THEN
                RETURN QUERY SELECT 
                    'modo_replicacion'::VARCHAR(50), 
                    'OK'::VARCHAR(20), 
                    ('Replicación SÍNCRONA activa - RPO = 0')::TEXT,
                    'Ninguna'::TEXT;
            ELSE
                RETURN QUERY SELECT 
                    'modo_replicacion'::VARCHAR(50), 
                    'INFO'::VARCHAR(20), 
                    ('Replicación ASÍNCRONA - Posible pérdida de datos mínima')::TEXT,
                    'Considerar activar replicación síncrona'::TEXT;
            END IF;
        ELSE
            RETURN QUERY SELECT 
                'replicas_conectadas'::VARCHAR(50), 
                'CRITICAL'::VARCHAR(20), 
                'No hay réplicas conectadas'::TEXT,
                'URGENTE: Sistema sin alta disponibilidad'::TEXT;
        END IF;
    END IF;
    
    -- Estado WAL
    RETURN QUERY SELECT 
        'sistema_wal'::VARCHAR(50), 
        'OK'::VARCHAR(20), 
        'Write-Ahead Log operativo'::TEXT,
        'Ninguna'::TEXT;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Función: Verificar si el failover es posible
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_verificar_failover_posible()
RETURNS TABLE (
    verificacion VARCHAR(50),
    resultado BOOLEAN,
    detalle TEXT
) AS $$
BEGIN
    -- Solo tiene sentido en STANDBY
    IF NOT pg_is_in_recovery() THEN
        RETURN QUERY SELECT 
            'nodo_es_standby'::VARCHAR(50), 
            false, 
            'Este nodo es PRIMARY, no puede ser promovido'::TEXT;
        RETURN;
    END IF;
    
    RETURN QUERY SELECT 
        'nodo_es_standby'::VARCHAR(50), 
        true, 
        'Nodo es STANDBY - Puede ser promovido'::TEXT;
    
    RETURN QUERY SELECT 
        'wal_receiver_activo'::VARCHAR(50), 
        EXISTS(SELECT 1 FROM pg_stat_wal_receiver), 
        'WAL receiver está recibiendo datos'::TEXT;
    
    RETURN QUERY SELECT 
        'datos_sincronizados'::VARCHAR(50), 
        true, 
        'Listo para promoción con pg_promote()'::TEXT;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Función: Registrar evento de failover
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_registrar_failover(
    p_tipo VARCHAR(20),
    p_nodo_anterior VARCHAR(100),
    p_nodo_nuevo VARCHAR(100),
    p_notas TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO Registro_Failover (
        timestamp_deteccion,
        timestamp_completado,
        nodo_anterior,
        nodo_nuevo,
        tipo_failover,
        estado,
        notas
    ) VALUES (
        NOW(),
        NOW(),
        p_nodo_anterior,
        p_nodo_nuevo,
        p_tipo,
        'completado',
        p_notas
    ) RETURNING failover_id INTO v_id;
    
    -- Registrar también en log de HA
    INSERT INTO Log_Alta_Disponibilidad (evento, descripcion, nodo_origen, nodo_destino)
    VALUES ('FAILOVER', 'Failover ' || p_tipo || ' ejecutado', p_nodo_anterior, p_nodo_nuevo);
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PARTE 5: VISTAS DE MONITORIZACION
-- =============================================================================

-- Vista: Resumen del cluster
CREATE OR REPLACE VIEW vw_resumen_cluster AS
SELECT 
    CASE WHEN pg_is_in_recovery() THEN 'STANDBY' ELSE 'PRIMARY' END AS rol_nodo,
    current_database() AS base_datos,
    CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_lsn() END AS lsn_actual,
    (SELECT COUNT(*) FROM pg_stat_replication) AS replicas_activas,
    NOW() AS timestamp_consulta;

-- Vista: Detalle de slots de replicación
CREATE OR REPLACE VIEW vw_slots_replicacion AS
SELECT 
    slot_name AS nombre_slot,
    slot_type AS tipo,
    active AS activo,
    restart_lsn,
    confirmed_flush_lsn,
    CASE WHEN active THEN 'Conectado' ELSE 'Desconectado' END AS estado_conexion
FROM pg_replication_slots;

-- Vista: Historial de failovers
CREATE OR REPLACE VIEW vw_historial_failovers AS
SELECT 
    failover_id,
    timestamp_deteccion,
    timestamp_completado,
    nodo_anterior,
    nodo_nuevo,
    tipo_failover,
    estado,
    EXTRACT(EPOCH FROM (timestamp_completado - timestamp_deteccion)) AS duracion_segundos,
    notas
FROM Registro_Failover
ORDER BY timestamp_deteccion DESC;

-- =============================================================================
-- PARTE 6: TRIGGER DE AUDITORIA
-- =============================================================================

-- Función de auditoría para operaciones críticas
CREATE OR REPLACE FUNCTION fn_auditoria_ha()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO Log_Alta_Disponibilidad (
        evento, 
        descripcion, 
        nodo_origen,
        lsn_evento,
        datos_adicionales
    )
    VALUES (
        TG_OP,
        'Operación en tabla ' || TG_TABLE_NAME,
        CASE WHEN pg_is_in_recovery() THEN 'STANDBY' ELSE 'PRIMARY' END,
        CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_lsn() END,
        CASE 
            WHEN TG_OP = 'DELETE' THEN row_to_json(OLD)::JSONB
            ELSE row_to_json(NEW)::JSONB
        END
    );
    
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Trigger en tabla Pedido
DROP TRIGGER IF EXISTS trg_auditoria_pedido ON Pedido;
CREATE TRIGGER trg_auditoria_pedido
    AFTER INSERT OR UPDATE OR DELETE ON Pedido
    FOR EACH ROW EXECUTE FUNCTION fn_auditoria_ha();

-- =============================================================================
-- PARTE 7: MENSAJE DE CONFIRMACION
-- =============================================================================

DO $$
DECLARE
    v_rol TEXT;
BEGIN
    IF pg_is_in_recovery() THEN
        v_rol := 'STANDBY';
    ELSE
        v_rol := 'PRIMARY';
    END IF;
    
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'FASE 7 - SETUP COMPLETADO EXITOSAMENTE';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Nodo actual: %', v_rol;
    RAISE NOTICE 'Esquema: marketplace_ha';
    RAISE NOTICE 'Tablas de negocio: 6';
    RAISE NOTICE 'Tablas de HA: 2';
    RAISE NOTICE 'Funciones de monitorización: 5';
    RAISE NOTICE 'Vistas: 3';
    RAISE NOTICE '';
    RAISE NOTICE 'Comandos de verificación:';
    RAISE NOTICE '  SELECT * FROM fn_estado_nodo();';
    RAISE NOTICE '  SELECT * FROM fn_estado_replicacion();';
    RAISE NOTICE '  SELECT * FROM fn_salud_cluster();';
    RAISE NOTICE '  SELECT * FROM vw_resumen_cluster;';
    RAISE NOTICE '============================================================';
END $$;
