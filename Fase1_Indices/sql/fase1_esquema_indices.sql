-- ============================================================
-- ACTIVIDAD 2 - FASE 1: INDEXACIÓN
-- Sistema de Marketplace Digital
-- Bases de Datos Avanzadas - UNIPRO
-- Autor: David Valbuena Segura
-- ============================================================

-- ============================================================
-- 1. CREACIÓN DE TABLAS (Modelo Lógico del Enunciado)
-- ============================================================

-- Tabla Categoria
CREATE TABLE IF NOT EXISTS Categoria (
    id_categoria SERIAL PRIMARY KEY,
    nombre_categoria VARCHAR(100) NOT NULL
);

-- Tabla Vendedor
CREATE TABLE IF NOT EXISTS Vendedor (
    id_vendedor SERIAL PRIMARY KEY,
    nombre_vendedor VARCHAR(150) NOT NULL
);

-- Tabla Cliente
CREATE TABLE IF NOT EXISTS Cliente (
    id_cliente SERIAL PRIMARY KEY,
    nombre_cliente VARCHAR(150) NOT NULL
);

-- Tabla Producto
CREATE TABLE IF NOT EXISTS Producto (
    id_producto SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    precio DECIMAL(10,2) NOT NULL CHECK (precio > 0),
    id_categoria INTEGER NOT NULL REFERENCES Categoria(id_categoria),
    id_vendedor INTEGER NOT NULL REFERENCES Vendedor(id_vendedor)
);

-- Tabla Pedido
CREATE TABLE IF NOT EXISTS Pedido (
    id_pedido SERIAL PRIMARY KEY,
    id_producto INTEGER NOT NULL REFERENCES Producto(id_producto),
    id_cliente INTEGER NOT NULL REFERENCES Cliente(id_cliente),
    fecha_pedido DATE NOT NULL DEFAULT CURRENT_DATE,
    cantidad INTEGER NOT NULL CHECK (cantidad > 0)
);

-- ============================================================
-- 2. ÍNDICES SIMPLES PARA CONSULTAS FRECUENTES
-- ============================================================

-- Índice para búsqueda parcial por nombre de producto
-- Utiliza varchar_pattern_ops para optimizar consultas LIKE 'texto%'
CREATE INDEX idx_producto_nombre 
    ON Producto (nombre varchar_pattern_ops);

-- Índice para consultas por rango de precios
-- B-tree permite recorrer rangos de valores ordenados eficientemente
CREATE INDEX idx_producto_precio 
    ON Producto (precio);

-- Índice para filtrado por categoría
-- Acelera la navegación del catálogo por categorías
CREATE INDEX idx_producto_categoria 
    ON Producto (id_categoria);

-- Índice para consultas de pedidos por cliente
-- Optimiza la recuperación del historial de compras
CREATE INDEX idx_pedido_cliente 
    ON Pedido (id_cliente);

-- ============================================================
-- 3. ÍNDICES COMPUESTOS
-- ============================================================

-- Índice compuesto (id_categoria, precio)
-- Justificación: La navegación típica del marketplace filtra primero
-- por categoría y luego ordena o filtra por precio. El orden de las
-- columnas es importante: id_categoria va primero porque es filtro
-- de igualdad, precio segundo para ordenamiento o rango.
CREATE INDEX idx_producto_categoria_precio 
    ON Producto (id_categoria, precio);

-- Índice compuesto (id_cliente, fecha_pedido DESC)
-- Justificación: El historial de pedidos de un cliente se consulta
-- frecuentemente ordenado por fecha descendente. Este índice satisface
-- tanto el filtrado como la ordenación sin operación de sort adicional.
CREATE INDEX idx_pedido_cliente_fecha 
    ON Pedido (id_cliente, fecha_pedido DESC);

-- Índice compuesto (id_vendedor, id_categoria)
-- Justificación: Permite consultas analíticas sobre el catálogo de un
-- vendedor agrupado por categoría, obteniendo estadísticas sin escanear
-- la tabla completa.
CREATE INDEX idx_producto_vendedor_categoria 
    ON Producto (id_vendedor, id_categoria);

-- ============================================================
-- 4. ASOCIACIONES FÍSICAS Y AGRUPAMIENTOS
-- ============================================================

-- CLUSTER: Organización física por categoría
-- Reorganiza las filas de Producto en el mismo orden que el índice,
-- almacenando productos de la misma categoría en páginas contiguas.
-- Esto reduce operaciones de E/S en consultas por categoría.
CLUSTER Producto USING idx_producto_categoria;

-- ============================================================
-- 5. PARTICIONAMIENTO ESTÁTICO POR RANGO DE PRECIOS
-- ============================================================

-- Nota: El particionamiento requiere recrear la tabla.
-- Se presenta como propuesta alternativa para catálogos extensos.

-- Eliminar tabla original y crear versión particionada
-- (Comentado para no afectar datos existentes en ejecución normal)

/*
DROP TABLE IF EXISTS Producto CASCADE;

CREATE TABLE Producto (
    id_producto SERIAL,
    nombre VARCHAR(200) NOT NULL,
    precio DECIMAL(10,2) NOT NULL CHECK (precio > 0),
    id_categoria INTEGER NOT NULL,
    id_vendedor INTEGER NOT NULL,
    PRIMARY KEY (id_producto, precio)
) PARTITION BY RANGE (precio);

-- Partición para productos económicos (0 - 50 EUR)
CREATE TABLE producto_economico PARTITION OF Producto
    FOR VALUES FROM (0) TO (50);

-- Partición para productos de gama media (50 - 200 EUR)
CREATE TABLE producto_medio PARTITION OF Producto
    FOR VALUES FROM (50) TO (200);

-- Partición para productos premium (200 - 1000 EUR)
CREATE TABLE producto_premium PARTITION OF Producto
    FOR VALUES FROM (200) TO (1000);

-- Partición para productos de lujo (más de 1000 EUR)
CREATE TABLE producto_lujo PARTITION OF Producto
    FOR VALUES FROM (1000) TO (MAXVALUE);

-- Añadir foreign keys después del particionamiento
ALTER TABLE producto_economico 
    ADD CONSTRAINT fk_eco_categoria FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria),
    ADD CONSTRAINT fk_eco_vendedor FOREIGN KEY (id_vendedor) REFERENCES Vendedor(id_vendedor);

ALTER TABLE producto_medio 
    ADD CONSTRAINT fk_med_categoria FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria),
    ADD CONSTRAINT fk_med_vendedor FOREIGN KEY (id_vendedor) REFERENCES Vendedor(id_vendedor);

ALTER TABLE producto_premium 
    ADD CONSTRAINT fk_pre_categoria FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria),
    ADD CONSTRAINT fk_pre_vendedor FOREIGN KEY (id_vendedor) REFERENCES Vendedor(id_vendedor);

ALTER TABLE producto_lujo 
    ADD CONSTRAINT fk_luj_categoria FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria),
    ADD CONSTRAINT fk_luj_vendedor FOREIGN KEY (id_vendedor) REFERENCES Vendedor(id_vendedor);
*/

-- ============================================================
-- 6. VERIFICACIÓN DE ÍNDICES CREADOS
-- ============================================================

SELECT 
    tablename AS tabla,
    indexname AS indice,
    indexdef AS definicion
FROM pg_indexes 
WHERE schemaname = 'public'
ORDER BY tablename, indexname;
