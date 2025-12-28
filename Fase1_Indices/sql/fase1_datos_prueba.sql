-- ============================================================
-- ACTIVIDAD 2 - FASE 1: DATOS DE PRUEBA
-- Sistema de Marketplace Digital
-- Bases de Datos Avanzadas - UNIPRO
-- Autor: David Valbuena Segura
-- ============================================================

-- ============================================================
-- 1. INSERTAR CATEGORÍAS
-- ============================================================

INSERT INTO Categoria (nombre_categoria) VALUES
('Electrónica'),
('Informática'),
('Hogar'),
('Deportes'),
('Moda'),
('Smartphones'),
('Tablets'),
('Auriculares'),
('Portátiles'),
('Componentes PC'),
('Periféricos'),
('Cocina'),
('Decoración'),
('Fitness'),
('Running');

-- ============================================================
-- 2. INSERTAR VENDEDORES
-- ============================================================

INSERT INTO Vendedor (nombre_vendedor) VALUES
('TechStore España'),
('ElectroMax'),
('HomePlus'),
('DeporTotal'),
('ModaOnline'),
('GadgetWorld'),
('InformáticaPro'),
('CasaIdeal'),
('SportLine'),
('FashionTrend');

-- ============================================================
-- 3. INSERTAR CLIENTES
-- ============================================================

INSERT INTO Cliente (nombre_cliente)
SELECT 
    (ARRAY['Juan García', 'María López', 'Carlos Martínez', 'Ana Sánchez', 
           'Pedro Fernández', 'Laura González', 'Miguel Rodríguez', 'Carmen Pérez',
           'David Gómez', 'Elena Ruiz', 'Antonio Díaz', 'Rosa Moreno',
           'Francisco Álvarez', 'Isabel Muñoz', 'Manuel Romero'])[1 + (i % 15)]
    || ' ' || i
FROM generate_series(1, 500) AS i;

-- ============================================================
-- 4. INSERTAR PRODUCTOS
-- ============================================================

INSERT INTO Producto (nombre, precio, id_categoria, id_vendedor)
SELECT 
    (ARRAY['Producto Premium', 'Artículo Pro', 'Item Deluxe', 
           'Modelo Básico', 'Versión Plus', 'Serie Advance',
           'Edición Special', 'Gama Elite', 'Línea Classic'])[1 + (i % 9)]
    || ' ' || 
    (ARRAY['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 
           'Omega', 'Sigma', 'Theta', 'Lambda'])[1 + ((i/9) % 9)]
    || ' ' || i,
    ROUND((9.99 + (i % 100) * 5 + random() * 50)::numeric, 2),
    1 + (i % 15),
    1 + (i % 10)
FROM generate_series(1, 500) AS i;

-- ============================================================
-- 5. INSERTAR PEDIDOS
-- ============================================================

INSERT INTO Pedido (id_producto, id_cliente, fecha_pedido, cantidad)
SELECT 
    1 + (i % 500),
    1 + (i % 500),
    CURRENT_DATE - ((i % 365) || ' days')::interval,
    1 + (i % 10)
FROM generate_series(1, 2000) AS i;

-- ============================================================
-- 6. ACTUALIZAR ESTADÍSTICAS
-- ============================================================

ANALYZE Categoria;
ANALYZE Vendedor;
ANALYZE Cliente;
ANALYZE Producto;
ANALYZE Pedido;

-- ============================================================
-- 7. VERIFICACIÓN DE DATOS INSERTADOS
-- ============================================================

SELECT 'Categoria' AS tabla, COUNT(*) AS registros FROM Categoria
UNION ALL SELECT 'Vendedor', COUNT(*) FROM Vendedor
UNION ALL SELECT 'Cliente', COUNT(*) FROM Cliente
UNION ALL SELECT 'Producto', COUNT(*) FROM Producto
UNION ALL SELECT 'Pedido', COUNT(*) FROM Pedido
ORDER BY tabla;

-- ============================================================
-- 8. MUESTRA DE DATOS
-- ============================================================

-- Muestra de productos por categoría
SELECT c.nombre_categoria, COUNT(*) AS total_productos
FROM Producto p
JOIN Categoria c ON p.id_categoria = c.id_categoria
GROUP BY c.nombre_categoria
ORDER BY total_productos DESC;

-- Distribución de precios
SELECT 
    CASE 
        WHEN precio < 50 THEN 'Económico (< 50€)'
        WHEN precio < 200 THEN 'Medio (50-200€)'
        WHEN precio < 1000 THEN 'Premium (200-1000€)'
        ELSE 'Lujo (> 1000€)'
    END AS rango_precio,
    COUNT(*) AS cantidad
FROM Producto
GROUP BY 1
ORDER BY MIN(precio);
