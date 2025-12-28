# Bases de Datos Avanzadas - Actividad 2

## Debatiendo las ideas de Subramanian & Saravanan

**Asignatura:** Bases de Datos Avanzadas  
**Universidad:** UNIPRO - Universidad Digital Europea  
**Autor:** David Valbuena Segura  
**Fecha:** Enero 2026

## Descripción

Este repositorio contiene la implementación práctica de un sistema de marketplace digital desarrollado a lo largo de siete fases evolutivas. Cada fase aborda un aspecto fundamental de los sistemas de bases de datos avanzados, desde la indexación hasta la alta disponibilidad.

El proyecto demuestra la aplicación práctica de conceptos teóricos sobre gestión de bases de datos, incluyendo optimización de consultas, control de concurrencia, recuperación ante fallos e integración de sistemas híbridos SQL/NoSQL.

## Tecnologías Utilizadas

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| PostgreSQL | 16.x | Base de datos relacional principal |
| MongoDB | 7.x | Base de datos NoSQL para datos semiestructurados |
| Docker | 24.x | Contenedorización y despliegue |
| Docker Compose | 2.x | Orquestación de servicios |
| pgAdmin | 4.x | Administración de PostgreSQL |
| Mongo Express | - | Administración de MongoDB |

## Estructura del Repositorio

```
BDA-Actividad2/
├── README.md
├──Docker/
│   ├── docker-compose.yml                # Infraestructura base PostgreSQL
│
├── fase1_indexacion/
│   ├── fase1_schema.sql                  # Esquema base del marketplace
│   ├── fase1_indices.sql                 # Creación de índices
│   ├── fase1_verificacion.sql            # Scripts de verificación
│   └── Capturas/
│
├── fase2_acid/
│   ├── fase2_acid.sql                    # Transacciones y propiedades ACID
│   └── Capturas/
│
├── fase3_concurrencia/
│   ├── fase3_concurrencia.sql            # Control de concurrencia
│   └── Capturas/
│
├── fase4_recuperacion/
│   ├── fase4_recuperacion.sql            # Mecanismos de recuperación
│   └── Capturas/
│
├── fase5_optimizacion/
│   ├── fase5_optimizacion.sql            # Optimización de consultas
│   └── Capturas/
│
├── fase6_nosql/
│   ├── fase6_integracion_hibrida.sql     # Sistema de sincronización
│   ├── fase6_pruebas_integracion.sql     # Pruebas de integración
│   ├── perfiles_usuario.json             # Datos migrados (clientes)
│   ├── catalogo_productos.json           # Datos migrados (productos)
│   ├── docker-compose-mongodb-simple.yml     # Infraestructura MongoDB
│   └── Capturas/
│
├── fase7_alta_disponibilidad/
│   ├── fase7_setup_ha.sql                # Configuración del cluster
│   ├── fase7_pruebas_failover.sql        # Pruebas de failover
│   ├── fase7_verificacion_rapida.sql     # Verificación del sistema
│   ├── failover_manager.py               # Script de failover automático
│   ├── docker-compose-ha.yml             # Infraestructura de alta disponibilidad
│   └── Capturas/

```

## Fases del Proyecto

### Fase 1: Indexación

Diseño e implementación de estrategias de indexación para optimizar consultas frecuentes en el marketplace.

**Contenido:**
- Índices B-tree para búsquedas por nombre, precio y categoría
- Índices compuestos para consultas combinadas
- Clustering y particionamiento por rangos de precio
- Análisis comparativo de rendimiento con y sin índices

### Fase 2: Propiedades ACID y Procesamiento Transaccional

Implementación de transacciones que garantizan atomicidad, consistencia, aislamiento y durabilidad.

**Contenido:**
- Extensión del modelo con control de stock y comprobantes
- Transacción de compra atómica (pedido, stock, comprobante)
- Escenarios de éxito y fallo con rollback automático
- Análisis detallado de cada propiedad ACID

### Fase 3: Concurrencia y Aislamiento

Control de acceso concurrente para prevenir anomalías en operaciones simultáneas.

**Contenido:**
- Simulación de compras concurrentes con stock limitado
- Análisis en niveles READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- Implementación de bloqueo pesimista con SELECT FOR UPDATE
- Función de compra segura con prevención de sobreventa

### Fase 4: Recuperación y Tolerancia a Fallos

Mecanismos de recuperación basados en Write-Ahead Logging (WAL).

**Contenido:**
- Ciclo de vida de transacciones registradas
- Simulación de fallos con operaciones UNDO/REDO
- Estrategia de checkpointing configurada
- Política de retención de logs y backup PITR

### Fase 5: Optimización de Consultas

Análisis y optimización de consultas analíticas mediante EXPLAIN ANALYZE.

**Contenido:**
- Identificación de cuellos de botella en consultas críticas
- Reestructuración de índices para patrones de acceso específicos
- Vistas materializadas para dashboards analíticos
- Mejoras de rendimiento documentadas (60-73% de reducción)

### Fase 6: Integración con NoSQL

Arquitectura híbrida PostgreSQL-MongoDB para datos semiestructurados.

**Contenido:**
- Selección y justificación de datos para migración
- Diseño de esquema NoSQL (colecciones y documentos)
- Proceso de migración mediante COPY y mongoimport
- Sistema de sincronización con triggers y cola de mensajes

### Fase 7: Alta Disponibilidad y Recuperación ante Fallos

Cluster PostgreSQL con replicación streaming y failover automático.

**Contenido:**
- Arquitectura Primary-Standby con replicación síncrona
- Configuración de RPO=0 y RTO menor a 30 segundos
- Failover automático mediante script de monitorización
- Prevención de split-brain y pruebas de recuperación

## Requisitos Previos

- Docker Desktop 4.x o superior
- Docker Compose 2.x
- Git
- 8 GB de RAM mínimo recomendado
- 10 GB de espacio en disco

## Instalación y Ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/Intraqua/BDA-Actividad2.git
cd BDA-Actividad2
```

### 2. Iniciar infraestructura base (Fases 1-5)

```bash
docker-compose up -d
```

Acceso a pgAdmin: http://localhost:5050
- Email: admin@admin.com
- Password: admin

### 3. Iniciar MongoDB (Fase 6)

```bash
docker-compose -f docker-compose-mongodb-simple.yml up -d
```

Acceso a Mongo Express: http://localhost:8081

### 4. Iniciar cluster de alta disponibilidad (Fase 7)

```bash
docker-compose -f docker-compose-ha.yml up -d
```

Puertos del cluster:
- Primary: localhost:5432
- Standby: localhost:5433
- pgAdmin: localhost:5050

### 5. Ejecutar scripts SQL

Los scripts de cada fase deben ejecutarse en orden secuencial desde pgAdmin o mediante psql:

```bash
# Conectar al contenedor PostgreSQL
docker exec -it marketplace-postgres psql -U postgres -d marketplace_indices

# Ejecutar script de una fase
\i /path/to/fase1_indices.sql
```

## Modelo de Datos

### Entidades Principales

| Entidad | Descripción |
|---------|-------------|
| Producto | Catálogo de productos con precio, stock y categoría |
| Categoria | Clasificación de productos |
| Vendedor | Vendedores del marketplace |
| Cliente | Usuarios compradores |
| Pedido | Registro de compras realizadas |
| Comprobante | Documentos de transacciones confirmadas |

### Diagrama Entidad-Relación

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Categoria  │────<│  Producto   │>────│  Vendedor   │
└─────────────┘     └──────┬──────┘     └─────────────┘
                          │
                          │             
                   ┌──────┴──────┐     ┌─────────────┐
                   │   Pedido    │>────│   Cliente   │
                   └──────┬──────┘     └─────────────┘
                          │            
                   ┌──────┴──────┐
                   │ Comprobante │
                   └─────────────┘
```

## Verificación de Funcionamiento

### Comprobar estado de los contenedores

```bash
docker ps
```

### Verificar replicación (Fase 7)

```sql
-- En el Primary
SELECT * FROM pg_stat_replication;

-- En el Standby
SELECT pg_is_in_recovery();
```

### Verificar sincronización PostgreSQL-MongoDB (Fase 6)

```sql
-- Consultar cola de sincronización
SELECT * FROM sync_queue WHERE procesado = FALSE;
```

## Consideraciones de Rendimiento

| Métrica | Antes | Después | Mejora |
|---------|-------|---------|--------|
| Q1 (categoría/mes) | 5.9 ms | 1.8 ms | 69.5% |
| Q2 (ingresos vendedor) | 4.5 ms | 1.2 ms | 73.3% |
| Q3 (historial cliente) | 3.8 ms | 1.5 ms | 60.5% |

## Referencias

- Material del curso: Bases de Datos Avanzadas (UNIPRO)
- PostgreSQL Documentation: https://www.postgresql.org/docs/
- MongoDB Documentation: https://www.mongodb.com/docs/
- Docker Documentation: https://docs.docker.com/

## Licencia

Este proyecto ha sido desarrollado con fines académicos para la asignatura de Bases de Datos Avanzadas de UNIPRO.

## Contacto

**Autor:** David Valbuena Segura  
**Repositorio:** https://github.com/Intraqua/BDA-Actividad2
