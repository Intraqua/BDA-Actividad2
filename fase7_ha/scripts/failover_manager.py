#!/usr/bin/env python3
# =============================================================================
# FASE 7 - FAILOVER MANAGER AUTOMATICO
# Sistema Marketplace Digital - Actividad 2
# =============================================================================
# Este script monitoriza el nodo PRIMARY y ejecuta failover automático
# cuando detecta que el PRIMARY no está disponible.
#
# Características:
#   - Detección de fallos mediante health checks
#   - Umbral configurable de intentos antes de failover
#   - Promoción automática del STANDBY a PRIMARY
#   - Logging detallado de todas las acciones
#   - Notificación del estado del cluster
# =============================================================================

import os
import sys
import time
import socket
import logging
from datetime import datetime

# Configurar logging
log_dir = '/var/log/failover'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{log_dir}/failover_manager.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('FailoverManager')

# Intentar importar psycopg2
try:
    import psycopg2
    from psycopg2 import sql, OperationalError
except ImportError:
    logger.error("psycopg2 no está instalado. Instalando...")
    os.system('pip install psycopg2-binary --quiet')
    import psycopg2
    from psycopg2 import sql, OperationalError


class FailoverManager:
    """
    Gestor de Failover Automático para PostgreSQL.
    
    Monitoriza el nodo PRIMARY y promueve automáticamente el STANDBY
    cuando detecta que el PRIMARY no está disponible.
    """
    
    def __init__(self):
        # Configuración desde variables de entorno
        self.primary_host = os.getenv('PRIMARY_HOST', 'pg-primary')
        self.standby_host = os.getenv('STANDBY_HOST', 'pg-standby')
        self.db_port = int(os.getenv('DB_PORT', 5432))
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'postgres123')
        self.db_name = os.getenv('DB_NAME', 'marketplace_ha')
        self.check_interval = int(os.getenv('CHECK_INTERVAL', 5))
        self.failover_threshold = int(os.getenv('FAILOVER_THRESHOLD', 3))
        
        # Estado interno
        self.primary_failures = 0
        self.failover_executed = False
        self.current_primary = self.primary_host
        
        logger.info("=" * 60)
        logger.info("FAILOVER MANAGER - INICIADO")
        logger.info("=" * 60)
        logger.info(f"Primary Host: {self.primary_host}")
        logger.info(f"Standby Host: {self.standby_host}")
        logger.info(f"Intervalo de verificación: {self.check_interval}s")
        logger.info(f"Umbral de failover: {self.failover_threshold} fallos")
        logger.info("=" * 60)
    
    def get_connection(self, host):
        """Obtiene conexión a un nodo PostgreSQL."""
        try:
            conn = psycopg2.connect(
                host=host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                dbname=self.db_name,
                connect_timeout=5
            )
            return conn
        except Exception as e:
            logger.debug(f"No se pudo conectar a {host}: {e}")
            return None
    
    def check_node_health(self, host):
        """
        Verifica la salud de un nodo PostgreSQL.
        
        Returns:
            dict: Estado del nodo con información detallada
        """
        result = {
            'host': host,
            'available': False,
            'is_primary': False,
            'is_standby': False,
            'replication_lag': None,
            'error': None
        }
        
        conn = self.get_connection(host)
        if not conn:
            result['error'] = 'No se pudo establecer conexión'
            return result
        
        try:
            with conn.cursor() as cur:
                # Verificar si está en recovery (standby) o no (primary)
                cur.execute("SELECT pg_is_in_recovery();")
                is_in_recovery = cur.fetchone()[0]
                
                result['available'] = True
                result['is_standby'] = is_in_recovery
                result['is_primary'] = not is_in_recovery
                
                # Si es primary, verificar lag de réplicas
                if not is_in_recovery:
                    cur.execute("""
                        SELECT 
                            application_name,
                            state,
                            pg_wal_lsn_diff(sent_lsn, replay_lsn) as lag_bytes
                        FROM pg_stat_replication;
                    """)
                    replicas = cur.fetchall()
                    if replicas:
                        result['replicas'] = len(replicas)
                        result['replication_lag'] = replicas[0][2] if replicas else 0
                
        except Exception as e:
            result['error'] = str(e)
        finally:
            conn.close()
        
        return result
    
    def promote_standby(self):
        """
        Promueve el nodo STANDBY a PRIMARY.
        
        Esta es la operación crítica de failover que convierte
        el standby en el nuevo primary.
        """
        logger.warning("=" * 60)
        logger.warning("INICIANDO PROCESO DE FAILOVER")
        logger.warning("=" * 60)
        
        conn = self.get_connection(self.standby_host)
        if not conn:
            logger.error(f"ERROR: No se puede conectar al standby {self.standby_host}")
            return False
        
        try:
            # Registrar timestamp del failover
            failover_time = datetime.now().isoformat()
            logger.info(f"Timestamp de failover: {failover_time}")
            
            with conn.cursor() as cur:
                # Verificar que realmente es un standby
                cur.execute("SELECT pg_is_in_recovery();")
                if not cur.fetchone()[0]:
                    logger.warning("El nodo ya es PRIMARY, no se requiere promoción")
                    return True
                
                # Ejecutar promoción
                logger.info(f"Ejecutando pg_promote() en {self.standby_host}...")
                cur.execute("SELECT pg_promote();")
                result = cur.fetchone()[0]
                
                if result:
                    logger.info("Comando pg_promote() ejecutado exitosamente")
                else:
                    logger.error("pg_promote() retornó FALSE")
                    return False
            
            conn.commit()
            
            # Esperar a que la promoción se complete
            logger.info("Esperando a que la promoción se complete...")
            time.sleep(3)
            
            # Verificar que la promoción fue exitosa
            verification_conn = self.get_connection(self.standby_host)
            if verification_conn:
                with verification_conn.cursor() as cur:
                    cur.execute("SELECT pg_is_in_recovery();")
                    still_standby = cur.fetchone()[0]
                    
                    if not still_standby:
                        logger.info("=" * 60)
                        logger.info("FAILOVER COMPLETADO EXITOSAMENTE")
                        logger.info(f"Nuevo PRIMARY: {self.standby_host}")
                        logger.info("=" * 60)
                        self.current_primary = self.standby_host
                        self.failover_executed = True
                        
                        # Registrar en log del sistema
                        self._log_failover_event(failover_time)
                        
                        return True
                    else:
                        logger.error("La promoción no se completó correctamente")
                        return False
                verification_conn.close()
            
        except Exception as e:
            logger.error(f"Error durante la promoción: {e}")
            return False
        finally:
            conn.close()
        
        return False
    
    def _log_failover_event(self, failover_time):
        """Registra el evento de failover en un archivo separado."""
        event_log = f'{log_dir}/failover_events.log'
        with open(event_log, 'a') as f:
            f.write(f"\n{'=' * 60}\n")
            f.write(f"FAILOVER EVENT\n")
            f.write(f"Timestamp: {failover_time}\n")
            f.write(f"Primary anterior: {self.primary_host}\n")
            f.write(f"Nuevo Primary: {self.standby_host}\n")
            f.write(f"Motivo: Primary no disponible después de {self.failover_threshold} intentos\n")
            f.write(f"{'=' * 60}\n")
    
    def run(self):
        """
        Bucle principal de monitorización.
        
        Continuamente verifica el estado del PRIMARY y ejecuta
        failover automático si es necesario.
        """
        logger.info("Iniciando monitorización del cluster...")
        
        # Esperar a que los nodos estén listos
        logger.info("Esperando 10 segundos para que los nodos se estabilicen...")
        time.sleep(10)
        
        while True:
            try:
                if self.failover_executed:
                    # Post-failover: monitorizar el nuevo primary
                    self._monitor_post_failover()
                else:
                    # Modo normal: monitorizar primary original
                    self._monitor_normal()
                
            except Exception as e:
                logger.error(f"Error en el ciclo de monitorización: {e}")
            
            time.sleep(self.check_interval)
    
    def _monitor_normal(self):
        """Monitorización en modo normal (antes de failover)."""
        primary_status = self.check_node_health(self.primary_host)
        standby_status = self.check_node_health(self.standby_host)
        
        # Log del estado actual
        if primary_status['available'] and primary_status['is_primary']:
            lag_info = ""
            if primary_status.get('replication_lag') is not None:
                lag_bytes = primary_status['replication_lag']
                lag_info = f" | Lag: {lag_bytes} bytes"
            logger.info(f"PRIMARY [{self.primary_host}]: OK{lag_info}")
            self.primary_failures = 0  # Reset contador
            
        else:
            self.primary_failures += 1
            logger.warning(
                f"PRIMARY [{self.primary_host}]: FALLO "
                f"({self.primary_failures}/{self.failover_threshold})"
            )
            
            # Verificar si se debe ejecutar failover
            if self.primary_failures >= self.failover_threshold:
                logger.warning("Umbral de fallos alcanzado")
                
                # Verificar que el standby está disponible
                if standby_status['available'] and standby_status['is_standby']:
                    logger.info(f"STANDBY [{self.standby_host}]: Disponible para promoción")
                    self.promote_standby()
                else:
                    logger.error(
                        f"STANDBY [{self.standby_host}]: No disponible para failover"
                    )
        
        # Estado del standby
        if standby_status['available']:
            role = "STANDBY" if standby_status['is_standby'] else "PRIMARY"
            logger.info(f"STANDBY [{self.standby_host}]: OK ({role})")
        else:
            logger.warning(f"STANDBY [{self.standby_host}]: No disponible")
    
    def _monitor_post_failover(self):
        """Monitorización después de un failover."""
        new_primary_status = self.check_node_health(self.standby_host)
        
        if new_primary_status['available'] and new_primary_status['is_primary']:
            logger.info(
                f"NUEVO PRIMARY [{self.standby_host}]: OK - "
                f"Sistema operando en modo failover"
            )
        else:
            logger.warning(
                f"NUEVO PRIMARY [{self.standby_host}]: "
                f"Estado inesperado después de failover"
            )
        
        # Verificar si el antiguo primary vuelve
        old_primary_status = self.check_node_health(self.primary_host)
        if old_primary_status['available']:
            logger.info(
                f"ANTIGUO PRIMARY [{self.primary_host}]: "
                f"Detectado online - Requiere reconfiguración manual como standby"
            )


def main():
    """Punto de entrada principal."""
    try:
        manager = FailoverManager()
        manager.run()
    except KeyboardInterrupt:
        logger.info("Failover Manager detenido por el usuario")
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
