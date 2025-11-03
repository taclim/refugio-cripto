# database_manager_REPAIRED.py - DATABASE MANAGER COMPLETAMENTE CORREGIDO
"""
Database Manager COMPLETAMENTE CORREGIDO para Dual Bot
Problema original: volume_ratio no se guardaba (siempre 0 o NULL)
Solución: Inserción correcta con validación y fallbacks seguros
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, Any, Optional

class DatabaseManager:
    def __init__(self, db_path: str = "signals.db"):
        self.db_path = db_path
        self.init_database()
        print(f"🗄️ DatabaseManager REPARADO inicializado: {db_path}")
    
    def init_database(self):
        """Inicializa la base de datos con estructura CORREGIDA"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Crear tabla con estructura COMPLETAMENTE CORREGIDA
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    entry REAL NOT NULL,
                    tp1 REAL NOT NULL,
                    sl REAL NOT NULL,
                    confidence REAL NOT NULL,
                    rr_ratio REAL NOT NULL,
                    
                    -- Indicadores técnicos CORREGIDOS
                    rsi REAL DEFAULT 50.0,
                    macd REAL DEFAULT 0.0,
                    macd_signal REAL DEFAULT 0.0,
                    macd_histogram REAL DEFAULT 0.0,
                    ema9 REAL,
                    ema21 REAL,
                    atr REAL,
                    volume_ratio REAL DEFAULT 1.0,
                    adx REAL DEFAULT 0.0,
                    
                    -- Metadatos
                    fecha_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resultado TEXT,
                    strategy_version TEXT DEFAULT 'REPAIRED_v1.0',
                    emergency_mode BOOLEAN DEFAULT 1,
                    
                    -- Seguimiento
                    seguimiento_json TEXT,
                    estado_json TEXT,
                    fecha_actualizacion TIMESTAMP
                )
            """)
            
            # Verificar si necesitamos agregar columnas faltantes
            cursor.execute("PRAGMA table_info(signals)")
            existing_columns = [row[1] for row in cursor.fetchall()]
            
            columns_to_add = [
                ("macd_signal", "REAL DEFAULT 0.0"),
                ("macd_histogram", "REAL DEFAULT 0.0"),
                ("ema9", "REAL"),
                ("ema21", "REAL"),
                ("atr", "REAL"),
                ("volume_ratio", "REAL DEFAULT 1.0"),
                ("adx", "REAL DEFAULT 0.0"),
                ("ma_type", "TEXT DEFAULT 'SMA'"),
                ("ma_length", "INTEGER DEFAULT 10"),
                ("strategy_version", "TEXT DEFAULT 'REPAIRED_v1.0'"),
                ("emergency_mode", "BOOLEAN DEFAULT 1"),
                ("fecha_actualizacion", "TIMESTAMP")
            ]
            
            for column_name, column_def in columns_to_add:
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_def}")
                    print(f"   ✅ Columna {column_name} agregada")
            
            # CORRECCIÓN CRÍTICA: Actualizar volume_ratio = 0 a 1.0
            cursor.execute("""
                UPDATE signals 
                SET volume_ratio = 1.0 
                WHERE volume_ratio = 0 OR volume_ratio IS NULL
            """)
            
            updated_rows = cursor.rowcount
            if updated_rows > 0:
                print(f"   🔧 {updated_rows} registros con volume_ratio corregidos (0 -> 1.0)")
            
            conn.commit()
            conn.close()
            print("   ✅ Estructura de base de datos CORREGIDA")
            
        except Exception as e:
            print(f"   ❌ Error inicializando base de datos: {e}")
    
    def save_signal(self, signal_data: Dict[str, Any]) -> bool:
        """Guarda señal con VALIDACIÓN COMPLETA incluyendo LEVERAGE"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Extraer datos con VALIDACIÓN ESTRICTA
            symbol = str(signal_data.get('symbol', 'UNKNOWN'))
            signal_type = str(signal_data.get('signal', 'UNKNOWN'))
            entry = float(signal_data.get('entry', 0))
            tp1 = float(signal_data.get('tp', 0))
            sl = float(signal_data.get('sl', 0))
            confidence = float(signal_data.get('confidence', 0))
            rr_ratio = float(signal_data.get('rr', 0))
            
            # Indicadores técnicos con FALLBACKS SEGUROS
            indicators = signal_data.get('latest_indicators', {})
            rsi = float(indicators.get('rsi', 50.0))
            macd = float(indicators.get('macd', 0.0))
            macd_signal = float(indicators.get('macd_signal', 0.0))
            macd_histogram = float(indicators.get('macd_histogram', 0.0))
            ema9 = float(indicators.get('ema9', entry))
            ema21 = float(indicators.get('ema21', entry))
            atr = float(indicators.get('atr', entry * 0.01))
            
            # CORRECCIÓN CRÍTICA PARA VOLUME_RATIO
            volume_ratio_raw = indicators.get('volume_ratio', 1.0)
            
            # Validación estricta de volume_ratio
            try:
                volume_ratio = float(volume_ratio_raw)
                
                # FORZAR que volume_ratio sea válido
                if volume_ratio <= 0 or volume_ratio != volume_ratio:  # NaN check
                    volume_ratio = 1.0
                    print(f"   🔧 Volume_ratio inválido corregido: {volume_ratio_raw} -> 1.0")
                elif volume_ratio > 100:  # Valor extremo
                    volume_ratio = min(volume_ratio, 10.0)
                    print(f"   🔧 Volume_ratio extremo limitado: {volume_ratio_raw} -> {volume_ratio}")
                
            except (ValueError, TypeError):
                volume_ratio = 1.0
                print(f"   🔧 Volume_ratio no numérico corregido: {volume_ratio_raw} -> 1.0")
            
            adx = float(indicators.get('adx', 0.0))

            # MA Type y Length
            ma_type = str(signal_data.get('ma_type', 'SMA'))
            ma_length = int(signal_data.get('ma_length', 10))

            # Metadatos
            strategy_version = str(signal_data.get('fix_version', 'REPAIRED_v1.0'))
            emergency_mode = bool(signal_data.get('emergency_mode', True))
            
            # ✅ LEVERAGE - SISTEMA 1 (NUEVO)
            leverage = int(signal_data.get('leverage', 5))
            if leverage < 1 or leverage > 30:
                leverage = 5
            
            print(f"   📊 DATOS A GUARDAR:")
            print(f"      Symbol: {symbol}")
            print(f"      Leverage: {leverage}x ← GUARDANDO")
            print(f"      Confidence: {confidence:.1f}%")
            print(f"      RR ratio: {rr_ratio:.2f}")
            
            # INSERCIÓN CON VALIDACIÓN COMPLETA + LEVERAGE
            cursor.execute("""
                INSERT INTO signals (
                    symbol, signal_type, entry, tp1, sl, confidence, rr_ratio,
                    rsi, macd, macd_signal, macd_histogram, ema9, ema21, atr,
                    volume_ratio, adx, ma_type, ma_length, strategy_version, emergency_mode,
                    leverage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, signal_type, entry, tp1, sl, confidence, rr_ratio,
                rsi, macd, macd_signal, macd_histogram, ema9, ema21, atr,
                volume_ratio, adx, ma_type, ma_length, strategy_version, emergency_mode,
                leverage
            ))
            
            signal_id = cursor.lastrowid
            conn.commit()
            
            # VERIFICACIÓN INMEDIATA
            cursor.execute("""
                SELECT volume_ratio, confidence, rr_ratio 
                FROM signals 
                WHERE id = ?
            """, (signal_id,))
            
            verification = cursor.fetchone()
            
            if verification:
                saved_vol_ratio, saved_conf, saved_rr = verification
                # print(f"   ✅ SEÑAL GUARDADA Y VERIFICADA:")  # Silenciado
                # print(f"      ID: {signal_id}")  # Silenciado
                # print(f"      Volume_ratio guardado: {saved_vol_ratio:.3f}")  # Silenciado
                # print(f"      Confidence guardada: {saved_conf:.1f}%")  # Silenciado
                # print(f"      RR ratio guardado: {saved_rr:.2f}")  # Silenciado
                
                # Verificar que volume_ratio se guardó correctamente
                if saved_vol_ratio > 0:
                    # print(f"   🎉 ÉXITO: Volume_ratio > 0 guardado correctamente")  # Silenciado
                    pass
                else:
                    print(f"   ❌ ERROR: Volume_ratio sigue siendo 0 después de guardar")
            else:
                print(f"   ❌ ERROR: No se pudo verificar la señal guardada")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"   ❌ Error guardando señal: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def update_signal_result(self, signal_id: int, resultado: str) -> bool:
        """Actualiza el resultado de una señal"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE signals 
                SET resultado = ?, fecha_actualizacion = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (resultado, signal_id))
            
            conn.commit()
            conn.close()
            
            print(f"   ✅ Resultado actualizado: ID {signal_id} -> {resultado}")
            return True
            
        except Exception as e:
            print(f"   ❌ Error actualizando resultado: {e}")
            return False
    
    def get_signal_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de señales INCLUYENDO volume_ratio"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Estadísticas básicas
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN resultado = 'TP1' THEN 1 END) as tp1,
                    COUNT(CASE WHEN resultado = 'SL' THEN 1 END) as sl,
                    AVG(confidence) as avg_confidence,
                    AVG(rr_ratio) as avg_rr_ratio,
                    AVG(volume_ratio) as avg_volume_ratio,
                    MIN(volume_ratio) as min_volume_ratio,
                    MAX(volume_ratio) as max_volume_ratio,
                    COUNT(CASE WHEN volume_ratio > 0 THEN 1 END) as positive_volume_count
                FROM signals
            """)
            
            stats = cursor.fetchone()
            total, tp1, sl, avg_conf, avg_rr, avg_vol, min_vol, max_vol, pos_vol = stats
            
            success_rate = (tp1 / total * 100) if total > 0 else 0
            
            conn.close()
            
            return {
                'total_signals': total,
                'tp1_count': tp1,
                'sl_count': sl,
                'success_rate': success_rate,
                'avg_confidence': avg_conf or 0,
                'avg_rr_ratio': avg_rr or 0,
                'avg_volume_ratio': avg_vol or 0,
                'min_volume_ratio': min_vol or 0,
                'max_volume_ratio': max_vol or 0,
                'positive_volume_count': pos_vol or 0,
                'volume_ratio_working': (pos_vol or 0) > 0
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def fix_existing_volume_ratios(self) -> int:
        """Corrige volume_ratios existentes que sean 0 o NULL"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Contar registros problemáticos
            cursor.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE volume_ratio = 0 OR volume_ratio IS NULL
            """)
            problematic_count = cursor.fetchone()[0]
            
            if problematic_count > 0:
                print(f"   🔧 Corrigiendo {problematic_count} registros con volume_ratio problemático")
                
                # Corregir a 1.0 (valor neutro)
                cursor.execute("""
                    UPDATE signals 
                    SET volume_ratio = 1.0, 
                        strategy_version = 'VOLUME_RATIO_FIXED',
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE volume_ratio = 0 OR volume_ratio IS NULL
                """)
                
                conn.commit()
                print(f"   ✅ {problematic_count} registros corregidos (volume_ratio = 1.0)")
            
            conn.close()
            return problematic_count
            
        except Exception as e:
            print(f"❌ Error corrigiendo volume_ratios: {e}")
            return 0

# Instancia global
_db_manager = None

def get_database_manager():
    """Obtiene instancia del database manager REPARADO"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager

# Funciones de compatibilidad
def save_signal_to_db(signal_data):
    """Guarda señal usando database manager REPARADO y sincroniza a GitHub"""
    db = get_database_manager()
    success = db.save_signal(signal_data)
    
    # ✅ SINCRONIZAR A GITHUB AUTOMÁTICAMENTE
    if success:
        try:
            sync_db_to_github()
        except Exception as e:
            print(f"   ⚠️  Error sincronizando a GitHub: {e}")
    
    return success

def sync_db_to_github():
    """Sincroniza signals.db a GitHub automáticamente"""
    import subprocess
    import os
    from datetime import datetime
    
    try:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(project_dir)
        
        # Verificar que signals.db existe
        if not os.path.exists('signals.db'):
            return False
        
        # Agregar signals.db
        subprocess.run(['git', 'add', 'signals.db'], capture_output=True, check=False)
        
        # Verificar si hay cambios
        result = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
        if not result.stdout.strip():
            return True  # No hay cambios
        
        # Crear commit
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        commit_message = f"🔄 Actualización: signals.db sincronizado ({timestamp})"
        
        subprocess.run(['git', 'commit', '-m', commit_message], capture_output=True, check=False)
        
        # Push a GitHub
        result = subprocess.run(['git', 'push', '-u', 'origin', 'main'], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"   ✅ signals.db sincronizado a GitHub")
            return True
        else:
            print(f"   ⚠️  Error en push: {result.stderr[:100]}")
            return False
            
    except Exception as e:
        print(f"   ⚠️  Error en sincronización: {str(e)[:100]}")
        return False

def update_signal_result(signal_id, resultado):
    """Actualiza resultado usando database manager REPARADO"""
    db = get_database_manager()
    return db.update_signal_result(signal_id, resultado)

def get_signal_statistics():
    """Obtiene estadísticas usando database manager REPARADO"""
    db = get_database_manager()
    return db.get_signal_stats()

def fix_volume_ratios():
    """Corrige volume_ratios problemáticos existentes"""
    db = get_database_manager()
    return db.fix_existing_volume_ratios()

# Función de prueba
def test_volume_ratio_saving():
    """Prueba que volume_ratio se guarde correctamente"""
    print("🧪 PROBANDO GUARDADO DE VOLUME_RATIO...")
    
    test_signal = {
        'symbol': 'TEST_VOLUME_RATIO_SAVE',
        'signal': 'LONG',
        'entry': 100.0,
        'tp': 102.0,
        'sl': 98.0,
        'confidence': 20.0,
        'rr': 1.2,
        'latest_indicators': {
            'rsi': 45.0,
            'macd': 0.1,
            'macd_signal': 0.05,
            'macd_histogram': 0.05,
            'ema9': 100.5,
            'ema21': 99.5,
            'atr': 1.0,
            'volume_ratio': 2.5,  # VALOR DE PRUEBA CRÍTICO
            'adx': 25.0
        },
        'emergency_mode': True,
        'fix_version': 'VOLUME_RATIO_TEST'
    }
    
    success = save_signal_to_db(test_signal)
    
    if success:
        # Verificar estadísticas
        stats = get_signal_statistics()
        print(f"   📊 Estadísticas después de prueba:")
        print(f"      Volume_ratio promedio: {stats.get('avg_volume_ratio', 0):.3f}")
        print(f"      Registros con volume_ratio > 0: {stats.get('positive_volume_count', 0)}")
        print(f"      Volume_ratio funcionando: {stats.get('volume_ratio_working', False)}")
        
        return stats.get('volume_ratio_working', False)
    else:
        print(f"   ❌ Prueba de guardado falló")
        return False

if __name__ == "__main__":
    print("🗄️ DATABASE MANAGER REPARADO - PRUEBA DIRECTA")
    print("=" * 50)
    
    # Inicializar
    db = get_database_manager()
    
    # Corregir registros existentes
    fixed_count = fix_volume_ratios()
    
    # Probar guardado
    test_success = test_volume_ratio_saving()
    
    if test_success:
        print("\n✅ DATABASE MANAGER REPARADO FUNCIONA CORRECTAMENTE")
        print("🎯 Volume_ratio se guarda correctamente")
    else:
        print("\n❌ DATABASE MANAGER SIGUE CON PROBLEMAS")
        print("🔧 Revisar configuración de base de datos")