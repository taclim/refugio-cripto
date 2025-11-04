#!/usr/bin/env python3
"""
Auto-sincronización de signals.db a GitHub
Monitorea cambios en signals.db y sincroniza automáticamente
Detecta: nuevas señales, señales cerradas, cambios en estado
"""

import sqlite3
import os
import subprocess
import time
import hashlib
from datetime import datetime
import json

class SignalsSyncMonitor:
    def __init__(self):
        self.db_path = 'signals.db'
        self.last_hash = None
        self.last_signal_count = 0
        self.last_active_count = 0
        self.last_closed_count = 0
        self.sync_interval = 10  # Verificar cada 10 segundos
        
        print("\n" + "="*80)
        print("🔄 AUTO-SINCRONIZACIÓN DE signals.db A GITHUB")
        print("="*80)
        print(f"📊 Base de datos: {self.db_path}")
        print(f"⏱️  Intervalo de verificación: {self.sync_interval}s")
        print("="*80 + "\n")
    
    def get_db_hash(self):
        """Calcula hash del archivo signals.db"""
        try:
            with open(self.db_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            print(f"❌ Error calculando hash: {e}")
            return None
    
    def get_signal_stats(self):
        """Obtiene estadísticas de signals.db"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total de señales
            cursor.execute("SELECT COUNT(*) FROM signals")
            total = cursor.fetchone()[0]
            
            # Señales activas
            cursor.execute("SELECT COUNT(*) FROM signals WHERE status = 'active'")
            active = cursor.fetchone()[0]
            
            # Señales cerradas
            cursor.execute("SELECT COUNT(*) FROM signals WHERE status = 'closed' OR resultado IS NOT NULL")
            closed = cursor.fetchone()[0]
            
            # Últimas señales
            cursor.execute("""
                SELECT id, symbol, signal_type, status, resultado, created_at 
                FROM signals 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            recent = cursor.fetchall()
            
            conn.close()
            
            return {
                'total': total,
                'active': active,
                'closed': closed,
                'recent': recent
            }
        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return None
    
    def detect_changes(self):
        """Detecta cambios en signals.db"""
        stats = self.get_signal_stats()
        if not stats:
            return False, "No se pudo leer signals.db"
        
        changes = []
        
        # Detectar nuevas señales
        if stats['total'] > self.last_signal_count:
            new_signals = stats['total'] - self.last_signal_count
            changes.append(f"✨ {new_signals} nueva(s) señal(es)")
        
        # Detectar nuevas señales activas
        if stats['active'] > self.last_active_count:
            new_active = stats['active'] - self.last_active_count
            changes.append(f"🟢 {new_active} señal(es) activada(s)")
        elif stats['active'] < self.last_active_count:
            closed_active = self.last_active_count - stats['active']
            changes.append(f"🔴 {closed_active} señal(es) cerrada(s)")
        
        # Detectar señales cerradas
        if stats['closed'] > self.last_closed_count:
            new_closed = stats['closed'] - self.last_closed_count
            changes.append(f"✅ {new_closed} señal(es) con resultado")
        
        # Actualizar contadores
        self.last_signal_count = stats['total']
        self.last_active_count = stats['active']
        self.last_closed_count = stats['closed']
        
        return len(changes) > 0, changes
    
    def sync_to_github(self):
        """Sincroniza signals.db a GitHub"""
        try:
            print("\n" + "🔄 SINCRONIZANDO A GITHUB...")
            
            # Verificar que signals.db existe
            if not os.path.exists(self.db_path):
                print(f"❌ {self.db_path} no encontrado")
                return False
            
            # Agregar signals.db
            print("📤 Agregando signals.db a Git...")
            result = subprocess.run(
                'git add signals.db',
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"⚠️  Error en git add: {result.stderr[:100]}")
                return False
            
            # Verificar si hay cambios
            result = subprocess.run(
                'git status --porcelain',
                shell=True,
                capture_output=True,
                text=True
            )
            
            if not result.stdout.strip():
                print("ℹ️  No hay cambios en signals.db")
                return True
            
            # Crear commit
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            commit_msg = f"🔄 Auto-sync: signals.db actualizado ({timestamp})"
            
            print(f"💾 Creando commit: {commit_msg}")
            result = subprocess.run(
                f'git commit -m "{commit_msg}"',
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"⚠️  Error en git commit: {result.stderr[:100]}")
                return False
            
            # Push a GitHub
            print("🚀 Enviando a GitHub...")
            result = subprocess.run(
                'git push -u origin main',
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print("✅ Sincronización exitosa")
                print("📊 Vercel se actualizará automáticamente en 1-2 minutos")
                return True
            else:
                print(f"❌ Error en git push: {result.stderr[:100]}")
                return False
        
        except Exception as e:
            print(f"❌ Error sincronizando: {e}")
            return False
    
    def print_stats(self):
        """Imprime estadísticas actuales"""
        stats = self.get_signal_stats()
        if not stats:
            return
        
        print(f"\n📊 ESTADÍSTICAS ACTUALES:")
        print(f"   📈 Total de señales: {stats['total']}")
        print(f"   🟢 Señales activas: {stats['active']}")
        print(f"   ✅ Señales cerradas: {stats['closed']}")
        
        if stats['recent']:
            print(f"\n   📋 Últimas 5 señales:")
            for signal in stats['recent']:
                sig_id, symbol, sig_type, status, resultado, created_at = signal
                status_emoji = "🟢" if status == 'active' else "✅" if resultado else "⏳"
                print(f"      {status_emoji} ID={sig_id} | {symbol} | {sig_type} | {status}")
    
    def run(self):
        """Ejecuta el monitor continuamente"""
        print("🚀 Iniciando monitor de sincronización...\n")
        
        # Inicializar estadísticas
        stats = self.get_signal_stats()
        if stats:
            self.last_signal_count = stats['total']
            self.last_active_count = stats['active']
            self.last_closed_count = stats['closed']
            self.print_stats()
        
        iteration = 0
        
        try:
            while True:
                iteration += 1
                
                # Verificar cambios
                has_changes, changes = self.detect_changes()
                
                if has_changes:
                    print(f"\n⏰ [{datetime.now().strftime('%H:%M:%S')}] Cambios detectados:")
                    for change in changes:
                        print(f"   {change}")
                    
                    # Sincronizar a GitHub
                    self.sync_to_github()
                    
                    # Mostrar estadísticas actualizadas
                    self.print_stats()
                else:
                    # Mostrar estado cada 30 segundos (3 iteraciones de 10s)
                    if iteration % 3 == 0:
                        print(f"⏰ [{datetime.now().strftime('%H:%M:%S')}] ✅ Sin cambios - Monitoreando...")
                
                # Esperar antes de siguiente verificación
                time.sleep(self.sync_interval)
        
        except KeyboardInterrupt:
            print("\n\n⏹️  Monitor detenido por el usuario")
        except Exception as e:
            print(f"\n❌ Error en monitor: {e}")

if __name__ == "__main__":
    monitor = SignalsSyncMonitor()
    monitor.run()
