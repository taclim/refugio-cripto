#!/usr/bin/env python3
"""
Script para sincronizar signals.db a GitHub automáticamente
Ejecutar después de que el bot actualice signals.db
"""

import subprocess
import os
from datetime import datetime

def sync_signals_db():
    """Sincroniza signals.db a GitHub"""
    
    try:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(project_dir)
        
        print(f"\n{'='*80}")
        print(f"🔄 SINCRONIZANDO signals.db A GITHUB")
        print(f"{'='*80}")
        print(f"📁 Directorio: {project_dir}")
        print(f"⏰ Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Verificar que signals.db existe
        if not os.path.exists('signals.db'):
            print("❌ signals.db no encontrado")
            return False
        
        print("✅ signals.db encontrado")
        
        # 1. Agregar signals.db
        print("\n📤 Agregando signals.db a Git...")
        result = subprocess.run(['git', 'add', 'signals.db'], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error: {result.stderr}")
            return False
        print("✅ signals.db agregado")
        
        # 2. Verificar si hay cambios
        print("\n🔍 Verificando cambios...")
        result = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
        if not result.stdout.strip():
            print("⚠️  No hay cambios para sincronizar")
            return True
        
        print(f"📝 Cambios detectados:\n{result.stdout}")
        
        # 3. Crear commit
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        commit_message = f"🔄 Actualización: signals.db sincronizado ({timestamp})"
        
        print(f"\n💾 Creando commit: {commit_message}")
        result = subprocess.run(['git', 'commit', '-m', commit_message], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error en commit: {result.stderr}")
            return False
        print("✅ Commit creado")
        
        # 4. Push a GitHub
        print("\n🚀 Enviando a GitHub...")
        result = subprocess.run(['git', 'push', '-u', 'origin', 'main'], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error en push: {result.stderr}")
            return False
        print("✅ Push completado")
        
        print(f"\n{'='*80}")
        print("✅ ✅ ✅ SINCRONIZACIÓN EXITOSA ✅ ✅ ✅")
        print(f"{'='*80}")
        print("📊 Vercel se actualizará automáticamente en 1-2 minutos")
        print(f"🌐 Dashboard: https://refugio-cripto.vercel.app")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == '__main__':
    success = sync_signals_db()
    exit(0 if success else 1)
