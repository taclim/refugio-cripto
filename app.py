"""
Backend para REFUGIO CRIPTO Dashboard
Usa las MISMAS APIs que el bot para obtener precios en tiempo real
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
from datetime import datetime, timedelta
import os
import requests
import json
import threading
import time

app = Flask(__name__)

# ✅ CONFIGURAR CORS CORRECTAMENTE PARA NGROK Y VERCEL
CORS(app, 
     origins="*",
     allow_headers=["Content-Type", "Authorization", "Cache-Control", "Pragma", "Expires"],
     methods=["GET", "POST", "OPTIONS"],
     supports_credentials=False,
     max_age=3600)

# ✅ MANEJAR PREFLIGHT REQUESTS (OPTIONS)
@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        response = jsonify({'status': 'ok'})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Cache-Control, Pragma, Expires'
        response.headers['Access-Control-Max-Age'] = '3600'
        return response, 200

# ✅ AGREGAR HEADERS CORS A TODAS LAS RESPUESTAS
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Cache-Control, Pragma, Expires'
    response.headers['Access-Control-Max-Age'] = '3600'
    response.headers['Access-Control-Expose-Headers'] = 'Content-Type'
    return response

DATABASE_PATH = 'signals.db'

# Caché de precios
PRICE_CACHE = {}
CACHE_EXPIRY = {}
CACHE_TTL = 30  # 30 segundos

# Configuración de APIs (mismas que el bot)
APIS_CONFIG = {
    'binance_futures': {
        'name': 'Binance Futures',
        'url_template': 'https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}',
        'format_symbol': lambda s: s.replace('/USDT:USDT', 'USDT').replace('/', ''),
        'price_path': 'price',
        'timeout': 3
    },
    'mexc_futures': {
        'name': 'MEXC Futures',
        'url_template': 'https://contract.mexc.com/api/v1/contract/ticker?symbol={symbol}',
        'format_symbol': lambda s: s.replace('/USDT:USDT', '_USDT').replace('/', '_'),
        'price_path': 'data.0.lastPrice',
        'timeout': 5
    },
    'gate_futures': {
        'name': 'Gate.io Futures',
        'url_template': 'https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={symbol}',
        'format_symbol': lambda s: s.replace('/USDT:USDT', '_USDT').replace('/', '_'),
        'price_path': 'last_price',
        'timeout': 5
    },
    'okx_futures': {
        'name': 'OKX Futures',
        'url_template': 'https://www.okx.com/api/v5/market/ticker?instId={symbol}',
        'format_symbol': lambda s: s.replace('/USDT:USDT', '-USDT').replace('/', '-'),
        'price_path': 'data.0.last',
        'timeout': 4
    },
    'kucoin_futures': {
        'name': 'KuCoin Futures',
        'url_template': 'https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}',
        'format_symbol': lambda s: s.split('/')[0] + 'USDTM',
        'price_path': 'data.price',
        'timeout': 5
    },
    'bybit_futures': {
        'name': 'Bybit Futures',
        'url_template': 'https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}',
        'format_symbol': lambda s: s.replace('/USDT:USDT', 'USDT').replace('/', ''),
        'price_path': 'result.list.0.lastPrice',
        'timeout': 4
    }
}

# Cargar mapeo de tokens a APIs
TOKEN_API_MAPPING = {}
try:
    if os.path.exists('token_api_mapping.json'):
        with open('token_api_mapping.json', 'r') as f:
            data = json.load(f)
            TOKEN_API_MAPPING = data.get('mapping', {})
            print(f"✅ Mapeo de tokens cargado: {len(TOKEN_API_MAPPING)} tokens")
    else:
        print("⚠️  token_api_mapping.json no encontrado")
except Exception as e:
    print(f"❌ Error cargando mapeo de tokens: {e}")

def get_nested_value(data, path):
    """Obtiene valor anidado usando notación de puntos"""
    try:
        # Si data es una lista, acceder al primer elemento
        if isinstance(data, list):
            if len(data) == 0:
                return None
            data = data[0]
        
        # Si data no es un diccionario, retornar None
        if not isinstance(data, dict):
            return None
        
        keys = path.split('.')
        current = data
        
        for key in keys:
            if key.isdigit():
                # Acceso a índice de lista
                if isinstance(current, list):
                    current = current[int(key)]
                else:
                    return None
            else:
                # Acceso a clave de diccionario
                if isinstance(current, dict):
                    current = current.get(key)
                    if current is None:
                        return None
                else:
                    return None
        
        return float(current) if current is not None else None
    except (KeyError, IndexError, ValueError, TypeError, AttributeError):
        return None

def get_current_price(symbol):
    """
    Obtiene precio actual usando las APIs del bot
    Usa el mapeo de tokens y fallback automático
    """
    try:
        # Obtener API asignada para este token
        assigned_api = TOKEN_API_MAPPING.get(symbol)
        
        # Crear lista de APIs a intentar
        apis_to_try = []
        if assigned_api and assigned_api in APIS_CONFIG:
            apis_to_try.append(assigned_api)
        
        # Agregar otras APIs como fallback
        for api_id in APIS_CONFIG.keys():
            if api_id not in apis_to_try:
                apis_to_try.append(api_id)
        
        # Intentar obtener precio de cada API
        for api_id in apis_to_try:
            try:
                config = APIS_CONFIG[api_id]
                
                # Formatear símbolo para esta API
                formatted_symbol = config['format_symbol'](f"{symbol}/USDT:USDT")
                url = config['url_template'].format(symbol=formatted_symbol)
                
                print(f"   📡 Intentando {config['name']} para {symbol}...")
                
                # Realizar petición
                response = requests.get(url, timeout=config['timeout'])
                
                if response.status_code == 200:
                    data = response.json()
                    price = None
                    
                    # Extraer precio
                    try:
                        if '.' in config['price_path']:
                            price = get_nested_value(data, config['price_path'])
                        else:
                            # Manejar tanto diccionarios como listas
                            if isinstance(data, list) and len(data) > 0:
                                price = float(data[0].get(config['price_path'], 0))
                            elif isinstance(data, dict):
                                price = float(data.get(config['price_path'], 0))
                            else:
                                price = 0
                    except (ValueError, TypeError):
                        price = None
                    
                    # Validar precio
                    if price is not None and price > 0:
                        print(f"   ✅ {symbol}: ${price:.6f} (desde {config['name']})")
                        return price
                    else:
                        if price == 0:
                            print(f"   ⚠️  {config['name']}: Precio es 0 (token no disponible)")
                        else:
                            print(f"   ⚠️  {config['name']}: Precio inválido ({price})")
                else:
                    print(f"   ⚠️  {config['name']}: HTTP {response.status_code}")
                    
            except requests.exceptions.Timeout:
                print(f"   ⚠️  {config['name']}: Timeout")
            except requests.exceptions.RequestException as e:
                print(f"   ⚠️  {config['name']}: Error de conexión")
            except Exception as e:
                print(f"   ⚠️  {config['name']}: Error - {str(e)[:50]}")
        
        print(f"   ❌ {symbol}: No se pudo obtener precio de ninguna API")
        return None
        
    except Exception as e:
        print(f"❌ Error obteniendo precio para {symbol}: {e}")
        return None

def get_db_connection():
    """Conecta a la base de datos"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_active_signals():
    """Obtiene todas las señales activas con precios actuales"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener columnas disponibles
        cursor.execute("PRAGMA table_info(signals)")
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]
        
        # Construir consulta
        select_fields = [
            'id', 'symbol', 'signal_type', 'entry', 'tp1', 'sl', 
            'confidence', 'status', 'created_at', 'ma_type', 'ma_length'
        ]
        
        query = f"""
            SELECT {', '.join(select_fields)}
            FROM signals
            WHERE status = 'active'
            ORDER BY created_at DESC
            LIMIT 20
        """
        
        cursor.execute(query)
        
        signals = []
        for row in cursor.fetchall():
            signal = dict(row)
            
            # Limpiar símbolo
            symbol = signal['symbol'].replace(':USDT', '').replace('/USDT', '')
            signal['symbol'] = symbol
            
            # Convertir a mayúsculas
            signal['type'] = signal['signal_type'].upper()
            
            # Asegurar valores numéricos
            signal['entry'] = float(signal['entry']) if signal['entry'] else 0
            signal['tp'] = float(signal['tp1']) if signal['tp1'] else 0
            signal['sl'] = float(signal['sl']) if signal['sl'] else 0
            signal['confidence'] = float(signal['confidence']) if signal['confidence'] else 50
            
            # OBTENER PRECIO ACTUAL DESDE LAS APIs
            print(f"\n🔍 Obteniendo precio para {symbol}...")
            current_price = get_current_price(symbol)
            
            if current_price:
                signal['current'] = current_price
            else:
                # Fallback a precio de entrada
                signal['current'] = signal['entry']
                print(f"⚠️  {symbol}: Usando precio de entrada = {signal['entry']}")
            
            signals.append(signal)
        
        conn.close()
        print(f"\n✅ {len(signals)} señales cargadas con precios actuales\n")
        return signals
    
    except Exception as e:
        print(f"❌ Error obteniendo señales: {e}")
        return []

@app.route('/api/operations', methods=['GET'])
def get_operations():
    """Endpoint: GET /api/operations - Retorna operaciones con precios actuales"""
    try:
        print("\n" + "="*80)
        print("📡 SOLICITUD: /api/operations")
        print("="*80)
        
        # ✅ OBTENER SEÑALES ACTIVAS (funciona en Vercel y localhost)
        signals = get_active_signals()
        
        return jsonify({
            'success': True,
            'data': signals,
            'count': len(signals),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """Endpoint: GET /api/statistics - Retorna estadísticas COMPLETAS"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # TOTAL DE SEÑALES (todas)
        cursor.execute("SELECT COUNT(*) FROM signals")
        total_signals = cursor.fetchone()[0]
        
        # SEÑALES ACTIVAS
        cursor.execute("SELECT COUNT(*) FROM signals WHERE status = 'active' OR resultado IS NULL")
        active_signals = cursor.fetchone()[0]
        
        # SEÑALES CERRADAS
        cursor.execute("SELECT COUNT(*) FROM signals WHERE status = 'closed' OR resultado IS NOT NULL")
        closed_signals = cursor.fetchone()[0]
        
        # LONG vs SHORT (activas)
        cursor.execute("SELECT COUNT(*) FROM signals WHERE signal_type = 'LONG' AND (status = 'active' OR resultado IS NULL)")
        long_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM signals WHERE signal_type = 'SHORT' AND (status = 'active' OR resultado IS NULL)")
        short_count = cursor.fetchone()[0]
        
        conn.close()
        
        # Obtener señales activas para calcular potencial
        signals = get_active_signals()
        
        total_potential = 0
        for signal in signals:
            if signal['type'] == 'LONG':
                potential = ((signal['tp'] - signal['current']) / signal['current'] * 100) if signal['current'] > 0 else 0
            elif signal['type'] == 'SHORT':
                potential = ((signal['current'] - signal['tp']) / signal['current'] * 100) if signal['current'] > 0 else 0
            else:
                potential = 0
            total_potential += potential
        
        avg_potential = total_potential / len(signals) if signals else 0
        
        return jsonify({
            'success': True,
            'data': {
                'total_signals': total_signals,
                'active_operations': active_signals,
                'closed_signals': closed_signals,
                'long_count': long_count,
                'short_count': short_count,
                'total_potential_gain': round(total_potential, 2),
                'avg_potential_gain': round(avg_potential, 2)
            },
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/health', methods=['GET'])
def health():
    """Endpoint: GET /api/health - Verifica estado"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM signals WHERE status = 'active'")
        count = cursor.fetchone()[0]
        conn.close()
        
        return jsonify({
            'status': 'OK',
            'database': 'connected',
            'active_signals': count,
            'apis_configured': len(APIS_CONFIG),
            'tokens_mapped': len(TOKEN_API_MAPPING),
            'timestamp': datetime.now().isoformat()
        })
    except:
        return jsonify({
            'status': 'ERROR',
            'database': 'disconnected',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/', methods=['GET'])
def serve_dashboard():
    """Sirve el dashboard HTML"""
    try:
        with open('index.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        return html_content
    except FileNotFoundError:
        return jsonify({
            'error': 'Dashboard no encontrado',
            'message': 'El archivo index.html no existe'
        }), 404
    except Exception as e:
        return jsonify({
            'error': 'Error sirviendo dashboard',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    print("\n" + "="*80)
    print("🚀 Iniciando servidor REFUGIO CRIPTO")
    print("="*80)
    print(f"📊 Base de datos: {DATABASE_PATH}")
    print(f"🌐 Servidor: http://localhost:5000")
    print(f"📡 API: http://localhost:5000/api")
    print(f"🔄 APIs configuradas: {len(APIS_CONFIG)}")
    print(f"🎯 Tokens mapeados: {len(TOKEN_API_MAPPING)}")
    print(f"🔄 Usando las MISMAS APIs que el bot para obtener precios")
    print("="*80)
    print("\n✅ Servidor iniciado. Accede a http://localhost:5000\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
