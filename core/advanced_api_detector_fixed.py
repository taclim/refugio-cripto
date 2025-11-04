# core/advanced_api_detector_fixed.py - Detector con fallback automático mejorado
import requests
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json

class AdvancedAPIDetectorFixed:
    """Detector avanzado de APIs con fallback automático mejorado + BALANCEO"""

    def __init__(self):
        # Importar balanceador
        from core.api_balancer import get_api_balancer
        self.balancer = get_api_balancer()

        # Configuración de APIs con prioridades y límites - SOLO FUTUROS USDT-M
        self.apis = {
            # 🥇 PRIORIDAD 1 - Binance Futures USDT-M (1,200 req/min)
            'binance_futures': {
                'name': 'Binance Futures',
                'priority': 1,
                'rate_limit': 1200,  # req/min
                'url_template': 'https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}',
                'format_symbol': self._format_binance_futures,
                'price_path': 'price',
                'timeout': 3,
                'weight': 1
            },
            
            # 🥈 PRIORIDAD 2 - Bybit Futures USDT-M (600 req/min)
            'bybit_futures': {
                'name': 'Bybit Futures',
                'priority': 2,
                'rate_limit': 600,  # req/min
                'url_template': 'https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}',
                'format_symbol': self._format_bybit_futures,
                'price_path': 'result.list.0.lastPrice',
                'timeout': 4,
                'weight': 1
            },
            
            # 🥉 PRIORIDAD 3 - OKX Futures USDT-M (600 req/min)
            'okx_futures': {
                'name': 'OKX Futures',
                'priority': 3,
                'rate_limit': 600,  # req/min
                'url_template': 'https://www.okx.com/api/v5/market/ticker?instId={symbol}',
                'format_symbol': self._format_okx_futures,
                'price_path': 'data.0.last',
                'timeout': 4,
                'weight': 1
            },
            
            # ⚠️ PRIORIDAD 4 - KuCoin Futures USDT-M (600 req/min)
            'kucoin_futures': {
                'name': 'KuCoin Futures',
                'priority': 4,
                'rate_limit': 600,  # req/min
                'url_template': 'https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}',
                'format_symbol': self._format_kucoin_futures,
                'price_path': 'data.price',
                'timeout': 5,
                'weight': 1
            },
            
            # 🔄 PRIORIDAD 5 - Gate.io Futures USDT-M (600 req/min)
            'gate_futures': {
                'name': 'Gate.io Futures',
                'priority': 5,
                'rate_limit': 600,  # req/min
                'url_template': 'https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={symbol}',
                'format_symbol': self._format_gate_futures,
                'price_path': 'last_price',
                'timeout': 5,
                'weight': 1
            },
            
            # 🔄 PRIORIDAD 6 - MEXC Futures USDT-M (600 req/min)
            'mexc_futures': {
                'name': 'MEXC Futures',
                'priority': 6,
                'rate_limit': 600,  # req/min
                'url_template': 'https://contract.mexc.com/api/v1/contract/ticker?symbol={symbol}',
                'format_symbol': self._format_mexc_futures,
                'price_path': 'data.0.lastPrice',
                'timeout': 5,
                'weight': 1
            },
            
            # 🔄 PRIORIDAD 7 - Bitfinex Futures USDT-M (600 req/min)
            'bitfinex_futures': {
                'name': 'Bitfinex Futures',
                'priority': 7,
                'rate_limit': 600,  # req/min
                'url_template': 'https://api-pub.bitfinex.com/v2/ticker/t{symbol}',
                'format_symbol': self._format_bitfinex_futures,
                'price_path': '6',
                'timeout': 5,
                'weight': 1
            },
            
            # 🔄 PRIORIDAD 8 - Coinbase Futures USDT-M (600 req/min)
            'coinbase_futures': {
                'name': 'Coinbase Futures',
                'priority': 8,
                'rate_limit': 600,  # req/min
                'url_template': 'https://api.exchange.coinbase.com/products/{symbol}/ticker',
                'format_symbol': self._format_coinbase_futures,
                'price_path': 'price',
                'timeout': 5,
                'weight': 1
            },
            
            # 🔄 PRIORIDAD 9 - Pionex Futures USDT-M (600 req/min)
            'pionex_futures': {
                'name': 'Pionex Futures',
                'priority': 9,
                'rate_limit': 600,  # req/min
                'url_template': 'https://api.pionex.com/api/v1/ticker?symbol={symbol}',
                'format_symbol': self._format_pionex_futures,
                'price_path': 'data.last',
                'timeout': 5,
                'weight': 1
            }
        }
        
        # Control de rate limiting
        self.api_usage = {}  # Contador de uso por API
        self.api_reset_time = {}  # Tiempo de reset por API
        self.failed_combinations = set()  # Combinaciones fallidas
        self.token_api_mapping = {}  # Mapeo de tokens a APIs
        self.api_health = {}  # Estado de salud de APIs
        self.failure_timestamps = {}  # Timestamps de fallos para retry
        
        # Inicializar contadores
        for api_id in self.apis.keys():
            self.api_usage[api_id] = 0
            self.api_reset_time[api_id] = datetime.now()
            self.api_health[api_id] = {'status': 'healthy', 'last_check': datetime.now(), 'avg_response_time': 0.0, 'response_count': 0}
        
        print("🚀 Detector avanzado CORREGIDO inicializado")
        print(f"📊 APIs configuradas: {len(self.apis)}")
        self._print_api_priorities()
    
    def _print_api_priorities(self):
        """Muestra las prioridades de APIs"""
        print("\n🏆 PRIORIDADES DE APIs:")
        sorted_apis = sorted(self.apis.items(), key=lambda x: x[1]['priority'])
        for api_id, config in sorted_apis:
            priority_emoji = {1: "🥇", 2: "🥈", 3: "����", 4: "⚠️", 5: "🔄", 6: "🆘"}
            emoji = priority_emoji.get(config['priority'], "📡")
            print(f"   {emoji} {config['name']}: {config['rate_limit']} req/min (Prioridad {config['priority']})")
    
    # Formateadores de símbolos para cada exchange
    def _format_binance_spot(self, mexc_symbol: str) -> str:
        """BTCUSDT"""
        return mexc_symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
    
    def _format_binance_futures(self, mexc_symbol: str) -> str:
        """BTCUSDT"""
        return mexc_symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
    
    def _format_gate_spot(self, mexc_symbol: str) -> str:
        """BTC_USDT"""
        return mexc_symbol.replace('/USDT:USDT', '_USDT').replace('/', '_')
    
    def _format_okx_spot(self, mexc_symbol: str) -> str:
        """BTC-USDT"""
        return mexc_symbol.replace('/USDT:USDT', '-USDT').replace('/', '-')
    
    def _format_mexc_spot(self, mexc_symbol: str) -> str:
        """BTCUSDT"""
        return mexc_symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
    
    def _format_mexc_futures(self, mexc_symbol: str) -> str:
        """BTC_USDT"""
        return mexc_symbol.replace('/USDT:USDT', '_USDT').replace('/', '_')
    
    def _format_mexc_futures_fallback(self, mexc_symbol: str) -> str:
        """BTC_USDT"""
        return mexc_symbol.replace('/USDT:USDT', '_USDT').replace('/', '_')
    
    def _format_bybit_futures(self, mexc_symbol: str) -> str:
        """BTCUSDT"""
        return mexc_symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
    
    def _format_okx_futures(self, mexc_symbol: str) -> str:
        """BTC-USDT"""
        return mexc_symbol.replace('/USDT:USDT', '-USDT').replace('/', '-')
    
    def _format_kucoin_futures(self, mexc_symbol: str) -> str:
        """BTCUSDTM"""
        base = mexc_symbol.split('/')[0]
        return f"{base}USDTM"
    
    def _format_gate_futures(self, mexc_symbol: str) -> str:
        """BTC_USDT"""
        return mexc_symbol.replace('/USDT:USDT', '_USDT').replace('/', '_')
    
    def _format_bitfinex_futures(self, mexc_symbol: str) -> str:
        """BTCF0:USTF0"""
        base = mexc_symbol.split('/')[0]
        return f"{base}F0:USTF0"
    
    def _format_coinbase_futures(self, mexc_symbol: str) -> str:
        """BTC-USD"""
        base = mexc_symbol.split('/')[0]
        return f"{base}-USD"
    
    def _format_pionex_futures(self, mexc_symbol: str) -> str:
        """BTC_USDT"""
        return mexc_symbol.replace('/USDT:USDT', '_USDT').replace('/', '_')
    
    def _can_use_api(self, api_id: str) -> bool:
        """Verifica si se puede usar una API (rate limiting)"""
        now = datetime.now()
        config = self.apis[api_id]
        
        # Reset contador si ha pasado un minuto
        if now - self.api_reset_time[api_id] >= timedelta(minutes=1):
            self.api_usage[api_id] = 0
            self.api_reset_time[api_id] = now
        
        # Verificar límite (usar 80% del límite para seguridad)
        safe_limit = int(config['rate_limit'] * 0.8)
        return self.api_usage[api_id] < safe_limit
    
    def _increment_api_usage(self, api_id: str, weight: int = 1):
        """Incrementa el contador de uso de una API"""
        self.api_usage[api_id] += weight
    
    def _should_retry_failed_combination(self, mexc_symbol: str, api_id: str) -> bool:
        """Verifica si se debe reintentar una combinación fallida después de un tiempo"""
        combination_key = f"{mexc_symbol}_{api_id}"
        
        if combination_key not in self.failure_timestamps:
            return True
        
        # Reintentar después de 5 minutos
        last_failure = self.failure_timestamps[combination_key]
        return datetime.now() - last_failure >= timedelta(minutes=5)
    
    def _mark_combination_failed(self, mexc_symbol: str, api_id: str):
        """Marca una combinación como fallida con timestamp"""
        combination_key = f"{mexc_symbol}_{api_id}"
        self.failure_timestamps[combination_key] = datetime.now()
        self.failed_combinations.add((mexc_symbol, api_id))
    
    def _get_nested_value(self, data: dict, path: str):
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
    
    def _test_api_endpoint(self, api_id: str, mexc_symbol: str) -> Tuple[Optional[float], str]:
        """Prueba un endpoint específico"""
        try:
            config = self.apis[api_id]
            
            # Verificar rate limiting
            if not self._can_use_api(api_id):
                return None, f"{config['name']}: Rate limit alcanzado"
            
            # Formatear símbolo
            formatted_symbol = config['format_symbol'](mexc_symbol)
            url = config['url_template'].format(symbol=formatted_symbol)
            
            # Realizar petición
            start_time = time.time()
            response = requests.get(url, timeout=config['timeout'])
            response_time = time.time() - start_time
            self._increment_api_usage(api_id, config['weight'])
            
            if response.status_code == 200:
                data = response.json()
                
                # Extraer precio usando el path configurado
                if '.' in config['price_path']:
                    price = self._get_nested_value(data, config['price_path'])
                else:
                    # Para arrays simples como Gate.io
                    if isinstance(data, list) and len(data) > 0:
                        price = float(data[0].get(config['price_path'], 0))
                    else:
                        price = float(data.get(config['price_path'], 0))
                
                if price and price > 0:
                    # Actualizar salud y tiempo de respuesta
                    self.api_health[api_id]['status'] = 'healthy'
                    self.api_health[api_id]['last_check'] = datetime.now()
                    old_avg = self.api_health[api_id]['avg_response_time']
                    count = self.api_health[api_id]['response_count'] + 1
                    new_avg = (old_avg * (count - 1) + response_time) / count
                    self.api_health[api_id]['avg_response_time'] = new_avg
                    self.api_health[api_id]['response_count'] = count
                    return price, config['name']
                else:
                    return None, f"{config['name']}: Precio inválido ({price})"
            else:
                return None, f"{config['name']}: HTTP {response.status_code}"
                
        except requests.exceptions.Timeout:
            return None, f"{config['name']}: Timeout"
        except requests.exceptions.RequestException as e:
            return None, f"{config['name']}: Error de conexión"
        except Exception as e:
            return None, f"{config['name']}: Error - {str(e)}"
    
    def detect_best_api_for_token(self, mexc_symbol: str) -> str:
        """
        Detecta la mejor API disponible usando BALANCEADOR INTELIGENTE
        Evita saturación distribuyendo carga entre múltiples APIs
        """

        print(f"⚖️ {mexc_symbol}: Usando balanceador inteligente...")

        # 1. PRIMERO: Usar balanceador para asignar API
        try:
            assigned_api, was_reassigned = self.balancer.assign_api_to_token(mexc_symbol)

            if assigned_api:
                api_name = self.apis[assigned_api]['name']
                status = "🔄 Reasignado" if was_reassigned else "✅ Asignado"
                print(f"   {status}: {mexc_symbol} → {api_name} (Balanceado)")

                # Verificar que la API asignada funcione
                if self._can_use_api(assigned_api):
                    price, test_status = self._test_api_endpoint(assigned_api, mexc_symbol)
                    if price is not None:
                        print(f"   ✅ API balanceada funciona: ${price:.6f}")
                        return assigned_api
                    else:
                        print(f"   ⚠️ API balanceada falló: {test_status}")
                        self._mark_combination_failed(mexc_symbol, assigned_api)
                else:
                    print(f"   🚫 API balanceada en rate limit: {api_name}")
                    self._mark_combination_failed(mexc_symbol, assigned_api)

        except Exception as e:
            print(f"   ❌ Error en balanceador: {e}")

        # 2. FALLBACK: Sistema tradicional si balanceador falla
        print(f"   🔄 Balanceador falló, usando fallback tradicional...")

        # Probar APIs en orden de prioridad
        sorted_apis = sorted(self.apis.items(), key=lambda x: (x[1]['priority'], self.api_health.get(x[0], {}).get('avg_response_time', 0.0)))

        for api_id, config in sorted_apis:
            # Saltar si está marcado como fallido recientemente
            if not self._should_retry_failed_combination(mexc_symbol, api_id):
                continue

            # Saltar si no se puede usar por rate limiting
            if not self._can_use_api(api_id):
                continue

            # Probar API
            price, status = self._test_api_endpoint(api_id, mexc_symbol)

            if price is not None:
                # Éxito - guardar mapeo y limpiar fallos anteriores
                self.token_api_mapping[mexc_symbol] = api_id
                combination_key = f"{mexc_symbol}_{api_id}"
                if combination_key in self.failure_timestamps:
                    del self.failure_timestamps[combination_key]
                self.failed_combinations.discard((mexc_symbol, api_id))

                print(f"✅ {mexc_symbol} → {config['name']} (${price:.6f}) [Fallback]")
                return api_id
            else:
                # Fallo - marcar como fallido
                self._mark_combination_failed(mexc_symbol, api_id)

        # Si llegamos aquí, ninguna API funcionó
        print(f"🆘 {mexc_symbol}: TODAS LAS APIs FALLARON - Token no disponible")
        return None
    
    def get_current_price(self, mexc_symbol: str, api_id: str = None) -> Tuple[Optional[float], str]:
        """Obtiene precio actual CON FALLBACK AUTOMÁTICO"""
        
        # Si no se especifica API, detectar automáticamente
        if api_id is None:
            api_id = self.detect_best_api_for_token(mexc_symbol)
            if api_id is None:
                return None, "Token no disponible en ningún exchange"
        
        # Usar API específica con fallback
        price, status = self._test_api_endpoint(api_id, mexc_symbol)
        
        # Si la API específica falla, buscar alternativa automáticamente
        if price is None and "HTTP 400" in status:
            print(f"⚠️ {mexc_symbol}: API {self.apis[api_id]['name']} falló, buscando alternativa...")
            self._mark_combination_failed(mexc_symbol, api_id)
            
            # Buscar alternativa automáticamente
            alternative_api = self.detect_best_api_for_token(mexc_symbol)
            if alternative_api and alternative_api != api_id:
                return self._test_api_endpoint(alternative_api, mexc_symbol)
        
        return price, status
    
    def get_detection_stats(self) -> Dict:
        """Obtiene estadísticas del detector"""
        now = datetime.now()
        
        # Calcular uso actual de APIs
        api_usage_stats = {}
        for api_id, config in self.apis.items():
            usage_pct = (self.api_usage[api_id] / (config['rate_limit'] * 0.8)) * 100
            api_usage_stats[api_id] = {
                'name': config['name'],
                'usage': self.api_usage[api_id],
                'limit': config['rate_limit'],
                'usage_pct': round(usage_pct, 1),
                'can_use': self._can_use_api(api_id),
                'health': self.api_health[api_id]['status']
            }
        
        return {
            'total_apis': len(self.apis),
            'tokens_mapped': len(self.token_api_mapping),
            'failed_combinations': len(self.failed_combinations),
            'api_usage': api_usage_stats,
            'token_mappings': self.token_api_mapping.copy(),
            'timestamp': now.isoformat()
        }
    
    def print_usage_stats(self):
        """Imprime estadísticas de uso"""
        stats = self.get_detection_stats()
        
        print(f"\n📊 ESTADÍSTICAS DE APIs:")
        print(f"   🎯 Tokens mapeados: {stats['tokens_mapped']}")
        print(f"   ❌ Combinaciones fallidas: {stats['failed_combinations']}")
        
        print(f"\n📈 USO DE APIs:")
        for api_id, api_stats in stats['api_usage'].items():
            status_emoji = "✅" if api_stats['can_use'] else "🚫"
            health_emoji = "💚" if api_stats['health'] == 'healthy' else "💔"
            print(f"   {status_emoji}{health_emoji} {api_stats['name']}: {api_stats['usage']}/{api_stats['limit']} ({api_stats['usage_pct']}%)")

    def print_api_assignment_for_signal(self, mexc_symbol: str, api_id: str):
        """Muestra asignación de API solo cuando se asigna a una señal específica"""
        if api_id and api_id in self.apis:
            api_name = self.apis[api_id]['name']
            usage = self.api_usage.get(api_id, 0)
            limit = self.apis[api_id]['rate_limit']
            usage_pct = (usage / (limit * 0.8)) * 100
            
            print(f"\n📈 API ASIGNADA PARA SEGUIMIENTO:")
            print(f"   🎯 Token: {mexc_symbol}")
            print(f"   📡 API: {api_name}")
            print(f"   📊 Uso actual: {usage}/{limit} ({usage_pct:.1f}%)")

# Instancia global
_advanced_api_detector_fixed = None

def get_advanced_api_detector_fixed() -> AdvancedAPIDetectorFixed:
    """Singleton para el detector avanzado corregido"""
    global _advanced_api_detector_fixed
    if _advanced_api_detector_fixed is None:
        _advanced_api_detector_fixed = AdvancedAPIDetectorFixed()
    return _advanced_api_detector_fixed