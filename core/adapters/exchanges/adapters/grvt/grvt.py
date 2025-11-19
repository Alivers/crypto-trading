"""
GRVT交易所适配器

基于MESA架构的GRVT适配器，提供统一的交易接口。
使用GRVT SDK (grvt-pysdk) 进行API交互和WebSocket连接。
整合了分离的模块：grvt_base.py、grvt_rest.py、grvt_websocket.py
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal

import yaml

# Logger will be set by ExchangeAdapter base class

from ...adapter import ExchangeAdapter
from ...interface import ExchangeConfig
from ...models import *
from ...subscription_manager import create_subscription_manager, DataType
from .grvt_base import GrvtBase
from .grvt_rest import GrvtRest
from .grvt_websocket import GrvtWebSocket


class GrvtAdapter(ExchangeAdapter):
    """GRVT交易所适配器 - 统一接口"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)

        config_file = self._load_grvt_config()
        config.extra_params = config_file.get('authentication', {})

        # 初始化各个模块（直接从ExchangeConfig获取配置）
        self._base = GrvtBase(config)
        self._rest = GrvtRest(config, self.logger)
        self._websocket = GrvtWebSocket(config, self.logger)
        
        # 设置logger
        self._base.set_logger(self.logger)
        
        # 设置基础URL
        self.base_url = getattr(config, 'base_url', None)
        self.ws_url = getattr(config, 'ws_url', None)
        
        # 符号映射
        self._symbol_mapping = getattr(config, 'symbol_mapping', {})
        
        # 连接状态
        self._connected = False
        self._authenticated = False
        
        # 缓存支持的交易对
        self._supported_symbols = []
        self._market_info = {}
        
        # 初始化订阅管理器（从ExchangeConfig构建配置字典）
        try:
            config_dict = self._build_subscription_config_dict(config)
            
            # 获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()
            
            self._subscription_manager = create_subscription_manager(
                exchange_config=config_dict,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )
            
            if self.logger:
                self.logger.info(
                    f"✅ GRVT订阅管理器初始化成功，模式: {config_dict.get('subscription_mode', {}).get('mode', 'unknown')}"
                )
        except Exception as e:
            if self.logger:
                self.logger.warning(f"创建GRVT订阅管理器失败，使用默认配置: {e}")
            # 使用默认配置
            default_config = self._get_default_config_dict()
            symbol_cache_service = self._get_symbol_cache_service()
            self._subscription_manager = create_subscription_manager(
                exchange_config=default_config,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )
    
    def _build_subscription_config_dict(self, config: ExchangeConfig) -> Dict[str, Any]:
        """从ExchangeConfig构建订阅管理器需要的配置字典"""
        # 从ExchangeConfig中提取订阅相关配置
        subscription_mode = getattr(config, 'subscription_mode', 'predefined')
        data_types = getattr(config, 'data_types', ['ticker', 'orderbook'])
        symbols = getattr(config, 'symbols', [])
        predefined_combinations = getattr(config, 'predefined_combinations', {})
        discovery_settings = getattr(config, 'discovery_settings', {})
        
        # 构建配置字典
        config_dict = {
            'exchange_id': 'grvt',
            'subscription_mode': {
                'mode': subscription_mode,
            }
        }
        
        if subscription_mode == 'predefined':
            # 构建数据类型字典
            data_types_dict = {}
            if isinstance(data_types, list):
                for dt in data_types:
                    data_types_dict[dt] = True
            else:
                data_types_dict = data_types
            
            config_dict['subscription_mode']['predefined'] = {
                'symbols': symbols,
                'data_types': data_types_dict
            }
        else:
            # 动态模式
            filter_criteria = discovery_settings.get('filters', {})
            config_dict['subscription_mode']['dynamic'] = {
                'discovery': {
                    'enabled': discovery_settings.get('enabled', True),
                    'filter_criteria': filter_criteria
                },
                'data_types': data_types if isinstance(data_types, dict) else {dt: True for dt in data_types},
                'dynamic_subscription': {
                    'auto_discover_interval': discovery_settings.get('auto_discover_interval', 600)
                }
            }
        
        # 添加自定义订阅组合
        if predefined_combinations:
            config_dict['custom_subscriptions'] = {
                'combinations': predefined_combinations,
                'active_combination': getattr(config, 'active_combination', 'major_coins')
            }
        
        return config_dict
    
    def _get_default_config_dict(self) -> Dict[str, Any]:
        """获取默认配置字典"""
        return {
            'exchange_id': 'grvt',
            'subscription_mode': {
                'mode': 'predefined',
                'predefined': {
                    'symbols': ['BTC_USDT_Perp', 'ETH_USDT_Perp', 'SOL_USDT_Perp'],
                    'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                }
            }
        }
    
    def _get_symbol_cache_service(self):
        """获取符号缓存服务实例"""
        try:
            # 尝试从依赖注入容器获取符号缓存服务
            from .....di.container import get_container
            from .....services.symbol_manager.interfaces.symbol_cache import ISymbolCacheService
            
            container = get_container()
            symbol_cache_service = container.get(ISymbolCacheService)
            
            if self.logger:
                self.logger.info("✅ 获取符号缓存服务成功")
            return symbol_cache_service
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ 获取符号缓存服务失败: {e}，返回None")
            return None

    def _load_grvt_config(self) -> Dict[str, Any]:
        """加载GRVT配置文件"""
        config_path = "config/exchanges/grvt_config.yaml"

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if self.logger:
                    self.logger.info(f"✅ 加载GRVT配置文件: {config_path}")
                return config.get('grvt', {})
        except FileNotFoundError:
            if self.logger:
                self.logger.warning(f"GRVT配置文件未找到: {config_path}")
            return {'exchange_id': 'grvt'}
        except Exception as e:
            if self.logger:
                self.logger.error(f"加载GRVT配置文件失败: {e}")
            return {'exchange_id': 'grvt'}
    
    async def _do_connect(self) -> bool:
        """连接实现"""
        try:
            # 初始化REST API
            rest_success = await self._rest.initialize()
            if not rest_success:
                self.logger.error("❌ GRVT REST API初始化失败")
                return False
            
            self._market_info = self._rest._market_info
            self._supported_symbols = self._rest._supported_symbols
            
            # 初始化WebSocket（可选）
            if self.config.enable_websocket:
                ws_success = await self._websocket.initialize()
                if not ws_success:
                    self.logger.warning("⚠️ GRVT WebSocket初始化失败，仅使用REST API")
                else:
                    await self._websocket.connect()
            
            market_count = len(self._market_info)
            self.logger.info(f"✅ GRVT连接成功{f'，加载 {market_count} 个市场' if market_count > 0 else ''}")
            return True

        except Exception as e:
            self.logger.error(f"❌ GRVT连接失败: {str(e)}")
            return False

    async def _do_disconnect(self) -> None:
        """断开连接实现"""
        try:
            # 关闭WebSocket连接
            if self._websocket:
                await self._websocket.disconnect()
            
            # 关闭REST连接
            if self._rest:
                await self._rest.close()
            
            self.logger.info("✅ GRVT连接已断开")
            
        except Exception as e:
            self.logger.error(f"❌ 断开GRVT连接失败: {str(e)}")

    async def _do_authenticate(self) -> bool:
        """认证实现"""
        try:
            # 测试API访问
            health_data = await self._rest.health_check()
            if not health_data.get('api_accessible', False):
                return False
            
            self.logger.info("✅ GRVT认证成功")
            return True

        except Exception as e:
            self.logger.error(f"❌ GRVT认证失败: {str(e)}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """健康检查实现"""
        try:
            return await self._rest.health_check()
        except Exception as e:
            return {
                'api_accessible': False,
                'error': str(e)
            }

    async def _do_heartbeat(self) -> None:
        """心跳实现"""
        try:
            # GRVT SDK会自动处理心跳，这里可以添加额外的检查
            pass
        except Exception as e:
            self.logger.error(f"❌ GRVT心跳失败: {str(e)}")

    # ==================== 市场数据接口实现 ====================

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return await self._rest.get_exchange_info()

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取行情数据"""
        return await self._rest.get_ticker(symbol)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个行情数据"""
        return await self._rest.get_tickers(symbols)

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        return await self._rest.get_orderbook(symbol, limit)

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        return await self._rest.get_ohlcv(symbol, timeframe, since, limit)

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取成交数据"""
        return await self._rest.get_trades(symbol, since, limit)

    # ==================== 账户接口实现 ====================

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        return await self._rest.get_balances()

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        return await self._rest.get_positions(symbols)

    # ==================== 交易接口实现 ====================

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None,
        batch_mode: bool = False
    ) -> OrderData:
        """创建订单"""
        order = await self._rest.create_order(symbol, side, order_type, amount, price, params)
        
        # 触发订单创建事件
        await self._handle_order_update(order)
        
        return order

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        order = await self._rest.cancel_order(order_id, symbol)
        
        # 触发订单更新事件
        await self._handle_order_update(order)
        
        return order

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        orders = await self._rest.cancel_all_orders(symbol)
        
        # 触发订单更新事件
        for order in orders:
            await self._handle_order_update(order)
        
        return orders

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        return await self._rest.get_order(order_id, symbol)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        return await self._rest.get_open_orders(symbol)

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        return await self._rest.get_order_history(symbol, since, limit)

    # ==================== 设置接口实现 ====================

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        return await self._rest.set_leverage(symbol, leverage)

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        return await self._rest.set_margin_mode(symbol, margin_mode)

    # ==================== 订阅接口实现 ====================

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            if self._websocket.is_connected:
                await self._websocket.subscribe_ticker(symbol, callback)
            else:
                self.logger.warning(f"⚠️ WebSocket未连接，使用轮询模式订阅ticker {symbol}")
                asyncio.create_task(self._poll_ticker(symbol, callback))
        except Exception as e:
            self.logger.error(f"❌ 订阅行情失败 {symbol}: {e}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        try:
            if self._websocket.is_connected:
                await self._websocket.subscribe_orderbook(symbol, callback)
            else:
                self.logger.warning(f"⚠️ WebSocket未连接，使用轮询模式订阅订单簿 {symbol}")
                asyncio.create_task(self._poll_orderbook(symbol, callback))
        except Exception as e:
            self.logger.error(f"❌ 订阅订单簿失败 {symbol}: {e}")

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            if self._websocket.is_connected:
                await self._websocket.subscribe_trades(symbol, callback)
            else:
                self.logger.warning(f"⚠️ WebSocket未连接，使用轮询模式订阅成交 {symbol}")
                asyncio.create_task(self._poll_trades(symbol, callback))
        except Exception as e:
            self.logger.error(f"❌ 订阅成交失败 {symbol}: {e}")

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流"""
        try:
            if self._websocket.is_user_connected:
                await self._websocket.subscribe_user_data(callback)
            else:
                self.logger.warning("⚠️ 用户数据流未连接，使用轮询模式")
                asyncio.create_task(self._poll_user_data(callback))
        except Exception as e:
            self.logger.error(f"❌ 订阅用户数据失败: {e}")

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        try:
            await self._websocket.unsubscribe(symbol)
            
            # 停止轮询
            if not hasattr(self, '_stop_polling'):
                self._stop_polling = set()
            
            if symbol:
                self._stop_polling.add(symbol)
            else:
                self._stop_polling.add('ALL')
                
        except Exception as e:
            self.logger.error(f"❌ 取消订阅失败: {e}")

    # ==================== 轮询模式实现 ====================

    async def _poll_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """轮询行情数据"""
        try:
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                ticker = await self.get_ticker(symbol)
                await self._safe_callback(callback, ticker)
                await asyncio.sleep(1)  # 1秒轮询间隔
        except Exception as e:
            self.logger.error(f"❌ 轮询行情失败 {symbol}: {e}")

    async def _poll_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """轮询订单簿数据"""
        try:
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                orderbook = await self.get_orderbook(symbol)
                await self._safe_callback(callback, orderbook)
                await asyncio.sleep(0.5)  # 0.5秒轮询间隔
        except Exception as e:
            self.logger.error(f"❌ 轮询订单簿失败 {symbol}: {e}")

    async def _poll_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """轮询成交数据"""
        try:
            last_trade_id = None
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                trades = await self.get_trades(symbol, limit=10)
                # 只推送新的成交
                for trade in trades:
                    if last_trade_id is None or trade.id != last_trade_id:
                        await self._safe_callback(callback, trade)
                        last_trade_id = trade.id
                await asyncio.sleep(1)  # 1秒轮询间隔
        except Exception as e:
            self.logger.error(f"❌ 轮询成交失败 {symbol}: {e}")

    async def _poll_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """轮询用户数据"""
        try:
            last_balances = {}
            last_orders = {}

            while 'ALL' not in getattr(self, '_stop_polling', set()):
                # 检查余额变化
                try:
                    current_balances = await self.get_balances()
                    if str(current_balances) != str(last_balances):
                        await self._safe_callback(callback, {'type': 'balance', 'data': current_balances})
                        last_balances = current_balances
                except Exception:
                    pass

                # 检查订单变化
                try:
                    current_orders = await self.get_open_orders()
                    if str(current_orders) != str(last_orders):
                        await self._safe_callback(callback, {'type': 'orders', 'data': current_orders})
                        last_orders = current_orders
                except Exception:
                    pass

                await asyncio.sleep(2)  # 2秒轮询间隔
        except Exception as e:
            self.logger.error(f"❌ 轮询用户数据失败: {e}")

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error(f"❌ 回调函数执行失败: {e}")

    # ==================== 工具方法 ====================

    def _map_symbol(self, symbol: str) -> str:
        """映射交易对符号"""
        return self._base.map_symbol_to_grvt(symbol)

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易对符号"""
        return self._base.map_symbol_from_grvt(exchange_symbol)

    def get_cached_ticker(self, symbol: str) -> Optional[TickerData]:
        """获取缓存的行情数据"""
        return self._websocket.get_cached_ticker(symbol)

    def get_cached_orderbook(self, symbol: str) -> Optional[OrderBookData]:
        """获取缓存的订单簿数据"""
        return self._websocket.get_cached_orderbook(symbol)

    @property
    def is_websocket_connected(self) -> bool:
        """检查WebSocket连接状态"""
        return self._websocket.is_connected

    @property
    def is_user_stream_connected(self) -> bool:
        """检查用户数据流连接状态"""
        return self._websocket.is_user_connected

    @property
    def supported_symbols(self) -> List[str]:
        """获取支持的交易对列表"""
        if self._market_info:
            return [self._reverse_map_symbol(s) for s in self._market_info.keys()]
        return []

    def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取交易对信息"""
        grvt_symbol = self._map_symbol(symbol)
        return self._market_info.get(grvt_symbol)

    # ==================== 订阅管理器方法 ====================
    
    def get_subscription_manager(self):
        """获取订阅管理器实例"""
        return getattr(self, '_subscription_manager', None)
    
    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        if hasattr(self, '_subscription_manager') and self._subscription_manager:
            return self._subscription_manager.get_subscription_stats()
        return {}
    
    # ==================== 工具方法 ====================

    def __str__(self) -> str:
        """字符串表示"""
        return f"GrvtAdapter(connected={self._connected}, symbols={len(self.supported_symbols)})"

    def __repr__(self) -> str:
        """详细字符串表示"""
        return (f"GrvtAdapter("
                f"config={self.config.exchange_id if self.config else None}, "
                f"rest_connected={self._rest.client is not None}, "
                f"ws_connected={self.is_websocket_connected}, "
                f"user_stream_connected={self.is_user_stream_connected}, "
                f"markets={len(self._market_info)})")
