"""
GRVT交易所WebSocket模块

使用GRVT SDK (GrvtCcxtWS) 实现WebSocket连接和实时数据订阅
"""

import asyncio
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from decimal import Decimal

from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv, GrvtWSEndpointType
from pysdk.grvt_ccxt_types import GrvtOrderType, GrvtOrderSide

from .grvt_base import GrvtBase
from ...models import (
    TickerData, OrderBookData, TradeData, OrderData, PositionData,
    OrderSide, OrderType, OrderStatus
)


class GrvtWebSocket(GrvtBase):
    """GRVT WebSocket客户端"""
    
    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger
        self.client: Optional[GrvtCcxtWS] = None
        
        # 从配置获取参数
        self.testnet = getattr(config, 'testnet', False)
        self.env = GrvtEnv.TESTNET if self.testnet else GrvtEnv.PROD
        
        # GRVT SDK参数
        self.parameters = {
            'trading_account_id': getattr(config, 'extra_params', {}).get('trading_account_id'),
            'private_key': getattr(config, 'api_secret', ''),
            'api_key': getattr(config, 'api_key', ''),
        }
        
        # 事件循环
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
        # 订阅管理
        self._ticker_callbacks: Dict[str, List[Callable]] = {}
        self._orderbook_callbacks: Dict[str, List[Callable]] = {}
        self._trade_callbacks: Dict[str, List[Callable]] = {}
        self._user_data_callbacks: List[Callable] = []
        
        # 数据缓存
        self._ticker_cache: Dict[str, TickerData] = {}
        self._orderbook_cache: Dict[str, OrderBookData] = {}
        
        # 连接状态
        self._connected = False
        self._initialized = False
    
    async def initialize(self) -> bool:
        """初始化GRVT WebSocket客户端"""
        try:
            self._loop = asyncio.get_event_loop()
            
            # 创建WebSocket客户端
            self.client = GrvtCcxtWS(
                env=self.env,
                loop=self._loop,
                logger=self.logger,
                parameters=self.parameters
            )
            
            # 初始化（加载市场、刷新cookie、连接WebSocket）
            await self.client.initialize()
            
            self._initialized = True
            
            if self.logger:
                self.logger.info("✅ GRVT WebSocket初始化成功")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ GRVT WebSocket初始化失败: {str(e)}")
            return False
    
    async def connect(self) -> bool:
        """建立WebSocket连接"""
        try:
            if not self._initialized:
                await self.initialize()

            self.logger.info(f"✅ GRVT WebSocket 尝试连接: {self.client.endpoint_types}")
            
            # 确保所有端点都已连接
            # TODO: 交易channel是另外的，需要cookie和account信息连接
            await self.client.connect_channel(GrvtWSEndpointType.MARKET_DATA)
            
            self._connected = True
            
            if self.logger:
                self.logger.info("✅ GRVT WebSocket连接成功")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ GRVT WebSocket连接失败: {str(e)}")
            self._connected = False
            return False
    
    async def disconnect(self):
        """断开WebSocket连接"""
        try:
            if self.client:
                # 关闭所有连接
                for endpoint_type in self.client.endpoint_types:
                    await self.client._close_connection(endpoint_type)
            
            self._connected = False
            
            if self.logger:
                self.logger.info("✅ GRVT WebSocket已断开")
        except Exception as e:
            if self.logger:
                self.logger.error(f"断开WebSocket连接失败: {e}")
    
    @property
    def is_connected(self) -> bool:
        """检查连接状态"""
        if not self.client:
            return False
        
        # 检查市场数据端点是否连接
        market_connected = self.client.is_endpoint_connected(GrvtWSEndpointType.MARKET_DATA)
        return market_connected and self._connected
    
    @property
    def is_user_connected(self) -> bool:
        """检查用户数据流连接状态"""
        if not self.client:
            return False
        
        # 检查交易数据端点是否连接
        trade_connected = self.client.is_endpoint_connected(GrvtWSEndpointType.TRADE_DATA)
        return trade_connected and self._connected
    
    # ==================== 订阅接口 ====================
    
    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            # 注册回调
            if symbol not in self._ticker_callbacks:
                self._ticker_callbacks[symbol] = []
            self._ticker_callbacks[symbol].append(callback)
            
            # 创建包装回调
            async def _ticker_callback(message: Dict[str, Any]):
                try:
                    feed_data = message.get('feed', {})
                    ticker_data = self._parse_ticker(feed_data, symbol)
                    
                    # 更新缓存
                    self._ticker_cache[symbol] = ticker_data
                    
                    # 调用所有注册的回调
                    for cb in self._ticker_callbacks.get(symbol, []):
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(ticker_data)
                            else:
                                cb(ticker_data)
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"Ticker回调执行失败: {e}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"解析ticker数据失败: {e}")
            
            # 订阅ticker流
            await self.client.subscribe(
                stream='ticker.s',  # 或 'ticker.d' 用于增量更新
                callback=_ticker_callback,
                params={'instrument': grvt_symbol, 'rate': 500}  # 500ms更新频率
            )
            
            if self.logger:
                self.logger.info(f"✅ 已订阅ticker: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅ticker失败 {symbol}: {e}")
            raise
    
    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            # 注册回调
            if symbol not in self._orderbook_callbacks:
                self._orderbook_callbacks[symbol] = []
            self._orderbook_callbacks[symbol].append(callback)
            
            # 创建包装回调
            async def _orderbook_callback(message: Dict[str, Any]):
                try:
                    feed_data = message.get('feed', {})
                    orderbook_data = self._parse_orderbook(feed_data, symbol)
                    
                    # 更新缓存
                    self._orderbook_cache[symbol] = orderbook_data
                    
                    # 调用所有注册的回调
                    for cb in self._orderbook_callbacks.get(symbol, []):
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(orderbook_data)
                            else:
                                cb(orderbook_data)
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"Orderbook回调执行失败: {e}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"解析orderbook数据失败: {e}")
            
            # 订阅订单簿流
            await self.client.subscribe(
                stream='book.s',  # 快照流
                callback=_orderbook_callback,
                params={'instrument': grvt_symbol, 'rate': 500, 'depth': 20}
            )
            
            if self.logger:
                self.logger.info(f"✅ 已订阅orderbook: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅orderbook失败 {symbol}: {e}")
            raise
    
    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            # 注册回调
            if symbol not in self._trade_callbacks:
                self._trade_callbacks[symbol] = []
            self._trade_callbacks[symbol].append(callback)
            
            # 创建包装回调
            async def _trade_callback(message: Dict[str, Any]):
                try:
                    feed_data = message.get('feed', {})
                    
                    # GRVT trade流可能返回单个trade或trade数组
                    trades = feed_data if isinstance(feed_data, list) else [feed_data]
                    
                    for trade_data in trades:
                        trade = self._parse_trade(trade_data, symbol)
                        
                        # 调用所有注册的回调
                        for cb in self._trade_callbacks.get(symbol, []):
                            try:
                                if asyncio.iscoroutinefunction(cb):
                                    await cb(trade)
                                else:
                                    cb(trade)
                            except Exception as e:
                                if self.logger:
                                    self.logger.error(f"Trade回调执行失败: {e}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"解析trade数据失败: {e}")
            
            # 订阅trade流
            await self.client.subscribe(
                stream='trade',
                callback=_trade_callback,
                params={'instrument': grvt_symbol, 'limit': 50}
            )
            
            if self.logger:
                self.logger.info(f"✅ 已订阅trades: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅trades失败 {symbol}: {e}")
            raise
    
    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流（订单、持仓、余额更新）"""
        try:
            # 注册回调
            self._user_data_callbacks.append(callback)
            
            # 订阅订单更新
            async def _order_callback(message: Dict[str, Any]):
                try:
                    feed_data = message.get('feed', {})
                    
                    # 解析订单数据
                    order_data = self._parse_order(feed_data)
                    
                    # 调用回调
                    user_data = {
                        'type': 'order',
                        'data': order_data
                    }
                    
                    for cb in self._user_data_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(user_data)
                            else:
                                cb(user_data)
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"User data回调执行失败: {e}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"解析order数据失败: {e}")
            
            # 订阅持仓更新
            async def _position_callback(message: Dict[str, Any]):
                try:
                    feed_data = message.get('feed', {})
                    
                    # 解析持仓数据
                    position_data = self._parse_position(feed_data)
                    
                    # 调用回调
                    user_data = {
                        'type': 'position',
                        'data': position_data
                    }
                    
                    for cb in self._user_data_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(user_data)
                            else:
                                cb(user_data)
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"User data回调执行失败: {e}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"解析position数据失败: {e}")
            
            # 订阅状态更新（包含余额信息）
            async def _state_callback(message: Dict[str, Any]):
                try:
                    feed_data = message.get('feed', {})
                    
                    # 调用回调
                    user_data = {
                        'type': 'state',
                        'data': feed_data
                    }
                    
                    for cb in self._user_data_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(user_data)
                            else:
                                cb(user_data)
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"User data回调执行失败: {e}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"解析state数据失败: {e}")
            
            # 订阅订单流
            await self.client.subscribe(
                stream='order',
                callback=_order_callback,
                params={}  # 订阅所有订单
            )
            
            # 订阅持仓流
            await self.client.subscribe(
                stream='position',
                callback=_position_callback,
                params={}  # 订阅所有持仓
            )
            
            # 订阅状态流
            await self.client.subscribe(
                stream='state',
                callback=_state_callback,
                params={}  # 订阅账户状态
            )
            
            if self.logger:
                self.logger.info("✅ 已订阅用户数据流")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅用户数据失败: {e}")
            raise
    
    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        try:
            if symbol:
                # 取消特定symbol的订阅
                grvt_symbol = self.map_symbol_to_grvt(symbol)
                
                # 取消ticker订阅
                if symbol in self._ticker_callbacks:
                    # GRVT SDK可能需要通过取消订阅来实现
                    self._ticker_callbacks.pop(symbol, None)
                
                # 取消orderbook订阅
                if symbol in self._orderbook_callbacks:
                    self._orderbook_callbacks.pop(symbol, None)
                
                # 取消trade订阅
                if symbol in self._trade_callbacks:
                    self._trade_callbacks.pop(symbol, None)
                
                if self.logger:
                    self.logger.info(f"已取消订阅: {symbol}")
            else:
                # 取消所有订阅
                self._ticker_callbacks.clear()
                self._orderbook_callbacks.clear()
                self._trade_callbacks.clear()
                self._user_data_callbacks.clear()
                
                if self.logger:
                    self.logger.info("已取消所有订阅")
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消订阅失败: {e}")
    
    # ==================== 工具方法 ====================
    
    def get_cached_ticker(self, symbol: str) -> Optional[TickerData]:
        """获取缓存的ticker数据"""
        return self._ticker_cache.get(symbol)
    
    def get_cached_orderbook(self, symbol: str) -> Optional[OrderBookData]:
        """获取缓存的orderbook数据"""
        return self._orderbook_cache.get(symbol)

