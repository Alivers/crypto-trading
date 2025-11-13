"""
GRVT交易所REST API模块

包装GRVT SDK (GrvtCcxt) 为异步REST API接口
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal

from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_env import GrvtEnv
from pysdk.grvt_ccxt_types import GrvtOrderType, GrvtOrderSide

from .grvt_base import GrvtBase
from ...models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData,
    PositionData, OHLCVData, ExchangeInfo, ExchangeType,
    OrderSide, OrderType, OrderStatus
)


class GrvtRest(GrvtBase):
    """GRVT REST API接口实现"""
    
    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger
        self.client: Optional[GrvtCcxt] = None
        
        # 从配置获取参数
        self.testnet = getattr(config, 'testnet', False)
        self.env = GrvtEnv.TESTNET if self.testnet else GrvtEnv.PROD
        
        # GRVT SDK参数
        self.parameters = {
            'trading_account_id': getattr(config, 'extra_params', {}).get('trading_account_id'),
            'private_key': getattr(config, 'api_secret', ''),
            'api_key': getattr(config, 'api_key', ''),
        }
        
        # 重试配置
        self.max_retries = 3
        self.retry_delay = 1.0
    
    async def initialize(self) -> bool:
        """初始化GRVT客户端"""
        try:
            # 在后台线程中创建同步客户端
            def _create_client():
                return GrvtCcxt(
                    env=self.env,
                    logger=self.logger,
                    parameters=self.parameters
                )
            
            self.client = await asyncio.to_thread(_create_client)
            
            # 加载市场信息
            await self._load_markets()
            
            if self.logger:
                self.logger.info(f"✅ GRVT REST初始化成功，加载 {len(self._market_info)} 个市场")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ GRVT REST初始化失败: {str(e)}")
            return False
    
    async def _load_markets(self):
        """加载市场信息"""
        try:
            markets = await self._execute_sync(self.client.load_markets)
            self._market_info = markets
            
            # 提取支持的交易对
            self._supported_symbols = list(markets.keys())
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"加载市场信息失败: {e}")
            self._market_info = {}
            self._supported_symbols = []
    
    async def close(self):
        """关闭连接"""
        if self.client:
            # GRVT SDK没有显式的close方法，只需清理引用
            self.client = None
    
    async def _execute_sync(self, func, *args, **kwargs):
        """在后台线程中执行同步函数"""
        return await asyncio.to_thread(func, *args, **kwargs)
    
    async def _execute_with_retry(self, func, *args, **kwargs):
        """带重试的API调用"""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                result = await self._execute_sync(func, *args, **kwargs)
                return result
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    if self.logger:
                        self.logger.warning(f"API调用失败 (尝试 {attempt + 1}/{self.max_retries}): {str(e)}")
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    if self.logger:
                        self.logger.error(f"API调用最终失败: {str(e)}")
        
        raise last_error
    
    # ==================== 市场数据接口 ====================
    
    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        try:
            return ExchangeInfo(
                name="GRVT",
                id="grvt",
                type=ExchangeType.PERPETUAL,
                supported_features=[
                    "perpetual_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                rate_limits={},
                precision={},
                fees={},
                markets=self._market_info,
                status="operational",
                timestamp=datetime.now()
            )
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取交易所信息失败: {str(e)}")
            raise
    
    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个行情数据"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            ticker_data = await self._execute_with_retry(
                self.client.fetch_ticker, grvt_symbol
            )
            return self._parse_ticker(ticker_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取行情失败 {symbol}: {str(e)}")
            raise
    
    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个行情数据"""
        try:
            if symbols:
                # 并发获取指定符号的行情
                tasks = [self.get_ticker(symbol) for symbol in symbols]
                return await asyncio.gather(*tasks, return_exceptions=True)
            else:
                # 获取所有行情（需要遍历所有市场）
                tickers = []
                for grvt_symbol in self._supported_symbols:
                    try:
                        symbol = self.map_symbol_from_grvt(grvt_symbol)
                        ticker = await self.get_ticker(symbol)
                        tickers.append(ticker)
                    except Exception as e:
                        if self.logger:
                            self.logger.warning(f"获取行情失败 {grvt_symbol}: {e}")
                        continue
                return tickers
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取多个行情失败: {str(e)}")
            return []
    
    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            params = {}
            if limit:
                params['limit'] = limit
            
            orderbook_data = await self._execute_with_retry(
                self.client.fetch_order_book, grvt_symbol, limit or 10, params
            )
            return self._parse_orderbook(orderbook_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单簿失败 {symbol}: {str(e)}")
            raise
    
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            # 转换时间框架（GRVT可能使用不同的格式）
            # 标准格式: 1m, 5m, 15m, 1h, 4h, 1d
            timeframe_map = {
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '30m': '30m',
                '1h': '1h',
                '4h': '4h',
                '1d': '1d',
            }
            grvt_timeframe = timeframe_map.get(timeframe, '1h')
            
            since_timestamp = None
            if since:
                since_timestamp = int(since.timestamp() * 1_000_000_000)  # 转换为纳秒
            
            params = {}
            if since_timestamp:
                params['since'] = since_timestamp
            if limit:
                params['limit'] = limit
            
            ohlcv_data = await self._execute_with_retry(
                self.client.fetch_ohlcv, grvt_symbol, grvt_timeframe, since_timestamp, limit, params
            )
            
            # 解析K线数据
            result = []
            for candle in ohlcv_data:
                if len(candle) >= 6:
                    result.append(OHLCVData(
                        symbol=symbol,
                        timestamp=self._parse_timestamp(candle[0], 'ms'),
                        open=self._safe_decimal(candle[1]),
                        high=self._safe_decimal(candle[2]),
                        low=self._safe_decimal(candle[3]),
                        close=self._safe_decimal(candle[4]),
                        volume=self._safe_decimal(candle[5]),
                        raw_data=candle
                    ))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取K线数据失败 {symbol}: {str(e)}")
            return []
    
    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取成交数据"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            since_timestamp = None
            if since:
                since_timestamp = int(since.timestamp() * 1_000_000_000)  # 转换为纳秒
            
            params = {}
            if since_timestamp:
                params['since'] = since_timestamp
            if limit:
                params['limit'] = limit
            
            trades_data = await self._execute_with_retry(
                self.client.fetch_recent_trades, grvt_symbol, limit, params
            )
            
            result = []
            for trade in trades_data:
                result.append(self._parse_trade(trade, symbol))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取成交数据失败 {symbol}: {str(e)}")
            return []
    
    # ==================== 账户接口 ====================
    
    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        try:
            balance_data = await self._execute_with_retry(
                self.client.fetch_balance, 'sub-account'
            )
            
            # GRVT返回的余额格式需要根据实际API调整
            result = []
            if isinstance(balance_data, dict):
                # CCXT格式: {'BTC': {'free': 1.0, 'used': 0.0, 'total': 1.0}, ...}
                for currency, balance_info in balance_data.items():
                    if currency not in ['info', 'free', 'used', 'total']:
                        if isinstance(balance_info, dict):
                            result.append(self._parse_balance(balance_info, currency))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取余额失败: {str(e)}")
            return []
    
    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        try:
            grvt_symbols = None
            if symbols:
                grvt_symbols = [self.map_symbol_to_grvt(s) for s in symbols]
            
            positions_data = await self._execute_with_retry(
                self.client.fetch_positions, grvt_symbols or [], {}
            )
            
            result = []
            for position in positions_data:
                parsed = self._parse_position(position)
                # 只返回有持仓的
                if parsed.size > 0:
                    result.append(parsed)
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取持仓失败: {str(e)}")
            return []
    
    # ==================== 交易接口 ====================
    
    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            # 转换订单类型
            grvt_order_type = self._convert_order_type(order_type)
            grvt_side = self._convert_order_side(side)
            
            # 准备参数
            order_params = params or {}
            
            # 调用SDK
            order_data = await self._execute_with_retry(
                self.client.create_order,
                grvt_symbol,
                grvt_order_type,
                grvt_side,
                float(amount),
                float(price) if price else None,
                order_params
            )
            
            # 解析订单响应
            return self._parse_order(order_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"创建订单失败 {symbol}: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            success = await self._execute_with_retry(
                self.client.cancel_order,
                id=order_id,
                symbol=grvt_symbol,
                params={}
            )
            
            if success:
                # 获取订单状态
                order_data = await self.get_order(order_id, symbol)
                return order_data
            else:
                raise Exception("取消订单失败")
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消订单失败 {order_id}: {str(e)}")
            raise
    
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        try:
            params = {}
            if symbol:
                grvt_symbol = self.map_symbol_to_grvt(symbol)
                # GRVT可能需要通过base/quote来过滤
                # 这里需要根据实际API调整
                params['base'] = grvt_symbol.split('_')[0] if '_' in grvt_symbol else None
            
            success = await self._execute_with_retry(
                self.client.cancel_all_orders,
                params=params
            )
            
            if success:
                # 获取所有被取消的订单
                open_orders = await self.get_open_orders(symbol)
                return open_orders
            else:
                return []
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消所有订单失败: {str(e)}")
            return []
    
    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        try:
            grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            order_data = await self._execute_with_retry(
                self.client.fetch_order,
                id=order_id,
                params={'symbol': grvt_symbol}
            )
            
            return self._parse_order(order_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单失败 {order_id}: {str(e)}")
            raise
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            grvt_symbol = None
            if symbol:
                grvt_symbol = self.map_symbol_to_grvt(symbol)
            
            orders_data = await self._execute_with_retry(
                self.client.fetch_open_orders,
                symbol=grvt_symbol,
                since=None,
                limit=None,
                params={}
            )
            
            result = []
            for order in orders_data:
                parsed = self._parse_order(order, symbol)
                result.append(parsed)
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取开放订单失败: {str(e)}")
            return []
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        try:
            params = {}
            if symbol:
                grvt_symbol = self.map_symbol_to_grvt(symbol)
                params['base'] = grvt_symbol.split('_')[0] if '_' in grvt_symbol else None
            
            if since:
                params['start_time'] = int(since.timestamp() * 1_000_000_000)  # 纳秒
            
            if limit:
                params['limit'] = limit
            
            history_data = await self._execute_with_retry(
                self.client.fetch_order_history,
                params=params
            )
            
            result = []
            orders = history_data.get('result', [])
            for order in orders:
                parsed = self._parse_order(order, symbol)
                result.append(parsed)
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取历史订单失败: {str(e)}")
            return []
    
    # ==================== 设置接口 ====================
    
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        # GRVT可能不支持通过API设置杠杆，需要根据实际API调整
        if self.logger:
            self.logger.warning("GRVT可能不支持通过API设置杠杆")
        return {
            'symbol': symbol,
            'leverage': leverage,
            'success': False,
            'message': 'Not supported'
        }
    
    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        # GRVT可能不支持通过API设置保证金模式，需要根据实际API调整
        if self.logger:
            self.logger.warning("GRVT可能不支持通过API设置保证金模式")
        return {
            'symbol': symbol,
            'margin_mode': margin_mode,
            'success': False,
            'message': 'Not supported'
        }
    
    # ==================== 工具方法 ====================
    
    def _convert_order_type(self, order_type: OrderType) -> str:
        """转换订单类型到GRVT格式"""
        if order_type == OrderType.MARKET:
            return GrvtOrderType.MARKET
        elif order_type == OrderType.LIMIT:
            return GrvtOrderType.LIMIT
        else:
            return GrvtOrderType.LIMIT
    
    def _convert_order_side(self, side: OrderSide) -> str:
        """转换订单方向到GRVT格式"""
        if side == OrderSide.BUY:
            return GrvtOrderSide.BUY
        elif side == OrderSide.SELL:
            return GrvtOrderSide.SELL
        else:
            return GrvtOrderSide.BUY
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 尝试获取市场信息
            await self._execute_with_retry(self.client.fetch_markets, {})
            
            return {
                'api_accessible': True,
                'markets_loaded': len(self._market_info) > 0,
                'market_count': len(self._market_info),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'api_accessible': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

