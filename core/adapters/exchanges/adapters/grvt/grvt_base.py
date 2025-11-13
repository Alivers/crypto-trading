"""
GRVT交易所基础模块

提供GRVT交易所的基础配置、符号映射、数据解析等公共功能
"""

from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime

from ...models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData,
    OrderSide, OrderType, OrderStatus, PositionData, PositionSide,
    MarginMode, OrderBookLevel
)


class GrvtBase:
    """GRVT基础工具类"""
    
    # GRVT符号格式：BTC_USDT_Perp, ETH_USDT_Perp
    # 标准格式：BTC/USDT:PERP, ETH/USDT:PERP
    
    def __init__(self, config=None):
        self.config = config
        self.logger = None
        
        # 符号映射（标准格式 -> GRVT格式）
        self._symbol_mapping = {}
        if config and hasattr(config, 'symbol_mapping'):
            self._symbol_mapping = config.symbol_mapping.copy()
        
        # 支持的交易对（从API动态获取）
        self._supported_symbols = []
        self._market_info = {}
    
    def set_logger(self, logger):
        """设置日志器"""
        self.logger = logger
    
    def _safe_decimal(self, value: Any, default: Decimal = Decimal('0')) -> Decimal:
        """安全转换为Decimal"""
        try:
            if value is None or value == '':
                return default
            return Decimal(str(value))
        except (ValueError, TypeError):
            if self.logger:
                self.logger.warning(f"无法转换为Decimal: {value}")
            return default
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        try:
            if value is None:
                return None
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为float"""
        try:
            if value is None:
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_timestamp(self, timestamp: Any, unit: str = 'ns') -> Optional[datetime]:
        """解析时间戳（GRVT使用纳秒）"""
        if timestamp is None:
            return None
        
        try:
            timestamp_int = int(timestamp)
            
            if unit == 'ns':
                return datetime.fromtimestamp(timestamp_int / 1_000_000_000)
            elif unit == 'ms':
                return datetime.fromtimestamp(timestamp_int / 1000)
            elif unit == 'us':
                return datetime.fromtimestamp(timestamp_int / 1_000_000)
            else:
                return datetime.fromtimestamp(timestamp_int)
        except (ValueError, TypeError, OSError):
            return None
    
    # ==================== 符号映射 ====================
    
    def map_symbol_to_grvt(self, symbol: str) -> str:
        """
        映射标准格式到GRVT格式
        例如: BTC/USDT:PERP -> BTC_USDT_Perp
        """
        # 先检查映射表
        if symbol in self._symbol_mapping:
            return self._symbol_mapping[symbol]
        
        # 标准格式转换
        # BTC/USDT:PERP -> BTC_USDT_Perp
        if ':' in symbol:
            base_quote, contract_type = symbol.split(':')
            if '/' in base_quote:
                base, quote = base_quote.split('/')
                return f"{base}_{quote}_Perp"
        
        # BTC/USDT -> BTC_USDT_Perp (假设是永续合约)
        if '/' in symbol:
            base, quote = symbol.split('/')
            return f"{base}_{quote}_Perp"
        
        # 如果已经是GRVT格式，直接返回
        return symbol
    
    def map_symbol_from_grvt(self, grvt_symbol: str) -> str:
        """
        反向映射GRVT格式到标准格式
        例如: BTC_USDT_Perp -> BTC/USDT:PERP
        """
        # 先检查反向映射表
        reverse_mapping = {v: k for k, v in self._symbol_mapping.items()}
        if grvt_symbol in reverse_mapping:
            return reverse_mapping[grvt_symbol]
        
        # GRVT格式转换
        # BTC_USDT_Perp -> BTC/USDT:PERP
        if '_Perp' in grvt_symbol:
            symbol_part = grvt_symbol.replace('_Perp', '')
            if '_' in symbol_part:
                parts = symbol_part.split('_')
                if len(parts) >= 2:
                    base = parts[0]
                    quote = '_'.join(parts[1:])  # 处理USDT_PERP这种情况
                    return f"{base}/{quote}:PERP"
        
        # 如果已经是标准格式，直接返回
        return grvt_symbol
    
    # ==================== 订单类型/状态映射 ====================
    
    def _parse_order_side(self, side: str) -> OrderSide:
        """解析订单方向"""
        if not side:
            return OrderSide.BUY
        
        side_lower = side.lower()
        if side_lower in ['buy', 'bid', 'long']:
            return OrderSide.BUY
        elif side_lower in ['sell', 'ask', 'short']:
            return OrderSide.SELL
        else:
            return OrderSide.BUY
    
    def _parse_order_type(self, order_type: str) -> OrderType:
        """解析订单类型"""
        if not order_type:
            return OrderType.LIMIT
        
        order_type_lower = order_type.lower()
        if order_type_lower == 'market':
            return OrderType.MARKET
        elif order_type_lower == 'limit':
            return OrderType.LIMIT
        elif order_type_lower == 'stop':
            return OrderType.STOP
        elif order_type_lower == 'stop_limit':
            return OrderType.STOP_LIMIT
        elif order_type_lower == 'take_profit':
            return OrderType.TAKE_PROFIT
        elif order_type_lower == 'take_profit_limit':
            return OrderType.TAKE_PROFIT_LIMIT
        else:
            return OrderType.LIMIT
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """解析订单状态"""
        if not status:
            return OrderStatus.PENDING
        
        status_lower = status.lower()
        if status_lower in ['new', 'open', 'pending', 'active']:
            return OrderStatus.OPEN
        elif status_lower in ['filled', 'closed', 'complete']:
            return OrderStatus.FILLED
        elif status_lower in ['canceled', 'cancelled']:
            return OrderStatus.CANCELED
        elif status_lower in ['partially_filled', 'partial']:
            return OrderStatus.OPEN  # 部分成交仍然是OPEN
        elif status_lower in ['rejected', 'failed']:
            return OrderStatus.REJECTED
        elif status_lower in ['expired']:
            return OrderStatus.EXPIRED
        else:
            return OrderStatus.UNKNOWN
    
    def _parse_position_side(self, side: str) -> PositionSide:
        """解析持仓方向"""
        if not side:
            return PositionSide.LONG
        
        side_lower = side.lower()
        if side_lower in ['long', 'buy']:
            return PositionSide.LONG
        elif side_lower in ['short', 'sell']:
            return PositionSide.SHORT
        else:
            return PositionSide.LONG
    
    # ==================== 数据解析 ====================
    
    def _parse_ticker(self, data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据"""
        # GRVT ticker格式参考
        # 需要根据实际API响应调整
        
        timestamp = None
        if 'timestamp' in data:
            timestamp = self._parse_timestamp(data['timestamp'], 'ns')
        elif 'ts' in data:
            timestamp = self._parse_timestamp(data['ts'], 'ns')
        
        return TickerData(
            symbol=symbol,
            last=self._safe_decimal(data.get('last') or data.get('lastPrice')),
            bid=self._safe_decimal(data.get('bid') or data.get('bidPrice')),
            ask=self._safe_decimal(data.get('ask') or data.get('askPrice')),
            bid_volume=self._safe_decimal(data.get('bidSize') or data.get('bidQty')),
            ask_volume=self._safe_decimal(data.get('askSize') or data.get('askQty')),
            high=self._safe_decimal(data.get('high') or data.get('highPrice')),
            low=self._safe_decimal(data.get('low') or data.get('lowPrice')),
            volume=self._safe_decimal(data.get('volume') or data.get('baseVolume')),
            quote_volume=self._safe_decimal(data.get('quoteVolume')),
            open=self._safe_decimal(data.get('open') or data.get('openPrice')),
            close=self._safe_decimal(data.get('close') or data.get('lastPrice')),
            change=self._safe_decimal(data.get('change') or data.get('priceChange')),
            percentage=self._safe_decimal(data.get('percentage') or data.get('priceChangePercent')),
            timestamp=timestamp or datetime.now(),
            raw_data=data
        )
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析订单簿数据"""
        bids = []
        asks = []
        
        # GRVT订单簿格式可能是 bids/asks 数组或对象
        bids_data = data.get('bids', [])
        asks_data = data.get('asks', [])
        
        # 处理数组格式 [[price, size], ...]
        if bids_data and isinstance(bids_data[0], list):
            for bid in bids_data:
                if len(bid) >= 2:
                    bids.append(OrderBookLevel(
                        price=self._safe_decimal(bid[0]),
                        size=self._safe_decimal(bid[1])
                    ))
        # 处理对象格式
        elif isinstance(bids_data, dict):
            for price, size in bids_data.items():
                bids.append(OrderBookLevel(
                    price=self._safe_decimal(price),
                    size=self._safe_decimal(size)
                ))
        
        if asks_data and isinstance(asks_data[0], list):
            for ask in asks_data:
                if len(ask) >= 2:
                    asks.append(OrderBookLevel(
                        price=self._safe_decimal(ask[0]),
                        size=self._safe_decimal(ask[1])
                    ))
        elif isinstance(asks_data, dict):
            for price, size in asks_data.items():
                asks.append(OrderBookLevel(
                    price=self._safe_decimal(price),
                    size=self._safe_decimal(size)
                ))
        
        # 排序：bids降序，asks升序
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)
        
        timestamp = None
        if 'timestamp' in data:
            timestamp = self._parse_timestamp(data['timestamp'], 'ns')
        elif 'ts' in data:
            timestamp = self._parse_timestamp(data['ts'], 'ns')
        
        return OrderBookData(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=timestamp or datetime.now(),
            nonce=data.get('nonce') or data.get('sequence'),
            raw_data=data
        )
    
    def _parse_trade(self, data: Dict[str, Any], symbol: str) -> TradeData:
        """解析成交数据"""
        # 判断买卖方向
        side = OrderSide.BUY
        if 'side' in data:
            side = self._parse_order_side(data['side'])
        elif 'isBuyerMaker' in data:
            side = OrderSide.SELL if data['isBuyerMaker'] else OrderSide.BUY
        
        timestamp = None
        if 'timestamp' in data:
            timestamp = self._parse_timestamp(data['timestamp'], 'ns')
        elif 'ts' in data:
            timestamp = self._parse_timestamp(data['ts'], 'ns')
        elif 'time' in data:
            timestamp = self._parse_timestamp(data['time'], 'ns')
        
        return TradeData(
            id=str(data.get('id') or data.get('tradeId') or ''),
            symbol=symbol,
            side=side,
            amount=self._safe_decimal(data.get('amount') or data.get('size') or data.get('qty')),
            price=self._safe_decimal(data.get('price')),
            cost=self._safe_decimal(data.get('cost') or data.get('quoteQty')),
            fee=None,  # GRVT trade数据中可能不包含fee
            timestamp=timestamp or datetime.now(),
            order_id=str(data.get('orderId', '')),
            raw_data=data
        )
    
    def _parse_order(self, data: Dict[str, Any], symbol: str = None) -> OrderData:
        """解析订单数据"""
        # GRVT订单格式需要根据实际API响应调整
        order_symbol = symbol or data.get('instrument') or data.get('symbol', '')
        if order_symbol:
            order_symbol = self.map_symbol_from_grvt(order_symbol)
        
        # 解析订单状态
        status_str = data.get('status') or data.get('state') or 'pending'
        status = self._parse_order_status(status_str)
        
        # 解析订单类型和方向
        order_type_str = data.get('orderType') or data.get('type') or 'limit'
        order_type = self._parse_order_type(order_type_str)
        
        side_str = data.get('side') or data.get('direction') or 'buy'
        side = self._parse_order_side(side_str)
        
        # 解析数量
        amount = self._safe_decimal(data.get('amount') or data.get('size') or data.get('quantity'))
        filled = self._safe_decimal(data.get('filled') or data.get('filledSize') or data.get('executedQty'))
        remaining = amount - filled if amount and filled else amount
        
        # 解析价格
        price = self._safe_decimal(data.get('price') or data.get('limitPrice'))
        average = self._safe_decimal(data.get('average') or data.get('avgPrice'))
        
        # 解析时间戳
        timestamp = None
        if 'timestamp' in data:
            timestamp = self._parse_timestamp(data['timestamp'], 'ns')
        elif 'createdAt' in data:
            timestamp = self._parse_timestamp(data['createdAt'], 'ns')
        elif 'time' in data:
            timestamp = self._parse_timestamp(data['time'], 'ns')
        
        updated = None
        if 'updatedAt' in data:
            updated = self._parse_timestamp(data['updatedAt'], 'ns')
        elif 'updateTime' in data:
            updated = self._parse_timestamp(data['updateTime'], 'ns')
        
        return OrderData(
            id=str(data.get('id') or data.get('orderId') or ''),
            client_id=str(data.get('clientOrderId') or data.get('clientId') or ''),
            symbol=order_symbol,
            side=side,
            type=order_type,
            amount=amount,
            price=price,
            filled=filled,
            remaining=remaining,
            cost=self._safe_decimal(data.get('cost') or (filled * average if filled and average else None)),
            average=average,
            status=status,
            timestamp=timestamp or datetime.now(),
            updated=updated,
            fee=None,  # 需要从其他接口获取
            trades=[],
            params=data.get('params', {}),
            raw_data=data
        )
    
    def _parse_balance(self, data: Dict[str, Any], currency: str = None) -> BalanceData:
        """解析余额数据"""
        # GRVT余额格式需要根据实际API响应调整
        currency = currency or data.get('currency') or data.get('asset', '')
        
        return BalanceData(
            currency=currency,
            free=self._safe_decimal(data.get('free') or data.get('available') or data.get('availableBalance')),
            used=self._safe_decimal(data.get('used') or data.get('locked') or data.get('lockedBalance')),
            total=self._safe_decimal(data.get('total') or data.get('balance') or data.get('totalBalance')),
            raw_data=data
        )
    
    def _parse_position(self, data: Dict[str, Any], symbol: str = None) -> PositionData:
        """解析持仓数据"""
        # GRVT持仓格式需要根据实际API响应调整
        position_symbol = symbol or data.get('instrument') or data.get('symbol', '')
        if position_symbol:
            position_symbol = self.map_symbol_from_grvt(position_symbol)
        
        # 解析持仓方向
        side_str = data.get('side') or data.get('direction')
        if side_str:
            side = self._parse_position_side(side_str)
        else:
            # 根据数量判断方向
            size = self._safe_decimal(data.get('size') or data.get('quantity'))
            side = PositionSide.LONG if size >= 0 else PositionSide.SHORT
        
        size = self._safe_decimal(data.get('size') or data.get('quantity') or data.get('positionSize'))
        entry_price = self._safe_decimal(data.get('entryPrice') or data.get('avgEntryPrice'))
        mark_price = self._safe_decimal(data.get('markPrice') or data.get('markPrice'))
        liquidation_price = self._safe_decimal(data.get('liquidationPrice'))
        
        # 计算未实现盈亏
        unrealized_pnl = None
        if size and entry_price and mark_price:
            if side == PositionSide.LONG:
                unrealized_pnl = (mark_price - entry_price) * abs(size)
            else:
                unrealized_pnl = (entry_price - mark_price) * abs(size)
        else:
            unrealized_pnl = self._safe_decimal(data.get('unrealizedPnl') or data.get('unrealizedPnl'))
        
        return PositionData(
            symbol=position_symbol,
            side=side,
            size=abs(size) if size else Decimal('0'),
            entry_price=entry_price,
            mark_price=mark_price,
            liquidation_price=liquidation_price,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=self._safe_decimal(data.get('realizedPnl')),
            margin=self._safe_decimal(data.get('margin') or data.get('marginUsed')),
            leverage=self._safe_int(data.get('leverage')),
            raw_data=data
        )
    
    # ==================== 工具方法 ====================
    
    def format_quantity(self, symbol: str, quantity: Decimal, symbol_info: Dict[str, Any] = None) -> Decimal:
        """格式化数量精度"""
        if symbol_info:
            min_size = self._safe_decimal(symbol_info.get('min_size') or symbol_info.get('minSize'))
            step_size = self._safe_decimal(symbol_info.get('step_size') or symbol_info.get('stepSize'))
            
            if step_size:
                # 向下取整到step_size的倍数
                quantity = (quantity // step_size) * step_size
            
            if min_size and quantity < min_size:
                quantity = min_size
        
        return quantity
    
    def format_price(self, symbol: str, price: Decimal, symbol_info: Dict[str, Any] = None) -> Decimal:
        """格式化价格精度"""
        if symbol_info:
            tick_size = self._safe_decimal(symbol_info.get('tick_size') or symbol_info.get('tickSize'))
            
            if tick_size:
                # 向下取整到tick_size的倍数
                price = (price // tick_size) * tick_size
        
        return price

