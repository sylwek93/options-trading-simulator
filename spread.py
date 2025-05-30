from datetime import datetime
from database import query_option_chain
import polars as pl

class PutCreditSpread:
    def __init__(self):

        self.spread_type = 'put_spread'
        self.current_status = 'active'
        self.slippage = None
        self.commission = None
        self.spx_price = None
        self.entry_time_str = None
        self.entry_date = None
        self.entry_time = None
        self.width = None
        self.offset = None
        self.stop_loss_type = None
        self.take_profit_level = None
        self.strikes = None
        self.max_loss = None
        self.max_profit = None
        self.break_even_level = None
        self.break_even_time = None
        self.entry_price = None
        self.exit_time = None
        self.exit_price = None
        self.pnl = None
        self.outcome = None

    def _get_spread_strikes(self):
        sell_leg = (round(self.spx_price / 5) * 5) + self.offset
        buy_leg = sell_leg - self.width
        return sell_leg, buy_leg
    
    def _calc_rounded_price(self, buy_ask, buy_bid, sell_ask, sell_bid):
  
        buy_ask = max(0, buy_ask)
        buy_bid = max(0, buy_bid)
        sell_ask = max(0, sell_ask)
        sell_bid = max(0, sell_bid)

        spread_price = round((round(((buy_ask - sell_ask) + (buy_bid - sell_bid)) / 2 / 0.05) * 0.05) + self.slippage, 2)

        if spread_price > 0:
            spread_price = - self.width
        
        return spread_price
    
    def _calculate_max_loss(self, entry_price, sell_strike, buy_strike):
        return -round(((sell_strike - buy_strike) + entry_price) * 100, 2)

    def _calculate_max_profit(self, entry_price):
        return round(-entry_price * 100, 2)
    
    def _calculate_break_even_points(self, entry_price, sell_strike):
        return round(sell_strike + entry_price, 2)
    
    def _hit_break_even_level(self, spx_price):
        return spx_price < self.break_even_level
    
    def _hit_take_profit_level(self, spread_price):
        return spread_price > round(self.entry_price * self.take_profit_level, 1)
    
    
    def get_spread_data(self, spx_price, eod_spx_price, current_time, strategy_data, slippage, commission):

        self.slippage = slippage
        self.commission = commission
        self.spx_price = spx_price
        self.entry_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        self.entry_date = current_time.strftime('%Y-%m-%d')
        self.entry_time = current_time.strftime('%H:%M:%S')
        self.width = strategy_data['width']
        self.offset = strategy_data['offset']
        self.stop_loss_type = strategy_data['stop_loss_type']
        self.take_profit_level = strategy_data['take_profit_level']

        sell_leg, buy_leg = self._get_spread_strikes()
        self.strikes = [sell_leg, buy_leg]

        sell_leg_data = query_option_chain(self.entry_date, self.entry_time, 'P', sell_leg)
        buy_leg_data = query_option_chain(self.entry_date, self.entry_time, 'P', buy_leg)

        
        sell_leg_data = sell_leg_data.rename({
            'strike': 'sell_strike',
            'bid': 'sell_bid',
            'ask': 'sell_ask'
        })
        
        buy_leg_data = buy_leg_data.rename({
            'strike': 'buy_strike',
            'bid': 'buy_bid',
            'ask': 'buy_ask'
        })
        
        spread_data = sell_leg_data.join(
            buy_leg_data,
            on='time',
            how='inner'
        )
        
        spread_data = spread_data.drop('spx_price_right')
        spread_data = spread_data.rename({'spx_price': 'spx_price'})
  
        spread_data = spread_data.with_columns(
            pl.struct(['buy_ask', 'buy_bid', 'sell_ask', 'sell_bid'])
            .map_elements(
                lambda row: self._calc_rounded_price(
                    row['buy_ask'], 
                    row['buy_bid'], 
                    row['sell_ask'], 
                    row['sell_bid']
                ),
                return_dtype=pl.Float64
            )
            .alias('spread_price')
        )

        self.entry_price = spread_data['spread_price'][0]
        self.max_loss = self._calculate_max_loss(self.entry_price, self.strikes[0], self.strikes[1])
        self.max_profit = self._calculate_max_profit(self.entry_price)
        self.break_even_level = self._calculate_break_even_points(self.entry_price, self.strikes[0])

        spread_data = spread_data.with_columns(
            pl.struct(['spx_price'])
            .map_elements(
                lambda row: self._hit_break_even_level(
                    row['spx_price']
                ), 
                return_dtype=pl.Boolean
            )
            .alias('is_break_even')
        )
        
        if spread_data.filter(pl.col('is_break_even') == True).height > 0:
            self.break_even_time = spread_data.filter(pl.col('is_break_even') == True).select('time').row(0)[0]

        spread_data = spread_data.with_columns(
            pl.struct(['spread_price'])
            .map_elements(
                lambda row: self._hit_take_profit_level(
                    row['spread_price']
                ),
                return_dtype=pl.Boolean
            )
            .alias('is_take_profit')
        )

        if spread_data.filter(pl.col('is_take_profit') == True).height > 0:
            self.exit_time = spread_data.filter(pl.col('is_take_profit') == True).select('time').row(0)[0]


        if self.stop_loss_type == 'bep':
            if self.break_even_time and self.exit_time:
                break_even_time_dt = datetime.strptime(self.break_even_time, '%H:%M:%S')
                exit_time_dt = datetime.strptime(self.exit_time, '%H:%M:%S')
                
                if break_even_time_dt < exit_time_dt:
                    self.exit_time = self.break_even_time
                    self.outcome = 'stop_loss'
                
            elif self.break_even_time and not self.exit_time:
                self.exit_time = self.break_even_time
                self.outcome = 'stop_loss'
            elif not self.break_even_time and self.exit_time:
                self.exit_time = self.exit_time
                self.outcome = 'take_profit'
            else:
                self.exit_time = spread_data.select('time').row(-1)[0]
                self.outcome = 'expire'

        elif self.stop_loss_type == 'expire':
            if self.exit_time:
                self.exit_time = self.exit_time
                self.outcome = 'take_profit'
            else:
                self.exit_time = spread_data.select('time').row(-1)[0]
                self.outcome = 'expire'
        
          
        self.exit_price = spread_data.filter(pl.col('time') == self.exit_time).select('spread_price').row(0)[0]

        validation_time = datetime.strptime(spread_data.select('time').row(-1)[0], '%H:%M:%S')

        if validation_time < datetime.strptime('21:50:00', '%H:%M:%S'):
            if eod_spx_price > self.strikes[0]:
                self.exit_time = spread_data.select('time').row(-1)[0]
                self.outcome = 'take_profit'
                self.exit_price = round(self.entry_price * self.take_profit_level, 1)
            else:
                self.exit_time = '22:00:00'
                self.outcome = 'expire'
                self.exit_price = -self.width


        exit_time_str = f"{self.entry_date} {self.exit_time}"
        self.exit_time = exit_time_str

        self.pnl = - round(((self.entry_price - self.exit_price) * 100) + self.commission, 2)

        if self.pnl < self.max_loss:
            self.pnl = self.max_loss - self.commission
        
        if self.pnl > self.max_profit:
            self.pnl = self.max_profit - self.commission


        if self.entry_price < - self.width:
            return None
        

        return_df = pl.DataFrame({
            'spread_type': pl.Series([self.spread_type], dtype=pl.Utf8),
            'width': pl.Series([self.width], dtype=pl.Float64),
            'offset': pl.Series([self.offset], dtype=pl.Float64),
            'stop_loss_type': pl.Series([self.stop_loss_type], dtype=pl.Utf8),
            'take_profit_level': pl.Series([self.take_profit_level], dtype=pl.Float64),
            'strikes': pl.Series([[float(strike) for strike in self.strikes]], dtype=pl.List(pl.Float64)),
            'max_loss': pl.Series([self.max_loss], dtype=pl.Float64),
            'max_profit': pl.Series([self.max_profit], dtype=pl.Float64),
            'break_even_level': pl.Series([self.break_even_level], dtype=pl.Float64),
            'break_even_time': pl.Series([self.break_even_time if self.break_even_time else None], dtype=pl.Utf8),
            'entry_time': pl.Series([self.entry_time_str], dtype=pl.Utf8),
            'entry_price': pl.Series([self.entry_price], dtype=pl.Float64),
            'exit_time': pl.Series([self.exit_time], dtype=pl.Utf8),
            'exit_price': pl.Series([self.exit_price], dtype=pl.Float64),
            'pnl': pl.Series([self.pnl], dtype=pl.Float64),
            'outcome': pl.Series([self.outcome], dtype=pl.Utf8),
            'current_status': pl.Series([self.current_status], dtype=pl.Utf8)
        })
        
        #spread_data.write_csv('tests/test.csv')
        return return_df

        
class CallCreditSpread:
    def __init__(self):

        self.spread_type = 'call_spread'
        self.current_status = 'active'
        self.slippage = None
        self.commission = None
        self.spx_price = None
        self.entry_time_str = None
        self.entry_date = None
        self.entry_time = None
        self.width = None
        self.offset = None
        self.stop_loss_type = None
        self.take_profit_level = None
        self.strikes = None
        self.max_loss = None
        self.max_profit = None
        self.break_even_level = None
        self.break_even_time = None
        self.entry_price = None
        self.exit_time = None
        self.exit_price = None
        self.pnl = None
        self.outcome = None

    def _get_spread_strikes(self):
        sell_leg = (round(self.spx_price / 5) * 5) - self.offset
        buy_leg = sell_leg + self.width
        return sell_leg, buy_leg

    def _calc_rounded_price(self, buy_ask, buy_bid, sell_ask, sell_bid):

        buy_ask = max(0, buy_ask)
        buy_bid = max(0, buy_bid)
        sell_ask = max(0, sell_ask)
        sell_bid = max(0, sell_bid)

        spread_price = round((round(
            ((buy_ask - sell_ask) + (buy_bid - sell_bid)) / 2 / 0.05) * 0.05) + self.slippage, 2)

        if spread_price > 0:
            spread_price = - self.width

        return spread_price

    def _calculate_max_loss(self, entry_price, sell_strike, buy_strike):
        return -round(((buy_strike - sell_strike) + entry_price) * 100, 2)

    def _calculate_max_profit(self, entry_price):
        return round(-entry_price * 100, 2)

    def _calculate_break_even_points(self, entry_price, sell_strike):
        return round(sell_strike - entry_price, 2)

    def _hit_break_even_level(self, spx_price):
        return spx_price > self.break_even_level

    def _hit_take_profit_level(self, spread_price):
        return spread_price > round(self.entry_price * self.take_profit_level, 1)

    def get_spread_data(self, spx_price, eod_spx_price, current_time, strategy_data, slippage, commission):

        self.slippage = slippage
        self.commission = commission
        self.spx_price = spx_price
        self.entry_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        self.entry_date = current_time.strftime('%Y-%m-%d')
        self.entry_time = current_time.strftime('%H:%M:%S')
        self.width = strategy_data['width']
        self.offset = strategy_data['offset']
        self.stop_loss_type = strategy_data['stop_loss_type']
        self.take_profit_level = strategy_data['take_profit_level']

        sell_leg, buy_leg = self._get_spread_strikes()
        self.strikes = [sell_leg, buy_leg]

        sell_leg_data = query_option_chain(self.entry_date, self.entry_time, 'C', sell_leg)
        buy_leg_data = query_option_chain(self.entry_date, self.entry_time, 'C', buy_leg)

        sell_leg_data = sell_leg_data.rename({
            'strike': 'sell_strike',
            'bid': 'sell_bid',
            'ask': 'sell_ask'
        })

        buy_leg_data = buy_leg_data.rename({
            'strike': 'buy_strike',
            'bid': 'buy_bid',
            'ask': 'buy_ask'
        })

        spread_data = sell_leg_data.join(
            buy_leg_data,
            on='time',
            how='inner'
        )

        spread_data = spread_data.drop('spx_price_right')
        spread_data = spread_data.rename({'spx_price': 'spx_price'})

        spread_data = spread_data.with_columns(
            pl.struct(['buy_ask', 'buy_bid', 'sell_ask', 'sell_bid'])
            .map_elements(
                lambda row: self._calc_rounded_price(
                    row['buy_ask'],
                    row['buy_bid'],
                    row['sell_ask'],
                    row['sell_bid']
                ),
                return_dtype=pl.Float64
            )
            .alias('spread_price')
        )

        self.entry_price = spread_data['spread_price'][0]
        self.max_loss = self._calculate_max_loss(self.entry_price, self.strikes[0], self.strikes[1])
        self.max_profit = self._calculate_max_profit(self.entry_price)
        self.break_even_level = self._calculate_break_even_points(self.entry_price, self.strikes[0])

        spread_data = spread_data.with_columns(
            pl.struct(['spx_price'])
            .map_elements(
                lambda row: self._hit_break_even_level(
                    row['spx_price']
                ),
                return_dtype=pl.Boolean
            )
            .alias('is_break_even')
        )

        if spread_data.filter(pl.col('is_break_even') == True).height > 0:
            self.break_even_time = spread_data.filter(
                pl.col('is_break_even') == True).select('time').row(0)[0]

        spread_data = spread_data.with_columns(
            pl.struct(['spread_price'])
            .map_elements(
                lambda row: self._hit_take_profit_level(
                    row['spread_price']
                ),
                return_dtype=pl.Boolean
            )
            .alias('is_take_profit')
        )

        if spread_data.filter(pl.col('is_take_profit') == True).height > 0:
            self.exit_time = spread_data.filter(
                pl.col('is_take_profit') == True).select('time').row(0)[0]

        if self.stop_loss_type == 'bep':
            if self.break_even_time and self.exit_time:
                break_even_time_dt = datetime.strptime(self.break_even_time, '%H:%M:%S')
                exit_time_dt = datetime.strptime(self.exit_time, '%H:%M:%S')

                if break_even_time_dt < exit_time_dt:
                    self.exit_time = self.break_even_time
                    self.outcome = 'stop_loss'

            elif self.break_even_time and not self.exit_time:
                self.exit_time = self.break_even_time
                self.outcome = 'stop_loss'
            elif not self.break_even_time and self.exit_time:
                self.exit_time = self.exit_time
                self.outcome = 'take_profit'
            else:
                self.exit_time = spread_data.select('time').row(-1)[0]
                self.outcome = 'expire'

        elif self.stop_loss_type == 'expire':
            if self.exit_time:
                self.exit_time = self.exit_time
                self.outcome = 'take_profit'
            else:
                self.exit_time = spread_data.select('time').row(-1)[0]
                self.outcome = 'expire'

        self.exit_price = spread_data.filter(pl.col('time') == self.exit_time).select('spread_price').row(0)[0]

        validation_time = datetime.strptime(spread_data.select('time').row(-1)[0], '%H:%M:%S')

        if validation_time < datetime.strptime('21:50:00', '%H:%M:%S'):
            if eod_spx_price < self.strikes[0]:
                self.exit_time = spread_data.select('time').row(-1)[0]
                self.outcome = 'take_profit'
                self.exit_price = round(self.entry_price * self.take_profit_level, 1)
            else:
                self.exit_time = '22:00:00'
                self.outcome = 'expire'
                self.exit_price = -self.width

        exit_time_str = f"{self.entry_date} {self.exit_time}"
        self.exit_time = exit_time_str

        self.pnl = - round(((self.entry_price - self.exit_price) * 100) + self.commission, 2)

        if self.pnl < self.max_loss:
            self.pnl = self.max_loss - self.commission

        if self.pnl > self.max_profit:
            self.pnl = self.max_profit - self.commission

        if self.entry_price < - self.width:
            return None

        return_df = pl.DataFrame({
            'spread_type': pl.Series([self.spread_type], dtype=pl.Utf8),
            'width': pl.Series([self.width], dtype=pl.Float64),
            'offset': pl.Series([self.offset], dtype=pl.Float64),
            'stop_loss_type': pl.Series([self.stop_loss_type], dtype=pl.Utf8),
            'take_profit_level': pl.Series([self.take_profit_level], dtype=pl.Float64),
            'strikes': pl.Series([[float(strike) for strike in self.strikes]], dtype=pl.List(pl.Float64)),
            'max_loss': pl.Series([self.max_loss], dtype=pl.Float64),
            'max_profit': pl.Series([self.max_profit], dtype=pl.Float64),
            'break_even_level': pl.Series([self.break_even_level], dtype=pl.Float64),
            'break_even_time': pl.Series([self.break_even_time if self.break_even_time else None], dtype=pl.Utf8),
            'entry_time': pl.Series([self.entry_time_str], dtype=pl.Utf8),
            'entry_price': pl.Series([self.entry_price], dtype=pl.Float64),
            'exit_time': pl.Series([self.exit_time], dtype=pl.Utf8),
            'exit_price': pl.Series([self.exit_price], dtype=pl.Float64),
            'pnl': pl.Series([self.pnl], dtype=pl.Float64),
            'outcome': pl.Series([self.outcome], dtype=pl.Utf8),
            'current_status': pl.Series([self.current_status], dtype=pl.Utf8)
        })

        #spread_data.write_csv('tests/test.csv')
        return return_df
        


'''start_time = datetime.strptime(f"2025-05-23 15:35:00", '%Y-%m-%d %H:%M:%S')

strategies_data = {
    'entries': "",
    'max_active_positions': 1,
    'width': 5,
    'offset': -10,
    'stop_loss_type': 'expire',
    'take_profit_level': 0.01,
    'active_positions': 0
}

p = CallCreditSpread()
p_data = p.get_spread_data(5785.83, 5800, start_time,
                           strategies_data, 0.1, 1.5)
       
print(p_data)'''

