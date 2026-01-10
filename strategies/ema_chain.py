from backtest_framework import Strategy

class EmaChainConditions(Strategy):
    def __init__(self, bet_size=10.0, side="LONG", **kwargs):
        super().__init__(bet_size=bet_size)
        
        # --- 1. EMA AYARLARI ---
        self.periods = [9, 20, 50, 100, 200, 300, 500, 1000, 2000, 5000]
        # Başlangıçta hepsi boş (None)
        self.emas = {p: None for p in self.periods}
        # Hız için çarpanları önceden hesapla: k = 2 / (n + 1)
        self.multipliers = {p: 2 / (p + 1) for p in self.periods}
        self.side = side

        # --- 2. KOŞUL DURUMLARI (STATE) ---
        # Ajanın okuyacağı değişkenler burada saklanır
        self.conditions = {
            'small_bull': False,
            'small_bear': False,
            'big_bull': False,
            'big_bear': False,
            'all_bull': False,            # (Big Bull + Small Bull)
            'all_bear': False,            # (Big Bear + Small Bear)
            'correction_long': False,     # (Big Bull + Small Bear) -> Yükseliş trendinde düzeltme
            'reaction_short': False       # (Big Bear + Small Bull) -> Düşüş trendinde tepki
        }

    def update_emas(self, close):
        """Streaming EMA Hesaplaması (Her mumda çalışır)"""
        for p in self.periods:
            if self.emas[p] is None:
                self.emas[p] = close
            else:
                self.emas[p] = (close - self.emas[p]) * self.multipliers[p] + self.emas[p]

    def _check_bullish_chain(self, periods, i_param):
        """
        Genel Bullish Zincir Kontrolü:
        ema[n] > ema[n+1] * (1 + i/100)
        """
        for idx in range(len(periods) - 1):
            fast_p = periods[idx]
            slow_p = periods[idx+1]
            
            val_fast = self.emas[fast_p]
            val_slow = self.emas[slow_p]
            
            # None kontrolü (EMA henüz hesaplanmadıysa)
            if val_fast is None or val_slow is None:
                return False
            
            threshold = val_slow * (1 + i_param / 100.0)
            
            if val_fast <= threshold:
                return False
        return True

    def _check_bearish_chain(self, periods, i_param):
        """
        Genel Bearish Zincir Kontrolü:
        ema[n] < ema[n+1] * (1 - i/100)
        """
        for idx in range(len(periods) - 1):
            fast_p = periods[idx]
            slow_p = periods[idx+1]
            
            val_fast = self.emas[fast_p]
            val_slow = self.emas[slow_p]
            
            # None kontrolü
            if val_fast is None or val_slow is None:
                return False
            
            threshold = val_slow * (1 - i_param / 100.0)
            
            if val_fast >= threshold:
                return False
        return True

    def on_candle(self, timestamp, open, high, low, close):
        # 1. EMA'ları Güncelle
        self.update_emas(close)
        
        # En büyük EMA (5000) oluşana kadar hesaplama yapma
        if self.emas[5000] is None:
            return None

        # --- KOŞUL HESAPLAMALARI ---
        
        # Parametreler: i=0.00001 (Small Bull)
        small_bull_periods = [9, 20, 50, 100, 200]
        is_small_bull = self._check_bullish_chain(small_bull_periods, i_param=0.00001)
        
        # Parametreler: i=0.0001 (Small Bear)
        small_bear_periods = [9, 20, 50, 100, 200]
        is_small_bear = self._check_bearish_chain(small_bear_periods, i_param=0.0001)
        
        # Parametreler: i=0.0001 (Big Bull)
        big_bull_periods = [300, 500, 1000, 2000, 5000]
        is_big_bull = self._check_bullish_chain(big_bull_periods, i_param=0.0001)
        
        # Parametreler: i=0.001 (Big Bear)
        big_bear_periods = [300, 500, 1000, 2000, 5000]
        is_big_bear = self._check_bearish_chain(big_bear_periods, i_param=0.001)

        # --- SONUÇLARI KAYDET ---
        self.conditions['small_bull'] = is_small_bull
        self.conditions['small_bear'] = is_small_bear
        self.conditions['big_bull'] = is_big_bull
        self.conditions['big_bear'] = is_big_bear
        
        # Kombinasyonlar
        self.conditions['all_bull'] = is_big_bull and is_small_bull
        self.conditions['all_bear'] = is_big_bear and is_small_bear
        self.conditions['correction_long'] = is_big_bull and is_small_bear
        self.conditions['reaction_short'] = is_big_bear and is_small_bull

        # --- İŞLEM MANTIĞI (CUSTOM TRIGGER) ---
        # Kullanıcı İsteği: "All Bull ise ve Pump > %2 ise SHORT aç"
        # Mantık: Çok güçlü yükseliş trendinde (All Bull) ani bir pump (%2) gelince 
        # düzeltme (mean reversion) ihtimaline oynayan bir Short stratejisi.
        
        # 1. Pump Hesapla (Bu mumdaki değişim)
        pump_pct = (close - open) / open if open != 0 else 0
        
        # 2. Kondisyon Kontrolü
        if self.conditions['all_bull'] and pump_pct > 0.02:
             return {
                 'action': 'SHORT',
                 'entry_price': close,
                 # TP/SL main.py'den gelen varsayılanları (self.tp, self.sl) kullanır
                 # Ancak burada hardcode etmek isterseniz değiştirebilirsiniz.
                 'tp': close * (1 - 0.04), # %4 TP
                 'sl': close * (1 + 0.04), # %4 SL
                 'pump_percent': pump_pct
             }
        
        return None 
