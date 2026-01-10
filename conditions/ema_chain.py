from backtest_framework import Strategy
import pandas as pd
import numpy as np

class EmaChainConditions(Strategy):
    """
    VECTORIZED (Fast) EMA Chain Calculation
    1. prep_data() ile tüm dosyanın EMA'ları tek seferde hesaplanır.
    2. on_candle() sadece hazır diziden veri okur.
    """
    def __init__(self, bet_size=10.0, side="LONG", tp=0.04, sl=0.04, **kwargs):
        super().__init__(bet_size=bet_size)
        
        self.tp = tp
        self.sl = sl
        
        # --- 1. EMA AYARLARI ---
        self.periods = [9, 20, 50, 100, 200, 300, 500, 1000, 2000, 5000]

        # Vektörel veri saklama (Dictionary of Numpy Arrays)
        self.ema_arrays = {} 
        self.cursor = 0 # Şu an kaçıncı mumdayız (Array index)
        self.data_len = 0

        # --- 2. KOŞUL DURUMLARI (STATE) ---
        self.conditions = {
            'small_bull': False,
            'small_bear': False,
            'big_bull': False,
            'big_bear': False,
            'all_bull': False,            
            'all_bear': False,            
            'correction_long': False,     
            'reaction_short': False       
        }

    def prep_data(self, df: pd.DataFrame):
        """
        OPTIMIZATION:
        Tüm EMA'ları döngüye girmeden önce Pandas ile tek seferde hesapla.
        """
        closes = df['close']
        self.data_len = len(closes)
        self.cursor = 0
        
        # Her periyot için tüm seriyi hesapla
        for p in self.periods:
            # Pandas ewm fonksiyonu C-optimized olduğu için çok hızlıdır
            ema_series = closes.ewm(span=p, adjust=False, min_periods=p).mean()
            # Hızlı erişim için numpy array'e çevir ve sakla
            self.ema_arrays[p] = ema_series.to_numpy()
            # NaN değerleri None veya -1 yapabiliriz, ama float arrayde NaN kalması daha güvenli
            # Hesaplama sırasında NaN kontrolü yapacağız.

    def _check_bullish_chain(self, periods, i_param):
        """
        Hafızadaki array'den o anki (self.cursor) değeri okuyarak kontrol eder.
        """
        idx_now = self.cursor
        
        for idx in range(len(periods) - 1):
            fast_p = periods[idx]
            slow_p = periods[idx+1]
            
            # Array sınır kontrolü (Güvenlik)
            if idx_now >= len(self.ema_arrays[fast_p]): return False

            val_fast = self.ema_arrays[fast_p][idx_now]
            val_slow = self.ema_arrays[slow_p][idx_now]
            
            # NaN kontrolü (EMA henüz oluşmadıysa)
            if np.isnan(val_fast) or np.isnan(val_slow):
                return False
            
            threshold = val_slow * (1 + i_param / 100.0)
            
            if val_fast <= threshold:
                return False
        return True

    def _check_bearish_chain(self, periods, i_param):
        idx_now = self.cursor
        
        for idx in range(len(periods) - 1):
            fast_p = periods[idx]
            slow_p = periods[idx+1]
            
            if idx_now >= len(self.ema_arrays[fast_p]): return False

            val_fast = self.ema_arrays[fast_p][idx_now]
            val_slow = self.ema_arrays[slow_p][idx_now]
            
            if np.isnan(val_fast) or np.isnan(val_slow):
                return False
            
            threshold = val_slow * (1 - i_param / 100.0)
            
            if val_fast >= threshold:
                return False
        return True

    def on_candle(self, timestamp, open, high, low, close):
        # Array bounds check
        if self.cursor >= self.data_len:
            return None

        # En büyük EMA (5000) hazır mı?
        # (Numpy arrayde o indexteki değer NaN değilse hazırdır)
        val_5000 = self.ema_arrays[5000][self.cursor]
        
        if np.isnan(val_5000):
            self.cursor += 1
            return None

        # --- KOŞUL HESAPLAMALARI (Lookup from Array) ---
        
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
        
        self.conditions['all_bull'] = is_big_bull and is_small_bull
        self.conditions['all_bear'] = is_big_bear and is_small_bear
        self.conditions['correction_long'] = is_big_bull and is_small_bear
        self.conditions['reaction_short'] = is_big_bear and is_small_bull

        # İlerlet
        self.cursor += 1
        
        return None 
