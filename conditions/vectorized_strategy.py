"""
Vektörel EMA + Pump + Marubozu Stratejisi
==========================================
Bu sınıf, piyasa verilerini vektörel (toplu) şekilde işleyerek hızlı backtest yapılmasını sağlar.

GELECEKTEKİ AGENTLAR İÇİN KRİTİK KURALLAR (USER DIRECTIVE):
1. Google Sheets API kararlılığı için toplu deneyler (batch) her zaman sıralı (sequential) yürütülmelidir. 
2. Tek bir backtest koşturulurken pairlar 8 çekirdek ile paralel işlenmelidir (Hız için).
3. Strateji hesaplamaları Pandas kütüphanesi ile vektörel yapılmalıdır.
4. SIDES (LONG/SHORT) ve TRIGGER CONDITIONS (PUMP/DUMP) tamamen bağımsızdır.
5. Google Sheets loglarına 'VECTORIZED' kelimesi yazılmamalıdır.
"""

from backtest_framework import Strategy
import pandas as pd
import numpy as np
import polars as pl

class VectorizedStrategy(Strategy):
    """
    Vektörel Strateji Sınıfı
    ------------------------
    Üç ana filtreyi (EMA, Pump/Dump, Marubozu) kullanarak giriş sinyali üretir.
    Veriyi toplu işlem (Pandas/Polars) yaparak performansı maksimize eder.

    GELECEKTEKİ AGENTLAR İÇİN KRİTİK NOT:
    SIDES (LONG/SHORT) ve TRIGGER CONDITIONS (PUMP/DUMP) birbirinden TAMAMEN bağımsızdır:
    - LONG after PUMP (Trend Following) veya LONG after DUMP (Mean Reversion)
    - SHORT after PUMP (Mean Reversion) veya SHORT after DUMP (Trend Following)
    Kodun içine hiçbir side-condition bağımlılığı sert şekilde yazılmamalıdır.
    """
    
    # =========================================================================
    # BÖLÜM 1: BAŞLATMA (INITIALIZATION)
    # =========================================================================
    def __init__(self, tp=0.04, sl=0.02, tsl=0.0, bet_size=7.0, side="SHORT", cond="pump",
                 pump_threshold=0.02, dump_threshold=0.02, marubozu_threshold=0.80, ema="none", **kwargs):
        """
        Strateji konfigürasyonunu ayarlar.
        
        GELECEKTEKİ AGENTLAR İÇİN NOT:
        - 'side' (LONG/SHORT) sadece işlem yönünü ve TP/SL mantığını belirler.
        - 'cond' (pump/dump) ise giriş sinyalinin neye göre (yükseliş/düşüş) tetikleneceğini belirler.
        Bu ikisi arasında sabit bir bağ yoktur; her kombinasyon test edilebilir.
        """
        super().__init__(bet_size=bet_size, tsl=tsl)
        
        # Temel parametreler
        self.tp = tp                          # Take Profit
        self.sl = sl                          # Stop Loss
        self.side = side                      # LONG veya SHORT (Trade Direction)
        self.cond = cond.lower()              # pump veya dump (Trigger Condition)
        self.pump_threshold = pump_threshold  # % Rise threshold
        self.dump_threshold = dump_threshold  # % Drop threshold
        self.marubozu_threshold = marubozu_threshold  # Marubozu eşiği
        self.ema = ema.lower()                # EMA durumu (bull/bear/none)
        
        # EMA Periyotları - Her zaman hesaplanır (filter "none" olsa bile)
        self.periods = [9, 20, 50, 100, 200, 300, 500, 1000, 2000, 5000]
    
    # =========================================================================
    # BÖLÜM 2: DOSYA OKUMA
    # =========================================================================
    def process_file(self, filepath):
        # Dosyayı okur ve veri işleme fonksiyonuna gönderir
        try:
            df = pd.read_parquet(filepath)
            return self.process_data(df)
        except Exception:
            return None

    # =========================================================================
    # BÖLÜM 3: ANA İŞLEME FONKSİYONU
    # =========================================================================
    def process_data(self, df):
        """
        Pandas DataFrame üzerinde vektörel sinyal hesaplaması yapar.
        
        AKIS:
        1. Gerekli EMA indikatörlerini toplu hesaplar (sadece ihtiyaç duyulan periyotlar).
        2. Mumların Pump (yükseliş) veya Dump (düşüş) durumlarını yüzdelik olarak belirler.
        3. Marubozu oranlarını (gövde/fitil) hesaplar.
        4. 'cond' parametresine göre sinyali (entry_signal) oluşturur.
        """
        try:
            # Boş veri kontrolü
            if df.empty:
                return None

            # Fiyat sütunlarını al
            closes = df['close']
            opens = df['open']
            highs = df['high']
            lows = df['low']
            
            # EMA filtrelemesi ve optimizasyonu
            # Sadece seçilen filtreye uygun EMA'ları hesapla (Hız optimizasyonu)
            
            ema_filter = None

            # Hangi EMA'lar lazım?
            needed_periods = []
            if self.ema == "none":
                needed_periods = []
            elif "small" in self.ema:
                needed_periods = [9, 20, 50, 100, 200]
            elif "big" in self.ema:
                needed_periods = [300, 500, 1000, 2000, 5000]
            elif "all" in self.ema:
                needed_periods = self.periods # [9...5000]
            elif "big_" in self.ema and "_small_" in self.ema:
                # Need both segments
                needed_periods = self.periods # Simplest is to calculate all for hybrids
            
            # Eğer EMA gerekliyse hesapla
            if needed_periods:
                ema_dict = {}
                for p in needed_periods:
                   ema_dict[p] = closes.ewm(span=p, adjust=False, min_periods=p).mean()
                
                # Zincir kontrol fonksiyonu
                def check_chain_optimized(periods, bullish):
                    mask = pd.Series(True, index=df.index)
                    threshold_pct = 0.00001/100.0 if bullish else 0.0001/100.0
                    
                    for i in range(len(periods) - 1):
                        fast_p = periods[i]
                        slow_p = periods[i + 1]
                        val_fast = ema_dict[fast_p]
                        val_slow = ema_dict[slow_p]
                        
                        if bullish:
                            # 9 > 20
                            threshold = val_slow * (1 + threshold_pct)
                            mask = mask & (val_fast > threshold)
                        else:
                            # 9 < 20
                            threshold = val_slow * (1 - threshold_pct)
                            mask = mask & (val_fast < threshold)
                    return mask

                # İlgili filtreyi uygula
                if self.ema == "all_bull":
                    ema_filter = check_chain_optimized(needed_periods, True)
                elif self.ema == "all_bear":
                    ema_filter = check_chain_optimized(needed_periods, False)
                elif self.ema == "small_bull":
                    ema_filter = check_chain_optimized(needed_periods, True)
                elif self.ema == "small_bear":
                    ema_filter = check_chain_optimized(needed_periods, False)
                elif self.ema == "big_bull":
                    ema_filter = check_chain_optimized(needed_periods, True)
                elif self.ema == "big_bear":
                    ema_filter = check_chain_optimized(needed_periods, False)
                elif "big_" in self.ema and "_small_" in self.ema:
                    # Hybrid EMA: big_bull_small_bear, big_bear_small_bull etc.
                    # Parse filters
                    parts = self.ema.split('_') # ['big', 'bull', 'small', 'bear']
                    big_side = parts[1]
                    small_side = parts[3]
                    
                    # Calculate Big Segments (300-5000)
                    big_periods = [300, 500, 1000, 2000, 5000]
                    big_filter = check_chain_optimized(big_periods, big_side == "bull")
                    
                    # Calculate Small Segments (9-200)
                    small_periods = [9, 20, 50, 100, 200]
                    small_filter = check_chain_optimized(small_periods, small_side == "bull")
                    
                    ema_filter = big_filter & small_filter
            
            # Eğer filtre hesaplanmadıysa (none veya hata), hepsi True
            if ema_filter is None:
                ema_filter = pd.Series(True, index=df.index)

            # Pump ve Dump durumlarını hesapla
            pump_pct = (closes - opens) / opens
            is_pump_up = pump_pct > self.pump_threshold
            is_pump_down = pump_pct < -self.dump_threshold
            
            # Marubozu (gövde doluluğu) oranını hesapla
            body_size = (closes - opens).abs()
            total_range = highs - lows
            marubozu_ratio = pd.Series(0.0, index=df.index)
            valid_range = total_range > 0
            marubozu_ratio[valid_range] = body_size[valid_range] / total_range[valid_range]
            
            # Eşik kontrolü
            is_marubozu = marubozu_ratio >= self.marubozu_threshold
            
            # Bağımsız giriş mantığı (Sadece 'cond' değerine bakar)
            
            if self.cond == "dump":
                # Trigger on red marubozu drop
                final_signal = is_pump_down & is_marubozu & ema_filter
            else:
                # Trigger on green marubozu rise (default: pump)
                final_signal = is_pump_up & is_marubozu & ema_filter
            
            # Sonuçları kontrol et ve DataFrame'i döndür
            # Hiç sinyal yoksa None döndür
            if not final_signal.any():
                return None
                
            # Sinyal sütununu ekle
            df['entry_signal'] = final_signal
            
            # Polars DataFrame'e çevir (backtest_framework bunu bekliyor)
            pl_df = pl.from_pandas(df)
            
            return pl_df
            
        except Exception as e:
            # Hata durumunda sessizce None döndür
            # (Bazı semboller için veri eksik olabilir)
            print(f"DEBUG ERROR: {e}")
            return None

    # =========================================================================
    # BÖLÜM 4: MUM MUM İŞLEME (KULLANILMIYOR)
    # =========================================================================
    def on_candle(self, timestamp, open, high, low, close):
        """
        Tek mum işleme fonksiyonu.
        Vektörel modda kullanılmıyor - sadece uyumluluk için var.
        """
        pass
