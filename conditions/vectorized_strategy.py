"""
Vektörel EMA + Pump + Marubozu Stratejisi
==========================================
Bu strateji tüm veriyi tek seferde işler (vektörel hesaplama).
Döngü yerine Pandas/Numpy kullanarak çok hızlı çalışır.

Kullanım:
- LONG: Boğa trendinde pump sonrası devam
- SHORT: Ayı trendinde pump sonrası geri dönüş
"""

from backtest_framework import Strategy
import pandas as pd
import numpy as np
import polars as pl


class VectorizedStrategy(Strategy):
    """
    Vektörel strateji - 3 koşulu birleştirir:
    1. EMA Zinciri (Tüm EMA'lar sıralı mı?)
    2. Pump Tespiti (Mum %X'den fazla hareket etti mi?)
    3. Marubozu Tespiti (Mum gövdesi fitilsiz mi?)
    """
    
    # =========================================================================
    # BÖLÜM 1: BAŞLATMA (INITIALIZATION)
    # =========================================================================
    def __init__(self, tp=0.04, sl=0.02, bet_size=7.0, side="SHORT",
                 pump_threshold=0.02, marubozu_threshold=0.80, ema="none", **kwargs):
        """
        Strateji parametrelerini ayarla.
        
        Parametreler:
        - tp: Take Profit yüzdesi (0.02 = %2)
        - sl: Stop Loss yüzdesi (0.02 = %2)  
        - bet_size: Pozisyon büyüklüğü (USD)
        - side: İşlem yönü ("LONG" veya "SHORT")
        - pump_threshold: Pump eşiği (0.02 = %2 hareket)
        - marubozu_threshold: Marubozu eşiği (0.80 = gövde >= %80)
        - ema: EMA durumu ("bull" = yukarı sıralı, "bear" = aşağı sıralı, "none" = kullanma)
        """
        super().__init__(bet_size=bet_size)
        
        # Temel parametreler
        self.tp = tp                          # Take Profit
        self.sl = sl                          # Stop Loss
        self.side = side                      # LONG veya SHORT
        self.pump_threshold = pump_threshold  # Pump eşiği
        self.marubozu_threshold = marubozu_threshold  # Marubozu eşiği
        self.ema = ema.lower()                # EMA durumu (bull/bear/none)
        
        # EMA Periyotları - Her zaman hesaplanır (filter "none" olsa bile)
        self.periods = [9, 20, 50, 100, 200, 300, 500, 1000, 2000, 5000]
    
    # =========================================================================
    # BÖLÜM 2: DOSYA OKUMA
    # =========================================================================
    def process_file(self, filepath):
        """
        Parquet dosyasını oku ve işle.
        Bu fonksiyon eski uyumluluk için var.
        """
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
        Tüm veriyi vektörel olarak işle.
        Her satır için döngü yapmadan, tüm hesaplamalar tek seferde yapılır.
        
        Döndürür: Polars DataFrame ('entry_signal' sütunu True olan mumlar sinyal)
        """
        try:
            # Boş veri kontrolü
            if df.empty:
                return None

            # -----------------------------------------------------------------
            # ADIM 1: VERİ SÜTUNLARINI AYIKLA
            # -----------------------------------------------------------------
            closes = df['close']   # Kapanış fiyatları
            opens = df['open']     # Açılış fiyatları
            highs = df['high']     # En yüksek fiyatlar
            lows = df['low']       # En düşük fiyatlar
            
            # -----------------------------------------------------------------
            # ADIM 2: EMA HESAPLAMA VE KONTROL (Conditional Optimization)
            # -----------------------------------------------------------------
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
            
            # Eğer EMA gerekliyse hesapla
            if needed_periods:
                ema_dict = {}
                for p in needed_periods:
                   ema_dict[p] = closes.ewm(span=p, adjust=False, min_periods=p).mean()
                
                # Zincir kontrol fonksiyonu (closure)
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
            
            # Eğer filtre hesaplanmadıysa (none veya hata), hepsi True
            if ema_filter is None:
                ema_filter = pd.Series(True, index=df.index)

            # -----------------------------------------------------------------
            # ADIM 4: PUMP TESPİTİ
            # -----------------------------------------------------------------
            # Pump = (Kapanış - Açılış) / Açılış
            pump_pct = (closes - opens) / opens
            is_pump_up = pump_pct > self.pump_threshold     # Yeşil pump
            is_pump_down = pump_pct < -self.pump_threshold  # Kırmızı dump
            
            # -----------------------------------------------------------------
            # ADIM 5: MARUBOZU TESPİTİ
            # -----------------------------------------------------------------
            body_size = (closes - opens).abs()  # Gövde büyüklüğü (mutlak değer)
            total_range = highs - lows          # Toplam mum uzunluğu (high-low)
            
            # Sıfıra bölme hatası önleme
            marubozu_ratio = pd.Series(0.0, index=df.index)
            valid_range = total_range > 0
            marubozu_ratio[valid_range] = body_size[valid_range] / total_range[valid_range]
            
            # Eşik kontrolü
            is_marubozu = marubozu_ratio >= self.marubozu_threshold
            
            # Final signal: Pump + Marubozu + EMA (HER BİRİ BAĞIMSIZ)
            if self.side == "SHORT":
                final_signal = is_pump_up & is_marubozu & ema_filter
            else:  # LONG
                final_signal = is_pump_up & is_marubozu & ema_filter
            
            # -----------------------------------------------------------------
            # ADIM 7: SONUÇ KONTROLÜ VE DÖNÜŞ
            # -----------------------------------------------------------------
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
