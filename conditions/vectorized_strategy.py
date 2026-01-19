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
            # ADIM 2: TÜM EMA'LARI HESAPLA (Vektörel)
            # -----------------------------------------------------------------
            # Her periyot için EMA hesapla ve sözlükte sakla
            # ewm = Exponential Weighted Mean (Üstel Ağırlıklı Ortalama)
            ema_dict = {}
            for p in self.periods:
                # span=p: Periyot uzunluğu
                # adjust=False: Klasik EMA formülü
                # min_periods=p: İlk p mum NaN olacak (yeterli veri yok)
                ema_dict[p] = closes.ewm(span=p, adjust=False, min_periods=p).mean()
            
            # -----------------------------------------------------------------
            # ADIM 3: EMA ZİNCİR KONTROLÜ
            # -----------------------------------------------------------------
            def check_chain(period_list, threshold_pct, bullish=True):
                """
                EMA zincirinin sıralı olup olmadığını kontrol et.
                
                Bullish (Boğa): EMA9 > EMA20 > EMA50 > ... (küçük > büyük)
                Bearish (Ayı): EMA9 < EMA20 < EMA50 < ... (küçük < büyük)
                
                threshold_pct: Fark için minimum eşik (gürültüyü filtreler)
                """
                # Başlangıçta tüm satırlar True
                mask = pd.Series(True, index=df.index)
                
                # Her ardışık EMA çiftini kontrol et
                for i in range(len(period_list) - 1):
                    fast_p = period_list[i]      # Hızlı EMA (kısa periyot)
                    slow_p = period_list[i + 1]  # Yavaş EMA (uzun periyot)
                    
                    val_fast = ema_dict[fast_p]
                    val_slow = ema_dict[slow_p]
                    
                    if bullish:
                        # Boğa: Hızlı EMA, yavaş EMA'nın üzerinde olmalı
                        threshold = val_slow * (1 + threshold_pct)
                        mask = mask & (val_fast > threshold)
                    else:
                        # Ayı: Hızlı EMA, yavaş EMA'nın altında olmalı
                        threshold = val_slow * (1 - threshold_pct)
                        mask = mask & (val_fast < threshold)
                return mask

            # -----------------------------------------------------------------
            # EMA CONDITIONS
            # -----------------------------------------------------------------
            # SMALL EMA (200'e kadar)
            small_periods = [9, 20, 50, 100, 200]
            small_bull = check_chain(small_periods, 0.00001/100.0, bullish=True)
            small_bear = check_chain(small_periods, 0.0001/100.0, bullish=False)
            
            # BIG EMA (200'den büyük)
            big_periods = [300, 500, 1000, 2000, 5000]
            big_bull = check_chain(big_periods, 0.00001/100.0, bullish=True)
            big_bear = check_chain(big_periods, 0.0001/100.0, bullish=False)
            
            # ALL EMA (hepsi - self.periods'dan)
            target_periods = self.periods
            all_bull = check_chain(target_periods, 0.00001/100.0, bullish=True)
            all_bear = check_chain(target_periods, 0.0001/100.0, bullish=False)
            
            # -----------------------------------------------------------------
            # ADIM 4: PUMP TESPİTİ
            # -----------------------------------------------------------------
            # Pump = (Kapanış - Açılış) / Açılış
            # Pozitif = Yeşil mum (yukarı pump)
            # Negatif = Kırmızı mum (aşağı dump)
            pump_pct = (closes - opens) / opens
            is_pump_up = pump_pct > self.pump_threshold     # Yeşil pump
            is_pump_down = pump_pct < -self.pump_threshold  # Kırmızı dump
            
            # -----------------------------------------------------------------
            # ADIM 5: MARUBOZU TESPİTİ
            # -----------------------------------------------------------------
            # Marubozu = Fitilsiz veya çok az fitilli mum
            # Formül: Gövde / Toplam Uzunluk >= Eşik
            #
            # Örnek: %80 eşik = Gövde, toplam mumun %80'i kadar olmalı
            # Bu güçlü bir momentum göstergesi
            
            body_size = (closes - opens).abs()  # Gövde büyüklüğü (mutlak değer)
            total_range = highs - lows          # Toplam mum uzunluğu (high-low)
            
            # Sıfıra bölme hatası önleme
            marubozu_ratio = pd.Series(0.0, index=df.index)
            valid_range = total_range > 0
            marubozu_ratio[valid_range] = body_size[valid_range] / total_range[valid_range]
            
            # Eşik kontrolü
            is_marubozu = marubozu_ratio >= self.marubozu_threshold
            
            # -----------------------------------------------------------------
            # ADIM 6: SİNYALLERİ BİRLEŞTİR
            # -----------------------------------------------------------------
            # Pump + Marubozu + EMA (BAĞIMSIZ)
            
            # EMA filtresi: Kullanıcı belirlediği duruma göre
            if self.ema == "all_bull":
                # ALL Bullish EMA: 9 > 20 > ... > 5000
                ema_filter = all_bull
            elif self.ema == "all_bear":
                # ALL Bearish EMA: 9 < 20 < ... < 5000
                ema_filter = all_bear
            elif self.ema == "small_bull":
                # SMALL Bullish EMA: 9 > 20 > 50 > 100 > 200
                ema_filter = small_bull
            elif self.ema == "small_bear":
                # SMALL Bearish EMA: 9 < 20 < 50 < 100 < 200
                ema_filter = small_bear
            elif self.ema == "big_bull":
                # BIG Bullish EMA: 300 > 500 > 1000 > 2000 > 5000
                ema_filter = big_bull
            elif self.ema == "big_bear":
                # BIG Bearish EMA: 300 < 500 < 1000 < 2000 < 5000
                ema_filter = big_bear
            else:  # "none"
                # EMA yok, tüm satırlar geçer
                ema_filter = pd.Series(True, index=df.index)
            
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
