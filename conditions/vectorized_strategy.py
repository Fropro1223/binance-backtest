"""
Vektörel EMA + Pump + Marubozu Stratejisi
==========================================
Bu sınıf, piyasa verilerini vektörel (toplu) şekilde işleyerek hızlı backtest yapılmasını sağlar.
Giriş sinyalleri (Pump/Dump), Marubozu mum yapısı ve opsiyonel EMA filtresi ile belirlenir.
Sinyal yönü (LONG/SHORT) ve tetikleyici (PUMP/DUMP) birbirinden tamamen bağımsızdır.
"""

from backtest_framework import Strategy
import pandas as pd
import numpy as np
import polars as pl

# =============================================================================
# IMPORTANT DEVELOPER NOTE (USER DIRECTIVE):
# =============================================================================
# SIDES (LONG/SHORT) ARE COMPLETELY INDEPENDENT OF TRIGGER CONDITIONS (PUMP/DUMP).
# Any combination can be tested:
# - LONG after PUMP (Trend Following)
# - LONG after DUMP (Mean Reversion)
# - SHORT after PUMP (Mean Reversion)
# - SHORT after DUMP (Trend Following)
#
# DO NOT hardcode side-condition dependencies. The user decides via CLI.
# =============================================================================


    """
    Vektörel Strateji Sınıfı
    ------------------------
    Üç ana filtreyi (EMA, Pump/Dump, Marubozu) kullanarak giriş sinyali üretir.
    Veriyi mum mum dönmek yerine tüm sütun üzerinde toplu işlem (Pandas/Polars) yaparak
    performansı maksimize eder.
    """
    
    # =VELÜM 1: BAŞLATMA (INITIALIZATION)
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
            
            # DECOUPLED TRIGGER LOGIC:
            # Side (Long/Short) is handled by the framework (tp/sl logic).
            # This function only decides WHERE to enter based on 'cond'.
            
            if self.cond == "dump":
                # Trigger on red marubozu drop
                final_signal = is_pump_down & is_marubozu & ema_filter
            else:
                # Trigger on green marubozu rise (default: pump)
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
