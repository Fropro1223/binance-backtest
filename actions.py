"""
ACTION MODULE
Görevi: Conditions (Koşullar) dosyasından gelen sinyalleri değerlendirip,
işleme girip girmemeye (veya çıkmaya) karar verir.

Burada matematiksel indikatör hesabı (EMA, RSI vb) yapılmaz.
Sadece MANTIK (Logic) ve KARAR (Decision) vardır.
"""

def evaluate_action(conditions_instance, candle_data):
    """
    conditions_instance: O anki durumu barındıran sınıf (örn: EmaChainConditions - conditions dict'i dolu)
    candle_data: {close, open, high, low, timestamp}
    """
    
    # 1. Gerekli Verileri Al
    state = conditions_instance.conditions # {'all_bull': True/False, ...}
    close = candle_data['close']
    open_price = candle_data['open']
    
    # 2. Yardımcı Hesaplamalar (Sadece karar için gereken basit şeyler)
    if open_price == 0: return None
    
    pump_pct = (close - open_price) / open_price
    
    # Marubozu Hesabı
    high = candle_data['high']
    low = candle_data['low']
    total_range = high - low
    body_size = abs(close - open_price)
    
    is_marubozu = False
    if total_range > 0:
        marubozu_ratio = body_size / total_range
        if marubozu_ratio >= 0.80:
            is_marubozu = True

    # --- STRATEJİ MANTIĞI BURADA ---
    # SENARYO: "Pump Short in Bull Trend"
    # Eğer:
    # 1. Piyasa Güçlü Boğa ise (all_bull)
    # 2. Anlık mum %2'den fazla Pump yaptıysa
    # 3. Mum gövdesi %80'den doluysa (Marubozu - yani güçlü momentum/exhaustion)
    # -> SHORT (Tepki Satışı)
    
    if state.get('all_bull') and pump_pct > 0.02 and is_marubozu:
        # TP/SL instancedan al
        tp_rate = getattr(conditions_instance, 'tp', 0.04)
        sl_rate = getattr(conditions_instance, 'sl', 0.04)
        
        return {
            'action': 'SHORT',
            'entry_price': close,
            'tp': close * (1 - tp_rate), 
            'sl': close * (1 + sl_rate),
            'pump_percent': pump_pct
        }
        
    return None
