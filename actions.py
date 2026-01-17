"""
ACTION MODULE
Görevi: Conditions (Koşullar) dosyasından gelen sinyalleri değerlendirip,
işleme girip girmemeye (veya çıkmaya) karar verir.

Burada matematiksel indikatör hesabı (EMA, RSI, Marubozu vb) yapılmaz.
Sadece MANTIK (Logic) ve KARAR (Decision) vardır.
"""

def evaluate_action(conditions_instance, candle_data):
    """
    conditions_instance: O anki durumu barındıran sınıf. multiple conditions birleşimi.
    candle_data: {close, open, high, low, timestamp}
    """
    
    # 1. Gerekli Verileri Al
    state = conditions_instance.conditions 
    # Beklenen state anahtarları: 
    # - 'all_bear' (EMA'dan gelir)
    # - 'is_marubozu' (Marubozu'dan gelir)
    # - 'is_pump' (Marubozu'dan gelir)
    
    close = candle_data['close']
    
    # --- STRATEJİ MANTIĞI BURADA ---
    # SENARYO: "Pump Short in Bear Trend"
    # Eğer:
    # 1. Piyasa Güçlü Ayı ise (all_bear)
    # 2. Anlık mum Pump yaptıysa (is_pump)
    # 3. Mum gövdesi Marubozu ise (is_marubozu)
    # -> SHORT (Trend Devamı)
    
    # Not: is_pump zaten %2 kontrolünü içeriyor (MarubozuConditions içinde)
    
    if state.get('all_bear') and state.get('is_pump') and state.get('is_marubozu'):
        # TP/SL instancedan al
        tp_rate = getattr(conditions_instance, 'tp', 0.02)
        sl_rate = getattr(conditions_instance, 'sl', 0.02)
        
        return {
            'action': 'SHORT',
            'entry_price': close,
            'tp': close * (1 - tp_rate),  # SHORT: TP aşağıda
            'sl': close * (1 + sl_rate),  # SHORT: SL yukarıda
            'pump_percent': state.get('pump_percentage', 0.0)
        }
        
    return None
