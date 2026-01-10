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
    # - 'all_bull' (EMA'dan gelir)
    # - 'is_marubozu' (Marubozu'dan gelir)
    # - 'is_pump' (Marubozu'dan gelir)
    
    close = candle_data['close']
    
    # --- STRATEJİ MANTIĞI BURADA ---
    # SENARYO: "Pump Short in Bull Trend"
    # Eğer:
    # 1. Piyasa Güçlü Boğa ise (all_bull)
    # 2. Anlık mum Pump yaptıysa (is_pump)
    # 3. Mum gövdesi Marubozu ise (is_marubozu)
    # -> SHORT (Tepki Satışı)
    
    # Not: is_pump zaten %2 kontrolünü içeriyor (MarubozuConditions içinde)
    
    if state.get('all_bull') and state.get('is_pump') and state.get('is_marubozu'):
        # TP/SL instancedan al
        tp_rate = getattr(conditions_instance, 'tp', 0.04)
        sl_rate = getattr(conditions_instance, 'sl', 0.04)
        
        return {
            'action': 'SHORT',
            'entry_price': close,
            'tp': close * (1 - tp_rate), 
            'sl': close * (1 + sl_rate),
            'pump_percent': state.get('pump_percentage', 0.0)
        }
        
    return None
