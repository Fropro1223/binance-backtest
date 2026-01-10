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
    pump_pct = (close - open_price) / open_price if open_price != 0 else 0
    
    # --- STRATEJİ MANTIĞI BURADA ---
    # SENARYO: "Pump Short in Bull Trend"
    # Eğer piyasa Boğa ise (state['all_bull']) VE anlık sert yükseliş varsa (%2) -> SHORT
    
    if state.get('all_bull') and pump_pct > 0.02:
        return {
            'action': 'SHORT',
            'entry_price': close,
            'tp': close * (1 - 0.04), # %4 Kar Al
            'sl': close * (1 + 0.04), # %4 Zarar Durdur
            'pump_percent': pump_pct
        }
        
    return None
