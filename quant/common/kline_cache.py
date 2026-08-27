def norm_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    return s if s.endswith("USDT") else s + "USDT"
