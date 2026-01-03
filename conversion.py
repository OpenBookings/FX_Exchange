from typing import Optional

def get_exchange_rate(currency_code: str, conn) -> Optional[float]:
    """Get the latest exchange rate for a currency from the database."""
    if currency_code.upper() == "EUR":
        return 1.0
    
    sql_select = """
    SELECT exchange_rate FROM exchange_rates
    WHERE currency_code = %s
    ORDER BY date DESC
    LIMIT 1
    """
    
    with conn.cursor() as cursor:
        cursor.execute(sql_select, (currency_code.upper(),))
        result = cursor.fetchone()
        if result is None:
            return None
        return float(result[0])

def convert_amount(amount: float, from_ccy: str, to_ccy: str, conn) -> float:
    """Convert amount from one currency to another using EUR as base."""
    from_ccy = from_ccy.upper()
    to_ccy = to_ccy.upper()

    if amount < 0:
        raise ValueError("amount must be >= 0")

    if from_ccy == to_ccy:
        return amount

    # Get rates - only fetch what we need
    r_from = get_exchange_rate(from_ccy, conn)
    r_to = get_exchange_rate(to_ccy, conn)
    
    if r_from is None:
        raise KeyError(f"Exchange rate not found for {from_ccy}")
    if r_to is None:
        raise KeyError(f"Exchange rate not found for {to_ccy}")

    # X -> Y using EUR base:
    # amount_in_eur = amount / r_from  (unless from is EUR)
    # result = amount_in_eur * r_to
    # Combined: amount * (r_to / r_from)
    return amount * (r_to / r_from)