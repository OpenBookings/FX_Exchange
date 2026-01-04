import logging
from typing import Optional

# Set up logging
logger = logging.getLogger(__name__)

def get_exchange_rate(currency_code: str, conn) -> Optional[float]:
    """Get the latest exchange rate for a currency from the database."""
    currency_code = currency_code.upper()
    
    if currency_code == "EUR":
        logger.debug("EUR is base currency, returning 1.0")
        return 1.0
    
    logger.debug(f"Fetching exchange rate for {currency_code}")
    sql_select = """
    SELECT exchange_rate FROM exchange_rates
    WHERE currency_code = %s
    ORDER BY date DESC
    LIMIT 1
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql_select, (currency_code,))
        result = cursor.fetchone()
        if result is None:
            logger.warning(f"Exchange rate not found for currency: {currency_code}")
            return None
        rate = float(result[0])
        logger.debug(f"Found exchange rate for {currency_code}: {rate}")
        return rate
    except Exception as e:
        logger.error(f"Error fetching exchange rate for {currency_code}: {e}", exc_info=True)
        raise
    finally:
        cursor.close()

def convert_amount(amount: float, from_ccy: str, to_ccy: str, conn) -> float:
    """Convert amount from one currency to another using EUR as base."""
    from_ccy = from_ccy.upper()
    to_ccy = to_ccy.upper()

    logger.debug(f"Converting {amount} from {from_ccy} to {to_ccy}")

    if amount < 0:
        logger.warning(f"Invalid amount: {amount} (must be >= 0)")
        raise ValueError("amount must be >= 0")

    if from_ccy == to_ccy:
        logger.debug("Same currency, no conversion needed")
        return amount

    # Get rates - only fetch what we need
    logger.debug(f"Fetching exchange rates for {from_ccy} and {to_ccy}")
    r_from = get_exchange_rate(from_ccy, conn)
    r_to = get_exchange_rate(to_ccy, conn)
    
    if r_from is None:
        logger.error(f"Exchange rate not found for source currency: {from_ccy}")
        raise KeyError(f"Exchange rate not found for {from_ccy}")
    if r_to is None:
        logger.error(f"Exchange rate not found for target currency: {to_ccy}")
        raise KeyError(f"Exchange rate not found for {to_ccy}")

    # X -> Y using EUR base:
    # amount_in_eur = amount / r_from  (unless from is EUR)
    # result = amount_in_eur * r_to
    # Combined: amount * (r_to / r_from)
    result = amount * (r_to / r_from)
    logger.info(f"Conversion: {amount} {from_ccy} = {result} {to_ccy} (rates: {from_ccy}={r_from}, {to_ccy}={r_to})")
    return result