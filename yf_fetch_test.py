"""Quick script to inspect yfinance output for a single day."""

from datetime import date

import pandas as pd
import yfinance as yf


def main() -> None:
    ticker = "DHL.DE"
    target_date = date(2025, 4, 1)
    start = target_date
    end = date(2025, 4, 2)

    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False, threads=False)
    if df.empty:
        print(f"No data returned for {ticker} on {target_date}.")
        return

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 160)

    print(df)


if __name__ == "__main__":
    main()
