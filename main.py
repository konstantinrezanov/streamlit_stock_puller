from fastapi import FastAPI, Query, HTTPException, Header, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
import requests
from typing import Optional, List

app = FastAPI(title="Stock Data API", version="2.1.0")


# ---------- Models ----------
class StockEntry(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class StockResponseSingle(BaseModel):
    ticker: str
    query_date: str
    data: StockEntry


class StockResponseAll(BaseModel):
    ticker: str
    data: List[StockEntry]


class TickerInfo(BaseModel):
    symbol: str
    name: str
    type: str
    region: str
    marketOpen: str
    marketClose: str
    timezone: str
    currency: str
    matchScore: str
    link: str


class TickerSearchResponse(BaseModel):
    best_matches: List[TickerInfo]


# ---------- User Interface ----------
@app.get("/", response_class=HTMLResponse)
def home():
    """Simple HTML form for user input."""
    return """
    <html>
        <head>
            <title>Stock Data Search</title>
        </head>
        <body>
            <h2>Search Stock Ticker</h2>
            <form action="/go_search" method="get">
                <label for="api_key">API Key:</label><br>
                <input type="text" id="api_key" name="api_key" required><br><br>
                
                <label for="keywords">Search Query (Company or Symbol):</label><br>
                <input type="text" id="keywords" name="keywords" required><br><br>
                
                <input type="submit" value="Search">
            </form>
        </body>
    </html>
    """


@app.get("/go_search")
def go_search(api_key: str, keywords: str):
    """Redirect form submission to search_ticker endpoint."""
    return RedirectResponse(url=f"/search_ticker?keywords={keywords}&api_key={api_key}")


# ---------- Stock Endpoint ----------
@app.get("/stock", response_model=None)
def get_stock(
    ticker: str = Query(..., description="Stock ticker symbol, e.g. AAPL"),
    query_date: Optional[str] = Query(
        None, description="Date in YYYY-MM-DD format. If not provided, full series is returned."
    ),
    api_key: Optional[str] = Query(None, description="Alpha Vantage API key (can also be provided in 'X-API-KEY' header)."),
    api_key_header: Optional[str] = Header(None, alias="X-API-KEY"),
):
    """Fetch stock data for a ticker: single day or full history."""
    key = api_key_header or api_key
    if not key:
        raise HTTPException(status_code=400, detail="API key required (use ?api_key=... or X-API-KEY header).")

    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={ticker.upper()}"
        f"&apikey={key}"
        f"&outputsize=compact"
    )

    r = requests.get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch data from Alpha Vantage")

    data = r.json()
    time_series = data.get("Time Series (Daily)")
    if not time_series:
        raise HTTPException(status_code=404, detail=f"No data available for {ticker}")

    if query_date:
        if query_date not in time_series:
            raise HTTPException(status_code=404, detail=f"No data for {ticker} on {query_date}")

        day_data = time_series[query_date]
        entry = StockEntry(
            date=query_date,
            open=float(day_data["1. open"]),
            high=float(day_data["2. high"]),
            low=float(day_data["3. low"]),
            close=float(day_data["4. close"]),
            volume=int(day_data.get("5. volume")),
        )
        return StockResponseSingle(ticker=ticker.upper(), query_date=query_date, data=entry)

    # Full history
    entries = [
        StockEntry(
            date=dt,
            open=float(values["1. open"]),
            high=float(values["2. high"]),
            low=float(values["3. low"]),
            close=float(values["4. close"]),
            volume=int(values.get("5. volume")),
        )
        for dt, values in sorted(time_series.items(), reverse=True)
    ]

    return StockResponseAll(ticker=ticker.upper(), data=entries)


# ---------- Symbol Search ----------
@app.get("/search_ticker", response_model=TickerSearchResponse)
def search_ticker(
    request: Request,
    keywords: str = Query(..., description="Company name or ticker fragment, e.g. 'Tesla' or 'AAPL'"),
    api_key: Optional[str] = Query(None, description="Alpha Vantage API key"),
    api_key_header: Optional[str] = Header(None, alias="X-API-KEY"),
):
    """Search for available tickers. Returns full Alpha Vantage match info + link to /stock."""
    key = api_key_header or api_key
    if not key:
        raise HTTPException(status_code=400, detail="API key required (use ?api_key=... or X-API-KEY header).")

    url = (
        f"https://www.alphavantage.co/query"
        f"?function=SYMBOL_SEARCH"
        f"&keywords={keywords}"
        f"&apikey={key}"
    )

    r = requests.get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch ticker data from Alpha Vantage")

    data = r.json()
    matches = data.get("bestMatches", [])
    if not matches:
        raise HTTPException(status_code=404, detail=f"No matches found for '{keywords}'")

    base_url = str(request.base_url).rstrip("/")

    result = [
        TickerInfo(
            symbol=m.get("1. symbol", ""),
            name=m.get("2. name", ""),
            type=m.get("3. type", ""),
            region=m.get("4. region", ""),
            marketOpen=m.get("5. marketOpen", ""),
            marketClose=m.get("6. marketClose", ""),
            timezone=m.get("7. timezone", ""),
            currency=m.get("8. currency", ""),
            matchScore=m.get("9. matchScore", ""),
            link=f"{base_url}/stock?ticker={m.get('1. symbol', '')}&api_key={key}"
        )
        for m in matches
    ]

    return TickerSearchResponse(best_matches=result)