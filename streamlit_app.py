# streamlit_app.py
import io
from datetime import date, timedelta

import json

import asyncio

import pandas as pd
import streamlit as st
import yfinance as yf
import aiohttp
import aiomoex

# ---------- Page config ----------
st.set_page_config(
    page_title="Ticker Workbook Builder",
    page_icon="📈",
    layout="wide"
)

# ---------- Helpers ----------
REQUIRED_COLUMNS = ["No", "Name", "Ticker", "IsRussian"]

TAB_LABELS = ["Build workbook", "Search ticker"]
TAB_TO_SLUG = {
    "Build workbook": "build",
    "Search ticker": "search",
}
SLUG_TO_TAB = {slug: label for label, slug in TAB_TO_SLUG.items()}
DEFAULT_TAB_SLUG = TAB_TO_SLUG["Build workbook"]


def _first_value(value):
    if isinstance(value, (list, tuple)):
        return value[0] if value else ""
    if value is None:
        return ""
    return str(value)


def _flatten_col_label(label) -> str:
    if isinstance(label, tuple):
        for part in label:
            if part is None:
                continue
            part_str = str(part).strip()
            if part_str:
                return part_str
        return ""
    return str(label).strip()


def _normalize_header(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def _run_async(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def get_query_params() -> dict:
    if hasattr(st, "query_params"):
        return dict(st.query_params)
    return st.experimental_get_query_params()


def set_query_params(**kwargs):
    managed_keys = {"tab"}
    filtered = {k: v for k, v in kwargs.items() if v}
    if hasattr(st, "query_params"):
        qp = st.query_params
        for key in managed_keys:
            if key in qp and key not in filtered:
                del qp[key]
        for key, value in filtered.items():
            if value:
                qp[key] = value
    else:
        st.experimental_set_query_params(**filtered)

def normalize_companies_df(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize the uploaded companies table."""
    # Normalize column names (strip spaces, case-insensitive match)
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [_flatten_col_label(col) for col in df.columns]
    df.columns = [str(c).strip() for c in df.columns]

    # Try to find required columns with normalized comparison
    colmap = {}
    for need in REQUIRED_COLUMNS:
        candidates = [c for c in df.columns if _normalize_header(c) == _normalize_header(need)]
        if not candidates:
            raise ValueError(f"Missing required column: '{need}'")
        colmap[need] = candidates[0]

    # Reorder and return
    df = df[[colmap["No"], colmap["Name"], colmap["Ticker"], colmap["IsRussian"]]]
    df = df.rename(columns={
        colmap["No"]: "No",
        colmap["Name"]: "Name",
        colmap["Ticker"]: "Ticker",
        colmap["IsRussian"]: "IsRussian",
    })

    df["No"] = df["No"].astype(str).str.strip()
    df["Name"] = df["Name"].astype(str).str.strip()
    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    df["IsRussian"] = (
        df["IsRussian"]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"1", "y", "yes", "true", "да", "ru", "russia"})
    )
    return df

def fetch_history_for_ticker(ticker: str, start_d: date, end_d: date) -> pd.DataFrame:
    """
    Fetch OHLCV using yfinance.
    Note: yfinance's 'end' is exclusive; add one day to make the range inclusive.
    """
    # Ensure inclusive end date
    end_exclusive = end_d + timedelta(days=1)
    try:
        df = yf.download(
            ticker,
            start=start_d,
            end=end_exclusive,
            progress=False,
            auto_adjust=False,  # keep raw OHLCV; you may switch to True if desired
            threads=False
        )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    if st.session_state.get("show_raw_yf"):
        st.write(f"Raw yfinance output for {ticker}")
        st.write("Columns:", list(df.columns))
        st.dataframe(df.head())

    df = df.reset_index()  # bring Date from index to column
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [_flatten_col_label(col) for col in df.columns]

    def _norm_key(name: str) -> str:
        return "".join(ch for ch in str(name).lower() if ch.isalnum())

    candidates = [
        ("open", "open"),
        ("regularmarketopen", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("adjclose", "close"),
        ("regularmarketclose", "close"),
        ("volume", "volume"),
        ("regularmarketvolume", "volume"),
    ]

    colmap = {"open": None, "high": None, "low": None, "close": None, "volume": None}
    for col in df.columns:
        key = _norm_key(col)
        for cand_key, target in candidates:
            if colmap[target] is None and key == cand_key:
                colmap[target] = col
                break

    try:
        date_col = next(c for c in df.columns if _norm_key(c) in {"date", "datetime"})
    except StopIteration:
        date_col = df.columns[0]

    if any(value is None for value in colmap.values()):
        return pd.DataFrame()

    ordered_cols = [colmap[label] for label in ["open", "high", "low", "close", "volume"]]
    df = df[[date_col, *ordered_cols]]
    df = df.rename(columns={
        date_col: "Date",
        colmap["open"]: "Open",
        colmap["high"]: "High",
        colmap["low"]: "Low",
        colmap["close"]: "Close",
        colmap["volume"]: "Volume",
    })
    # Rename to requested (English equivalents of Russian labels):
    # Дата -> Date, Цена -> Price (using Close), Откр. -> Open, Макс. -> High, Мин. -> Low, Объём -> Volume
    df = df.rename(columns={"Close": "Price"})
    # Ensure dtypes are clean
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df[["Date", "Price", "Open", "High", "Low", "Volume"]]


def fetch_history_for_russian_ticker(ticker: str, start_d: date, end_d: date) -> pd.DataFrame:
    async def _fetch():
        async with aiohttp.ClientSession() as session:
            return await aiomoex.get_board_history(
                session,
                ticker,
                start=start_d.isoformat(),
                end=end_d.isoformat(),
            )

    try:
        data = _run_async(_fetch())
    except Exception:
        data = []

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if st.session_state.get("show_raw_yf"):
        st.write(f"Raw MOEX output for {ticker}")
        st.write("Columns:", list(df.columns))
        st.dataframe(df.head())

    normalized_cols = { _normalize_header(col): col for col in df.columns }

    date_col = normalized_cols.get("tradedate") or normalized_cols.get("date")
    price_col = normalized_cols.get("close")
    volume_col = normalized_cols.get("volume") or normalized_cols.get("vol")

    if not all([date_col, price_col, volume_col]):
        return pd.DataFrame()

    result = pd.DataFrame({
        "Date": pd.to_datetime(df[date_col]).dt.date,
        "Price": pd.to_numeric(df[price_col], errors="coerce"),
        "Volume": pd.to_numeric(df[volume_col], errors="coerce"),
    })

    # MOEX endpoint does not expose open/high/low in this mode; duplicate Price for consistency
    result["Open"] = result["Price"]
    result["High"] = result["Price"]
    result["Low"] = result["Price"]

    return result[["Date", "Price", "Open", "High", "Low", "Volume"]]

def autosize_columns(writer, sheet_name: str, df: pd.DataFrame, start_row=0, start_col=0, extra_pad=2):
    """Autosize Excel columns based on dataframe content lengths."""
    worksheet = writer.sheets[sheet_name]
    for i, col in enumerate(df.columns):
        # max length between header and values
        max_len = len(str(col))
        series_as_str = df[col].astype(str)
        if not series_as_str.empty:
            max_len = max(max_len, series_as_str.map(len).max())
        worksheet.set_column(start_col + i, start_col + i, max_len + extra_pad)

def write_company_sheet(writer, sheet_name: str, df_prices: pd.DataFrame):
    """
    Write a per-company sheet:
    - Data table with columns: Date, Price, Open, High, Low, Volume
    - On the right, place a VWAP (weighted mean price) computed via SUMPRODUCT.
    """
    df_prices = df_prices.copy()
    if isinstance(df_prices.columns, pd.MultiIndex):
        df_prices.columns = [_flatten_col_label(col) for col in df_prices.columns]
    df_prices.index += 1  # not necessary, but keeps mental mapping simple

    df_prices.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book

    # Formats
    money_fmt = workbook.add_format({"num_format": "#,##0.00"})
    int_fmt = workbook.add_format({"num_format": "#,##0"})
    date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd"})
    bold_fmt = workbook.add_format({"bold": True})

    nrows = len(df_prices)
    # Apply basic column formats
    # A: Date, B: Price, C: Open, D: High, E: Low, F: Volume
    worksheet.set_column("A:A", 12, date_fmt)
    worksheet.set_column("B:E", 12, money_fmt)
    worksheet.set_column("F:F", 12, int_fmt)

    # Autosize to content as well
    autosize_columns(writer, sheet_name, df_prices, start_row=0, start_col=0, extra_pad=2)

    # Place VWAP (Close*Volume / Volume) using SUMPRODUCT to the right of the table
    # We'll use columns G (label) and H (value)
    label_cell = "G2"
    value_cell = "H2"
    worksheet.write(label_cell, "VWAP (Price × Volume)", bold_fmt)

    # Data cells (Excel is 1-based; our header is on row 1)
    # Prices: B2:B{n+1}, Volume: F2:F{n+1}
    start_row = 2
    end_row = nrows + 1
    price_range = f"B{start_row}:B{end_row}"
    vol_range = f"F{start_row}:F{end_row}"
    vwap_formula = f"=IFERROR(SUMPRODUCT({price_range},{vol_range})/SUM({vol_range}), \"\")"
    worksheet.write_formula(value_cell, vwap_formula, money_fmt)

def build_workbook_bytes(companies_df: pd.DataFrame, start_d: date, end_d: date) -> bytes:
    """Build the Excel workbook in-memory and return as bytes."""
    output = io.BytesIO()
    total = len(companies_df)
    progress_bar = st.progress(0.0, text="Preparing to fetch data…")
    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as writer:
        # Sheet 1: companies (as provided)
        companies_df.to_excel(writer, sheet_name="companies", index=False)
        autosize_columns(writer, "companies", companies_df, start_row=0, start_col=0, extra_pad=2)

        # Per company sheets
        for idx, row in enumerate(companies_df.itertuples(index=False), start=1):
            sheet_name = str(getattr(row, "No"))[:31]  # Excel sheet name limit
            ticker = str(getattr(row, "Ticker")).strip()
            company_name = str(getattr(row, "Name"))
            is_russian = bool(getattr(row, "IsRussian"))

            if total:
                progress_value = (idx - 1) / total
            else:
                progress_value = 0.0

            progress_text = f"Fetching {company_name} ({ticker}) — {idx}/{total}"
            progress_bar.progress(progress_value, text=progress_text)

            if is_russian:
                prices_df = fetch_history_for_russian_ticker(ticker, start_d, end_d)
            else:
                prices_df = fetch_history_for_ticker(ticker, start_d, end_d)
            # If empty, still create a sheet with headers and a note
            if prices_df.empty:
                empty_df = pd.DataFrame(columns=["Date", "Price", "Open", "High", "Low", "Volume"])
                empty_df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                worksheet.write("H2", "No data for the selected range/ticker.")
                autosize_columns(writer, sheet_name, empty_df, start_row=0, start_col=0, extra_pad=2)
            else:
                write_company_sheet(writer, sheet_name, prices_df)

        progress_bar.progress(1.0 if total else 1.0, text="All tickers fetched. Building workbook…")

    return output.getvalue()

# ---------- UI ----------
params = get_query_params()
tab_slug_from_url = _first_value(params.get("tab"))
if tab_slug_from_url not in SLUG_TO_TAB:
    tab_slug_from_url = DEFAULT_TAB_SLUG

active_tab_slug = tab_slug_from_url

st.title("Ticker Workbook Builder")

tab_build, tab_search = st.tabs(TAB_LABELS)

tab_mapping_js = json.dumps(TAB_TO_SLUG)
desired_slug_js = json.dumps(active_tab_slug)

st.markdown(
    f"""
    <script>
    (function syncTabs() {{
        const TAB_MAPPING = {tab_mapping_js};
        const desiredSlug = {desired_slug_js};
        const root = window.parent && window.parent !== window ? window.parent : window;

        function getTabs() {{
            return Array.from(root.document.querySelectorAll('button[data-baseweb="tab"]'));
        }}

        function bindTabClicks(tabs) {{
            tabs.forEach((tab) => {{
                if (tab.dataset.tabSyncBound === 'true') {{
                    return;
                }}
                tab.addEventListener('click', () => {{
                    const label = tab.innerText.trim();
                    const slug = TAB_MAPPING[label];
                    if (!slug) {{
                        return;
                    }}
                    const url = new URL(root.location.href);
                    url.searchParams.set('tab', slug);
                    const query = url.searchParams.toString();
                    const newUrl = query ? `${{url.pathname}}?${{query}}` : url.pathname;
                    root.history.replaceState(null, '', newUrl);
                }});
                tab.dataset.tabSyncBound = 'true';
            }});
        }}

        function activateDesiredTab(tabs) {{
            if (!desiredSlug) {{
                return;
            }}
            const entry = Object.entries(TAB_MAPPING).find(([, slug]) => slug === desiredSlug);
            if (!entry) {{
                return;
            }}
            const [label] = entry;
            const target = tabs.find((tab) => tab.innerText.trim() === label);
            if (target && target.getAttribute('aria-selected') !== 'true') {{
                target.click();
            }}
        }}

        function init() {{
            const tabs = getTabs();
            if (!tabs.length) {{
                window.setTimeout(init, 50);
                return;
            }}
            bindTabClicks(tabs);
            activateDesiredTab(tabs);
        }}

        init();
    }})();
    </script>
    """,
    unsafe_allow_html=True
)

with tab_build:
    st.subheader("1) Upload companies list")
    uploaded = st.file_uploader(
        "Upload an Excel file (.xlsx) with columns: No, Name, Ticker, IsRussian",
        type=["xlsx"],
        accept_multiple_files=False
    )

    st.checkbox("Show raw yfinance responses while building", key="show_raw_yf")

    st.subheader("2) Select date range")
    today = date.today()
    default_start = today - timedelta(days=365)
    start_date = st.date_input("Start date", value=default_start)
    end_date = st.date_input("End date", value=today)

    if start_date > end_date:
        st.error("Start date must be on or before End date.")
    else:
        st.info("The selected end date is treated as inclusive.")

    st.subheader("3) Build and download")
    build_btn = st.button("Build Excel workbook", type="primary", use_container_width=True)

    if build_btn:
        if uploaded is None:
            st.error("Please upload an Excel file with the required columns.")
        else:
            try:
                raw_df = pd.read_excel(uploaded)
                companies = normalize_companies_df(raw_df)
            except Exception as e:
                st.error(f"Failed to read/validate the uploaded file: {e}")
            else:
                with st.spinner("Fetching data and generating workbook…"):
                    wb_bytes = build_workbook_bytes(companies, start_date, end_date)

                filename = f"companies_prices_{start_date}_to_{end_date}.xlsx"
                st.success("Workbook is ready.")
                st.download_button(
                    label="Download Excel workbook",
                    data=wb_bytes,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

                st.caption(
                    "Workbook structure: sheet 'companies' mirrors your input. "
                    "Each additional sheet is named by the company's 'No' and includes "
                    "columns [Date, Price, Open, High, Low, Volume] plus a VWAP "
                    "(weighted mean price) computed with Excel SUMPRODUCT."
                )
with tab_search:
    st.subheader("Search for tickers (via yfinance.Search)")

    query = st.text_input("Enter a company name or ticker keyword:", "")
    search_btn = st.button("Search", use_container_width=True)

    if search_btn and query.strip():
        try:
            search = yf.Search(query.strip(), max_results=10)
            quotes = search.quotes  # list of dicts
            if not quotes:
                st.warning("No results found.")
            else:
                results = []
                for q in quotes:
                    symbol = q.get("symbol")
                    results.append({
                        "Symbol": symbol,
                        "Name": q.get("shortname"),
                        "Type": q.get("quoteType"),
                        "Exchange": q.get("exchange"),
                        "Currency": q.get("currency"),
                        "Link": f"https://finance.yahoo.com/quote/{symbol}" if symbol else ""
                    })
                df_results = pd.DataFrame(results)
                st.write("### Results")
                st.write(df_results.to_html(escape=False, render_links=True), unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Search failed: {e}")

set_query_params(tab=active_tab_slug)

# Footer note
st.caption("Tip: Ensure your Excel headers include 'No', 'Name', 'Ticker', 'IsRussian'. "
           "Set 'IsRussian' to yes/true/1 for MOEX tickers; others are fetched via Yahoo Finance.")
