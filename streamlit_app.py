# streamlit_app.py
import io
import calendar
from datetime import date, timedelta

import json

import asyncio

import pandas as pd
import streamlit as st
import yfinance as yf
import aiohttp
import aiomoex
from xlsxwriter.utility import xl_col_to_name

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


def quarter_date_range(year: int, quarter: int) -> tuple[date, date]:
    if quarter not in (1, 2, 3, 4):
        raise ValueError("Quarter must be between 1 and 4")
    start_month = (quarter - 1) * 3 + 1
    start = date(year, start_month, 1)
    end_month = start_month + 2
    last_day = calendar.monthrange(year, end_month)[1]
    end = date(year, end_month, last_day)
    return start, end


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

    base_columns = ["Date", "Price", "Open", "High", "Low", "Volume"]
    has_period = "Period" in df_prices.columns
    if has_period:
        columns_order = ["Period", *base_columns]
    else:
        columns_order = base_columns

    for col in columns_order:
        if col not in df_prices.columns:
            df_prices[col] = pd.NA

    df_prices = df_prices[columns_order]

    for numeric_col in ["Price", "Open", "High", "Low", "Volume"]:
        if numeric_col in df_prices.columns:
            df_prices[numeric_col] = pd.to_numeric(df_prices[numeric_col], errors="coerce")
    if "Date" in df_prices.columns:
        df_prices["Date"] = pd.to_datetime(df_prices["Date"]).dt.date

    df_prices.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book

    # Formats
    money_fmt = workbook.add_format({"num_format": "#,##0.00"})
    int_fmt = workbook.add_format({"num_format": "#,##0"})
    date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd"})
    bold_fmt = workbook.add_format({"bold": True})

    start_col = 0
    for idx, col_name in enumerate(columns_order):
        excel_col = start_col + idx
        if col_name == "Date":
            worksheet.set_column(excel_col, excel_col, 12, date_fmt)
        elif col_name in {"Price", "Open", "High", "Low"}:
            worksheet.set_column(excel_col, excel_col, 12, money_fmt)
        elif col_name == "Volume":
            worksheet.set_column(excel_col, excel_col, 12, int_fmt)
        else:
            worksheet.set_column(excel_col, excel_col, 14)

    # Autosize to content as well
    autosize_columns(writer, sheet_name, df_prices, start_row=0, start_col=0, extra_pad=2)

    # Build VWAP summary using Excel formulas
    summary_col = start_col + len(columns_order) + 1
    worksheet.write(0, summary_col, "VWAP period", bold_fmt)
    worksheet.write(0, summary_col + 1, "Value", bold_fmt)

    def period_rows(label: str) -> tuple[int, int] | None:
        if not has_period:
            return None
        matches = df_prices.index[df_prices["Period"] == label].tolist()
        if not matches:
            return None
        start_row = matches[0] + 2  # Excel is 1-based and header occupies row 1
        end_row = matches[-1] + 2
        return start_row, end_row

    current_summary_row = 1
    if has_period:
        unique_periods = list(dict.fromkeys(df_prices["Period"].dropna().tolist()))
        for period_label in unique_periods:
            rows = period_rows(period_label)
            if not rows:
                continue
            start_row, end_row = rows
            worksheet.write(current_summary_row, summary_col, str(period_label))
            price_col_letter = xl_col_to_name(columns_order.index("Price"))
            volume_col_letter = xl_col_to_name(columns_order.index("Volume"))
            formula = (
                f"=IFERROR(SUMPRODUCT({price_col_letter}{start_row}:{price_col_letter}{end_row},"
                f"{volume_col_letter}{start_row}:{volume_col_letter}{end_row})/"
                f"SUM({volume_col_letter}{start_row}:{volume_col_letter}{end_row}),\"\")"
            )
            worksheet.write_formula(current_summary_row, summary_col + 1, formula, money_fmt)
            current_summary_row += 1

    # Overall VWAP
    worksheet.write(current_summary_row, summary_col, "Overall")
    price_col_letter = xl_col_to_name(columns_order.index("Price"))
    volume_col_letter = xl_col_to_name(columns_order.index("Volume"))
    overall_start = 2
    overall_end = len(df_prices) + 1
    overall_formula = (
        f"=IFERROR(SUMPRODUCT({price_col_letter}{overall_start}:{price_col_letter}{overall_end},"
        f"{volume_col_letter}{overall_start}:{volume_col_letter}{overall_end})/"
        f"SUM({volume_col_letter}{overall_start}:{volume_col_letter}{overall_end}),\"\")"
    )
    worksheet.write_formula(current_summary_row, summary_col + 1, overall_formula, money_fmt)

def build_workbook_bytes(companies_df: pd.DataFrame, periods: list[dict]) -> bytes:
    """Build the Excel workbook in-memory and return as bytes."""
    output = io.BytesIO()
    total = len(companies_df)
    progress_bar = st.progress(0.0, text="Preparing to fetch data…")
    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as writer:
        # Sheet 1: companies (as provided)
        companies_df.to_excel(writer, sheet_name="companies", index=False)
        autosize_columns(writer, "companies", companies_df, start_row=0, start_col=0, extra_pad=2)
        companies_ws = writer.sheets["companies"]

        # Per company sheets
        per_company_periods = periods or []
        period_count = max(len(per_company_periods), 1)

        for idx, row in enumerate(companies_df.itertuples(index=False), start=1):
            sheet_name = str(getattr(row, "No"))[:31]  # Excel sheet name limit
            ticker = str(getattr(row, "Ticker")).strip()
            company_name = str(getattr(row, "Name"))
            is_russian = bool(getattr(row, "IsRussian"))

            collected_frames: list[pd.DataFrame] = []

            if per_company_periods:
                for period_idx, period in enumerate(per_company_periods, start=1):
                    if total:
                        progress_value = ((idx - 1) + (period_idx - 1) / period_count) / total
                    else:
                        progress_value = 0.0

                    progress_text = (
                        f"Fetching {company_name} ({ticker}) — {idx}/{total} · {period['label']}"
                    )
                    progress_bar.progress(progress_value, text=progress_text)

                    if is_russian:
                        period_df = fetch_history_for_russian_ticker(
                            ticker,
                            period["start"],
                            period["end"],
                        )
                    else:
                        period_df = fetch_history_for_ticker(
                            ticker,
                            period["start"],
                            period["end"],
                        )

                    if not period_df.empty:
                        period_df = period_df.copy()
                        period_df["Period"] = period["label"]
                        collected_frames.append(period_df)
            else:
                if total:
                    progress_value = (idx - 1) / total
                else:
                    progress_value = 0.0
                progress_bar.progress(progress_value, text=f"Fetching {company_name} ({ticker}) — {idx}/{total}")

            if collected_frames:
                prices_df = pd.concat(collected_frames, ignore_index=True)
                prices_df = prices_df.sort_values(["Period", "Date"]).reset_index(drop=True)
                write_company_sheet(writer, sheet_name, prices_df)
            else:
                empty_df = pd.DataFrame(
                    columns=["Period", "Date", "Price", "Open", "High", "Low", "Volume"]
                )
                empty_df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                worksheet.write("H2", "No data for the selected range/ticker.")
                autosize_columns(writer, sheet_name, empty_df, start_row=0, start_col=0, extra_pad=2)

            # Add hyperlink from companies sheet "No" column to the corresponding sheet
            link_cell = f"A{idx + 1}"
            display_text = str(getattr(row, "No"))
            sheet_ref = sheet_name.replace("'", "''")  # escape single quotes for Excel
            companies_ws.write_url(
                link_cell,
                f"internal:'{sheet_ref}'!A1",
                string=display_text
            )

        if total:
            progress_bar.progress(1.0, text="All tickers fetched. Building workbook…")
        else:
            progress_bar.progress(1.0, text="No companies supplied.")

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

    today = date.today()

    st.subheader("2) Select quarters")
    quarter_choices = []
    for year in range(today.year, today.year - 6, -1):
        for quarter in range(4, 0, -1):
            label = f"{year} Q{quarter}"
            quarter_choices.append((label, year, quarter))
    current_quarter = (today.month - 1) // 3 + 1
    if current_quarter == 1:
        default_year = today.year - 1
        default_quarter = 4
    else:
        default_year = today.year
        default_quarter = current_quarter - 1
    default_label = f"{default_year} Q{default_quarter}"
    quarter_labels = [label for label, _, _ in quarter_choices]
    default_selection = [default_label] if default_label in quarter_labels else quarter_labels[:1]
    selected_quarters = st.multiselect(
        "Choose one or more quarters to include",
        options=quarter_labels,
        default=default_selection
    )
    quarter_lookup = {label: (year, quarter) for label, year, quarter in quarter_choices}

    st.subheader("3) Optional date range")
    with st.expander("Add a custom date range", expanded=False):
        start_date = st.date_input("Start date", value=None, key="custom_date_start")
        end_date = st.date_input("End date", value=None, key="custom_date_end")
        include_custom_range = st.checkbox(
            "Include this date range",
            value=False,
            key="include_custom_range"
        )

        if include_custom_range:
            if not start_date or not end_date:
                st.warning("Provide both start and end dates to include the custom range.")
            elif start_date > end_date:
                st.warning("Start date must be on or before End date.")

    st.subheader("4) Build and download")
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
                periods: list[dict] = []
                for label in selected_quarters:
                    if label not in quarter_lookup:
                        continue
                    year, quarter = quarter_lookup[label]
                    try:
                        q_start, q_end = quarter_date_range(year, quarter)
                    except ValueError:
                        continue
                    periods.append({"label": label, "start": q_start, "end": q_end})

                if include_custom_range and start_date and end_date and start_date <= end_date:
                    periods.append({
                        "label": f"Custom {start_date} to {end_date}",
                        "start": start_date,
                        "end": end_date,
                    })

                if not periods:
                    st.error("Select at least one quarter or include the custom date range before building.")
                else:
                    with st.spinner("Fetching data and generating workbook…"):
                        wb_bytes = build_workbook_bytes(companies, periods)

                    filename = f"companies_prices_{date.today()}.xlsx"
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
                        "all requested periods in one table with per-period VWAP totals."
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
           "Set 'IsRussian' to yes/true/1 for MOEX tickers; others are fetched via Yahoo Finance. "
           "Select any quarters you need, keep the date range if desired, and the workbook will compute VWAP per period.")
from xlsxwriter.utility import xl_col_to_name
