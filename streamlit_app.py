# streamlit_app.py
import io
import calendar
from datetime import date, timedelta
import contextlib

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

TAB_LABELS = ["Build workbook", "Financial metrics", "Search ticker"]
TAB_TO_SLUG = {
    "Build workbook": "build",
    "Financial metrics": "financials",
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


def year_date_range(year: int) -> tuple[date, date]:
    start = date(year, 1, 1)
    end = date(year, 12, 31)
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

    # Preserve original column order, but normalize required column names
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

@st.cache_data(show_spinner=False)
def _get_income_statement(ticker: str, period_type: str) -> pd.DataFrame:
    yf_ticker = yf.Ticker(ticker)
    if period_type == "Annual":
        stmt = getattr(yf_ticker, "income_stmt", None)
        if stmt is None or stmt.empty:
            stmt = getattr(yf_ticker, "financials", None)
    else:
        stmt = getattr(yf_ticker, "quarterly_income_stmt", None)
        if stmt is None or stmt.empty:
            stmt = getattr(yf_ticker, "quarterly_financials", None)

    if stmt is None or not isinstance(stmt, pd.DataFrame) or stmt.empty:
        return pd.DataFrame()
    return stmt.copy()

def fetch_financials_for_ticker(ticker: str, period_type: str, max_periods: int) -> pd.DataFrame:
    stmt = _get_income_statement(ticker, period_type)
    if stmt is None or stmt.empty:
        return pd.DataFrame()

    normalized_index = { _normalize_header(idx): idx for idx in stmt.index }
    revenue_label = normalized_index.get(_normalize_header("Total Revenue"))
    net_label = normalized_index.get(_normalize_header("Net Income"))

    period_cols = list(stmt.columns)
    period_dates = pd.to_datetime(period_cols, errors="coerce")
    order = sorted(
        range(len(period_cols)),
        key=lambda i: (pd.Timestamp.min if pd.isna(period_dates[i]) else period_dates[i]),
        reverse=True
    )
    ordered_cols = [period_cols[i] for i in order][:max_periods]

    rows = []
    for col in ordered_cols:
        period_end = pd.to_datetime(col, errors="coerce")
        period_end_value = period_end.date() if pd.notna(period_end) else ""

        gross_value = pd.NA
        net_value = pd.NA
        if revenue_label is not None:
            gross_value = stmt.at[revenue_label, col]
        if net_label is not None:
            net_value = stmt.at[net_label, col]

        rows.append({
            "Ticker": ticker,
            "Period Type": period_type,
            "Period End": period_end_value,
            "Gross Revenue": gross_value,
            "Net Profit": net_value,
            "Warning": "",
        })

    return pd.DataFrame(rows)

def fetch_history_for_ticker(ticker: str, start_d: date, end_d: date) -> pd.DataFrame:
    """
    Fetch OHLCV using yfinance.
    Note: yfinance's 'end' is exclusive; add one day to make the range inclusive.
    """
    # Ensure inclusive end date
    end_exclusive = end_d + timedelta(days=1)
    err_buf = io.StringIO()
    try:
        with contextlib.redirect_stderr(err_buf):
            df = yf.download(
                ticker,
                start=start_d,
                end=end_exclusive,
                progress=False,
                auto_adjust=False,  # keep raw OHLCV; you may switch to True if desired
                threads=False
            )
    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        if "YFRateLimitError" in error_text or "Rate limited" in error_text:
            st.warning(
                f"Yahoo Finance rate limit hit while fetching {ticker}. "
                "Please try again later."
            )
        df = pd.DataFrame()
    else:
        err_text = err_buf.getvalue()
        if "YFRateLimitError" in err_text or "Rate limited" in err_text:
            st.warning(
                f"Yahoo Finance rate limit hit while fetching {ticker}. "
                "Please try again later."
            )

    if df is None or df.empty:
        return pd.DataFrame()

    if st.session_state.get("show_raw_yf"):
        st.write(f"Raw yfinance output for {ticker}")
        st.write("Columns:", list(df.columns))
        st.dataframe(df.head())

    df = df.reset_index()  # bring Date from index to column
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [_flatten_col_label(col) for col in df.columns]

    normalized = {
        _normalize_header(col): col for col in df.columns
    }

    date_col = normalized.get("date") or normalized.get("datetime") or df.columns[0]
    open_col = normalized.get("open") or normalized.get("regularmarketopen")
    high_col = normalized.get("high")
    low_col = normalized.get("low")
    close_col = normalized.get("close") or normalized.get("regularmarketclose") or normalized.get("adjclose")
    volume_col = normalized.get("volume") or normalized.get("regularmarketvolume")

    if not all([open_col, high_col, low_col, close_col, volume_col]):
        return pd.DataFrame()

    df = df[[date_col, open_col, high_col, low_col, close_col, volume_col]]
    df = df.rename(columns={
        date_col: "Date",
        open_col: "Open",
        high_col: "High",
        low_col: "Low",
        close_col: "Close",
        volume_col: "Volume",
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

def write_company_sheet(writer, sheet_name: str, df_prices: pd.DataFrame) -> str | None:
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
    percent_fmt = workbook.add_format({"num_format": "0.00%"})

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
    unique_periods: list[str] = []
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

    # Percent change between first and last selected periods (if >= 2 periods)
    change_cell_ref = None
    if has_period and len(unique_periods) >= 2:
        change_row = current_summary_row + 1
        worksheet.write(change_row, summary_col, "Change % (first->last)")
        value_col_letter = xl_col_to_name(summary_col + 1)
        first_row = 1
        last_row = len(unique_periods)
        first_cell = f"{value_col_letter}{first_row + 1}"
        last_cell = f"{value_col_letter}{last_row + 1}"
        change_formula = f"=IFERROR(({last_cell}-{first_cell})/{first_cell},\"\")"
        worksheet.write_formula(change_row, summary_col + 1, change_formula, percent_fmt)
        change_cell_ref = f"{value_col_letter}{change_row + 1}"

    return change_cell_ref

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
        workbook = writer.book
        bold_fmt = workbook.add_format({"bold": True})
        percent_fmt = workbook.add_format({"num_format": "0.00%"})
        change_col = len(companies_df.columns)
        change_header = "Change % (first->last)"
        companies_ws.write(0, change_col, change_header, bold_fmt)
        change_width = max(len(change_header), len("0.00%")) + 2
        companies_ws.set_column(change_col, change_col, change_width, percent_fmt)

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
                change_cell_ref = write_company_sheet(writer, sheet_name, prices_df)
            else:
                empty_df = pd.DataFrame(
                    columns=["Period", "Date", "Price", "Open", "High", "Low", "Volume"]
                )
                empty_df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                worksheet.write("H2", "No data for the selected range/ticker.")
                autosize_columns(writer, sheet_name, empty_df, start_row=0, start_col=0, extra_pad=2)
                change_cell_ref = None

            # Add hyperlink from companies sheet "No" column to the corresponding sheet
            link_cell = f"A{idx + 1}"
            display_text = str(getattr(row, "No"))
            sheet_ref = sheet_name.replace("'", "''")  # escape single quotes for Excel
            companies_ws.write_url(
                link_cell,
                f"internal:'{sheet_ref}'!A1",
                string=display_text
            )
            if change_cell_ref:
                formula = f"='{sheet_ref}'!{change_cell_ref}"
                companies_ws.write_formula(idx, change_col, formula, percent_fmt)

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

tab_build, tab_financials, tab_search = st.tabs(TAB_LABELS)

tab_mapping_js = json.dumps(TAB_TO_SLUG)
desired_slug_js = json.dumps(active_tab_slug)

with tab_build:
    st.subheader("1) Upload companies list")
    uploaded = st.file_uploader(
        "Upload an Excel file (.xlsx) with columns: No, Name, Ticker, IsRussian",
        type=["xlsx"],
        accept_multiple_files=False
    )

    st.checkbox("Show raw yfinance responses while building", key="show_raw_yf")

    today = date.today()

    st.subheader("2) Select periods")
    period_mode = st.radio(
        "Period mode",
        options=["Quarterly", "Yearly"],
        horizontal=True
    )

    year_choices = list(range(today.year, today.year - 6, -1))

    selected_quarters: list[str] = []
    quarter_lookup: dict[str, tuple[int, int]] = {}
    selected_years: list[int] = []

    if period_mode == "Quarterly":
        quarter_choices = []
        for year in year_choices:
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
    else:
        default_year = today.year - 1
        default_years = [default_year] if default_year in year_choices else year_choices[:1]
        selected_years = st.multiselect(
            "Choose one or more years to include",
            options=year_choices,
            default=default_years
        )

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
                if period_mode == "Quarterly":
                    for label in selected_quarters:
                        if label not in quarter_lookup:
                            continue
                        year, quarter = quarter_lookup[label]
                        try:
                            q_start, q_end = quarter_date_range(year, quarter)
                        except ValueError:
                            continue
                        periods.append({"label": label, "start": q_start, "end": q_end})
                else:
                    for year in selected_years:
                        try:
                            y_start, y_end = year_date_range(int(year))
                        except ValueError:
                            continue
                        periods.append({"label": f"{int(year)}", "start": y_start, "end": y_end})

                if include_custom_range and start_date and end_date and start_date <= end_date:
                    periods.append({
                        "label": f"Custom {start_date} to {end_date}",
                        "start": start_date,
                        "end": end_date,
                    })

                if not periods:
                    st.error("Select at least one period or include the custom date range before building.")
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

with tab_financials:
    st.subheader("Financial metrics (Gross revenue, Net profit)")
    st.caption("Gross revenue uses Yahoo Finance 'Total Revenue'. Net profit uses 'Net Income'.")

    fin_uploaded = st.file_uploader(
        "Upload an Excel file (.xlsx) with columns: No, Name, Ticker, IsRussian",
        type=["xlsx"],
        accept_multiple_files=False,
        key="financials_upload"
    )

    period_type = st.radio(
        "Period type",
        options=["Quarterly", "Annual"],
        horizontal=True,
        key="financials_period_type"
    )
    max_periods = st.slider(
        "Latest N periods",
        min_value=1,
        max_value=12,
        value=4,
        step=1,
        key="financials_max_periods"
    )

    include_gross = st.checkbox("Gross revenue", value=True, key="financials_gross")
    include_net = st.checkbox("Net profit", value=True, key="financials_net")

    st.info("Russian tickers are skipped: MOEX does not provide income statements via this app's data source.")

    fin_btn = st.button("Fetch financial metrics", type="primary", use_container_width=True, key="financials_fetch")

    if fin_btn:
        if fin_uploaded is None:
            st.error("Please upload an Excel file with the required columns.")
        elif not include_gross and not include_net:
            st.error("Select at least one metric to fetch.")
        else:
            try:
                raw_df = pd.read_excel(fin_uploaded)
                companies = normalize_companies_df(raw_df)
            except Exception as e:
                st.error(f"Failed to read/validate the uploaded file: {e}")
            else:
                result_frames: list[pd.DataFrame] = []
                total = len(companies)
                progress = st.progress(0.0, text="Preparing to fetch financials…")
                for idx, row in enumerate(companies.itertuples(index=False), start=1):
                    ticker = str(getattr(row, "Ticker")).strip()
                    is_russian = bool(getattr(row, "IsRussian"))
                    progress_value = (idx - 1) / total if total else 1.0
                    progress.progress(progress_value, text=f"Fetching {ticker} — {idx}/{total}")

                    if is_russian:
                        result_frames.append(pd.DataFrame([{
                            "Ticker": ticker,
                            "Period Type": period_type,
                            "Period End": "",
                            "Gross Revenue": pd.NA,
                            "Net Profit": pd.NA,
                            "Warning": "Financial statements not supported for MOEX tickers",
                        }]))
                        continue

                    fin_df = fetch_financials_for_ticker(ticker, period_type, max_periods)
                    if fin_df.empty:
                        result_frames.append(pd.DataFrame([{
                            "Ticker": ticker,
                            "Period Type": period_type,
                            "Period End": "",
                            "Gross Revenue": pd.NA,
                            "Net Profit": pd.NA,
                            "Warning": "No income statement data",
                        }]))
                    else:
                        result_frames.append(fin_df)

                if result_frames:
                    results = pd.concat(result_frames, ignore_index=True)
                else:
                    results = pd.DataFrame(
                        columns=["Ticker", "Period Type", "Period End", "Gross Revenue", "Net Profit", "Warning"]
                    )

                if not include_gross and "Gross Revenue" in results.columns:
                    results = results.drop(columns=["Gross Revenue"])
                if not include_net and "Net Profit" in results.columns:
                    results = results.drop(columns=["Net Profit"])

                if "Period End" in results.columns:
                    results = results.sort_values(
                        by=["Ticker", "Period End"],
                        ascending=[True, False],
                        na_position="last"
                    )

                if total:
                    progress.progress(1.0, text="All financials fetched.")
                else:
                    progress.progress(1.0, text="No companies supplied.")

                st.dataframe(results, use_container_width=True)
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
           "Select the needed quarters or years, keep the date range if desired, and the workbook will compute VWAP per period.")
