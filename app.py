from __future__ import annotations

import traceback
import re
import json
import os
import subprocess

import streamlit as st
import gspread
import pandas as pd
import plotly.express as px
from google.oauth2.service_account import Credentials

# ── Configuration ──────────────────────────────────────────────────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

SHEET_CONFIGS = {
    "OKR": {
        "url": "https://docs.google.com/spreadsheets/d/1L5eaUt3JzDE-QLgsUg5sgivglhrTfb5s6cP9ZpVcpeg/edit",
        "label": "OKR 需求跟踪",
    },
    "Feature": {
        "url": "https://docs.google.com/spreadsheets/d/1QptQ8ERrCpn8sme8SdLPswEpY9oK5TKLZEJ7GVjO3Hs/edit",
        "label": "Feature 项目跟踪",
    },
}

JIRA_PROJECTS = ["SPCB", "SPCSP"]
JIRA_BASE_URL = "https://jira.shopee.io"
JIRA_YEAR = "2026"

JIRA_FIELD_IDS = {
    "product_manager": "customfield_10306",
    "estimated_prd_signoff": "customfield_36701",
    "prd_link": "customfield_15707",
    "product_line": "customfield_35604",
    "project_type": "customfield_12411",
    "prd_review_end": "customfield_11546",
    "key_project": "customfield_29500",
}

JIRA_SEARCH_FIELDS = [
    "summary", "status", "priority", "assignee", "components",
    JIRA_FIELD_IDS["product_manager"],
    JIRA_FIELD_IDS["estimated_prd_signoff"],
    JIRA_FIELD_IDS["prd_link"],
    JIRA_FIELD_IDS["product_line"],
    JIRA_FIELD_IDS["project_type"],
    JIRA_FIELD_IDS["prd_review_end"],
    JIRA_FIELD_IDS["key_project"],
]


# ── Auth Gate ──────────────────────────────────────────────────────────────────

def check_password() -> bool:
    """Return True if the user has entered the correct password, or if no
    password is configured (local dev)."""
    try:
        if "password" not in st.secrets:
            return True
    except Exception:
        return True

    if st.session_state.get("authenticated"):
        return True

    pwd = st.text_input("请输入访问密码", type="password", key="_login_pwd")
    if pwd and pwd == st.secrets["password"]:
        st.session_state["authenticated"] = True
        st.rerun()
    elif pwd:
        st.error("密码错误")
    return False


# ── Google Sheets Client ───────────────────────────────────────────────────────

@st.cache_resource
def get_client():
    if os.path.exists("credentials.json"):
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    else:
        service_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(service_info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_data(ttl=600)
def fetch_tab_names(sheet_key: str) -> list[str]:
    client = get_client()
    spreadsheet = client.open_by_url(SHEET_CONFIGS[sheet_key]["url"])
    return [ws.title for ws in spreadsheet.worksheets()]


# ── Data Loading ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def fetch_okr_data(tab_name: str) -> pd.DataFrame:
    """Sheet 1: header is row 1, data from row 2."""
    client = get_client()
    spreadsheet = client.open_by_url(SHEET_CONFIGS["OKR"]["url"])
    ws = spreadsheet.worksheet(tab_name)
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    df["_source_tab"] = tab_name
    df["_source"] = "OKR"
    return df


@st.cache_data(ttl=300)
def fetch_feature_data(tab_name: str) -> pd.DataFrame:
    """Sheet 2: row 1 = category label, row 2 = example, row 3 = actual header."""
    client = get_client()
    spreadsheet = client.open_by_url(SHEET_CONFIGS["Feature"]["url"])
    ws = spreadsheet.worksheet(tab_name)
    rows = ws.get_all_values()
    if len(rows) < 4:
        return pd.DataFrame()

    headers = rows[2]
    seen: dict[str, int] = {}
    unique_headers: list[str] = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            unique_headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            unique_headers.append(h)

    df = pd.DataFrame(rows[3:], columns=unique_headers)
    df["_source_tab"] = tab_name
    df["_source"] = "Feature"
    return df


# ── Jira Data Loading ─────────────────────────────────────────────────────

def _get_jira_token() -> str:
    """Extract Jira PAT from Streamlit secrets or skynet-base key store."""
    try:
        token = st.secrets.get("jira_token", "")
        if token:
            return token
    except Exception:
        pass

    try:
        result = subprocess.run(
            ["skynet-base", "key", "get", "JIRA_TOKEN"],
            capture_output=True, text=True, timeout=10,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "JIRA_TOKEN 未配置。本地运行: `skynet-base setup token`；"
            "Cloud 部署: 在 Streamlit Secrets 中设置 jira_token"
        )
    if result.returncode != 0:
        raise RuntimeError("JIRA_TOKEN 未配置，请在 Terminal 运行 `skynet-base setup token`")

    import re as _re
    m = _re.search(r"JIRA_TOKEN:\s*(.+)", result.stdout)
    if not m:
        raise RuntimeError("无法解析 JIRA_TOKEN")
    return m.group(1).strip()


def _jira_api_search(jql: str, fields: list[str], max_results: int = 200) -> list[dict]:
    """Call Jira REST API v2 search with pagination."""
    import urllib.request
    import urllib.parse
    import urllib.error

    token = _get_jira_token()
    all_issues: list[dict] = []
    start_at = 0

    while True:
        params = urllib.parse.urlencode({
            "jql": jql,
            "fields": ",".join(fields),
            "maxResults": min(100, max_results - len(all_issues)),
            "startAt": start_at,
        })
        url = f"{JIRA_BASE_URL}/rest/api/2/search?{params}"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise RuntimeError("Jira 认证失败，请运行 `skynet-base setup token` 重新配置")
            raise RuntimeError(f"Jira API 错误 ({e.code}): {e.reason}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"Jira 连接失败: {e.reason}")

        issues = data.get("issues", [])
        all_issues.extend(issues)
        total = data.get("total", 0)

        if len(all_issues) >= total or len(all_issues) >= max_results or not issues:
            break
        start_at += len(issues)

    return all_issues


def build_jira_jql(projects: list[str], year: str = JIRA_YEAR) -> str:
    proj_clause = ", ".join(projects)
    return (
        f"project in ({proj_clause}) AND issuetype = Epic"
        f' AND "Project Type" = "Feature Project"'
        f' AND "POP Request Link" is not EMPTY'
        f" AND status not in (Closed, Icebox)"
        f" AND created >= {int(year) - 1}-01-01"
        f" AND ("
        f"priority in (Highest, High)"
        f' OR (priority in (Medium, Low) AND "Key Project" is not EMPTY)'
        f")"
    )


JIRA_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jira_cache.json")


def _load_jira_cache() -> list[dict]:
    """Load Jira issues from local cache file (for cloud fallback)."""
    if not os.path.exists(JIRA_CACHE_FILE):
        return []
    with open(JIRA_CACHE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("issues", [])


def _jira_cache_meta() -> str:
    """Return a human-readable sync timestamp from cache, or empty string."""
    if not os.path.exists(JIRA_CACHE_FILE):
        return ""
    with open(JIRA_CACHE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("synced_at", "")


@st.cache_data(ttl=300)
def fetch_jira_issues(jql: str) -> tuple[list[dict], bool]:
    """Search Jira issues via REST API; fall back to cache on failure.

    Returns (issues, from_cache).
    """
    try:
        issues = _jira_api_search(jql, JIRA_SEARCH_FIELDS, max_results=1500)
        return issues, False
    except Exception:
        cached = _load_jira_cache()
        if cached:
            return cached, True
        raise


@st.cache_data(ttl=3600)
def discover_jira_fields(issue_key: str) -> dict:
    """Read a single issue to discover all available field names."""
    import urllib.request
    import urllib.error

    token = _get_jira_token()
    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        raise RuntimeError(f"读取 {issue_key} 失败: {e}")

    fields = data.get("fields", {})
    return {k: _summarize_field_value(v) for k, v in fields.items()}


def _summarize_field_value(val) -> str:
    if val is None:
        return "null"
    if isinstance(val, str):
        return val[:100] if len(val) > 100 else val
    if isinstance(val, dict):
        name = val.get("name") or val.get("displayName") or val.get("value")
        if name:
            return str(name)
        return json.dumps(val, ensure_ascii=False)[:100]
    if isinstance(val, list):
        previews = []
        for item in val[:3]:
            if isinstance(item, dict):
                previews.append(item.get("name") or item.get("value") or str(item)[:40])
            else:
                previews.append(str(item)[:40])
        suffix = f" +{len(val)-3} more" if len(val) > 3 else ""
        return f"[{', '.join(str(p) for p in previews)}{suffix}]"
    return str(val)[:100]


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_review_month(date_str: str) -> str:
    """Extract YYYY-MM from various date formats found in the sheets."""
    if not date_str or not isinstance(date_str, str):
        return ""
    s = date_str.strip()

    m = re.match(r"(\d{4})-(\d{1,2})-\d{1,2}", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"

    m = re.match(r"(\d{4})\s*(\d{1,2})月", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"

    return ""


def clean_pm_name(name: str) -> str:
    """Normalize PM names so that variants like 'Xun Li' and 'xun.li' merge."""
    if not name or not isinstance(name, str):
        return ""
    name = name.strip()
    if not name:
        return ""

    if "@" in name:
        name = name.split("@")[0]

    # Remove parenthetical nicknames, e.g. "Xinyuan Yang (Miranda)"
    name = re.sub(r"\s*\(.*?\)\s*", "", name)

    # Handle "/" separator (multiple PMs in one cell)
    if "/" in name:
        parts = [clean_pm_name(p) for p in name.split("/")]
        return " / ".join(p for p in parts if p)

    name = name.lower().strip()
    # "First Last" → "first.last"
    name = re.sub(r"\s+", ".", name)
    name = re.sub(r"\.+", ".", name)
    name = name.strip(".")
    return name


PM_GROUPS = {
    "CS Channel": [
        "danqian.wang", "pengbin.feng", "mengchen.tang",
        "jiamin.qi", "xun.li", "xinlei.lv",
    ],
    "Agent Console": [
        "pufan.hu", "kaiyang.chen", "jialun.lv", "wenzhen.zang",
    ],
    "Chatbot Platform": [
        "junhao.wu", "xi.cen", "mingzhuo.he",
    ],
    "Others": [
        "shunxiu.luo", "amber.xu", "xinyuan.yang", "feiyu.fan",
    ],
}

PM_TO_GROUP = {pm: group for group, pms in PM_GROUPS.items() for pm in pms}
GROUP_ORDER = list(PM_GROUPS.keys())


def get_pm_group(pm: str) -> str:
    return PM_TO_GROUP.get(pm, "未分组")


def safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype(str).str.strip()
    return pd.Series([""] * len(df), dtype=str)


# ── Normalization ──────────────────────────────────────────────────────────────

COMMON_COLS = [
    "PM", "Review_Month", "PRD_Review_Month", "Requirement", "Status",
    "Priority", "Product_Line", "Ticket", "_source", "_source_tab",
]


def normalize_okr(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COMMON_COLS)

    out = pd.DataFrame()
    out["PM"] = safe_col(df, "Product Manager\n(From Jira)").apply(clean_pm_name)
    out["Requirement"] = safe_col(df, "EPIC Description")
    out["Status"] = safe_col(df, "Project Status (From Jira)")
    out["Priority"] = safe_col(df, "Cycle Priority\n(To Jira)")
    out["Product_Line"] = safe_col(df, "Main Product Line\n (From Jira)")
    out["Ticket"] = safe_col(df, "EPIC Jira")
    out["_source"] = "OKR"
    out["_source_tab"] = safe_col(df, "_source_tab")

    prd_review = safe_col(df, "PRD Review End Date\n(To Jira)")
    prd_month = safe_col(df, "PRD Month")
    est_month = safe_col(df, "Est. PRD Month")
    est_signoff = safe_col(df, "Estimated PRD Sign Off Date\n(From Jira)")

    out["PRD_Review_Month"] = prd_review.apply(parse_review_month)

    out["Review_Month"] = out["PRD_Review_Month"].copy()
    mask = out["Review_Month"] == ""
    out.loc[mask, "Review_Month"] = prd_month[mask].apply(parse_review_month)
    mask = out["Review_Month"] == ""
    out.loc[mask, "Review_Month"] = est_month[mask].apply(parse_review_month)
    mask = out["Review_Month"] == ""
    out.loc[mask, "Review_Month"] = est_signoff[mask].apply(parse_review_month)

    out = out[(out["PM"] != "") & (out["Requirement"] != "")]
    return out[COMMON_COLS].reset_index(drop=True)


def _jira_field_str(obj) -> str:
    """Extract a string value from a Jira field that may be str, dict, or None."""
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj.strip()
    if isinstance(obj, dict):
        return (obj.get("name") or obj.get("displayName") or obj.get("value") or "").strip()
    return str(obj).strip()


def normalize_jira(
    issues: list[dict],
    allowed_pms: list[str] | None = None,
    year: str = JIRA_YEAR,
) -> pd.DataFrame:
    """Normalize Jira REST API issues to COMMON_COLS.

    Rules for month assignment:
    - Past months (before current month): use PRD Review End Date,
      require PRD Link non-empty.
    - Current month & future: use Estimated PRD Sign Off Date,
      PRD Link may be empty.
    """
    if not issues:
        return pd.DataFrame(columns=COMMON_COLS)

    from datetime import date
    current_month = date.today().strftime("%Y-%m")

    f_pm = JIRA_FIELD_IDS["product_manager"]
    f_signoff = JIRA_FIELD_IDS["estimated_prd_signoff"]
    f_prd_link = JIRA_FIELD_IDS["prd_link"]
    f_product_line = JIRA_FIELD_IDS["product_line"]
    f_prd_review_end = JIRA_FIELD_IDS["prd_review_end"]

    rows: list[dict] = []
    for issue in issues:
        fields = issue.get("fields", {})
        key = issue.get("key", "")

        prd_link = _jira_field_str(fields.get(f_prd_link))
        signoff_str = _jira_field_str(fields.get(f_signoff))
        prd_review_str = _jira_field_str(fields.get(f_prd_review_end))

        signoff_month = parse_review_month(signoff_str)
        prd_review_month = parse_review_month(prd_review_str)

        review_month = ""
        if prd_review_month and prd_review_month < current_month:
            if not prd_link:
                continue
            review_month = prd_review_month
        elif signoff_month and signoff_month >= current_month:
            review_month = signoff_month
        elif prd_review_month:
            if not prd_link:
                continue
            review_month = prd_review_month
        elif signoff_month:
            review_month = signoff_month

        if review_month and not review_month.startswith(year):
            continue

        pm_raw = fields.get(f_pm)
        if pm_raw is None:
            pm_raw = fields.get("assignee")
        pm_clean = clean_pm_name(_jira_field_str(pm_raw))

        if allowed_pms and pm_clean and pm_clean not in allowed_pms:
            continue

        status_name = _jira_field_str(fields.get("status"))
        priority_name = _jira_field_str(fields.get("priority"))
        product_line = _jira_field_str(fields.get(f_product_line))

        if not product_line:
            components = fields.get("components") or []
            if isinstance(components, list):
                product_line = ", ".join(_jira_field_str(c) for c in components if _jira_field_str(c))

        rows.append({
            "PM": pm_clean,
            "Requirement": fields.get("summary", ""),
            "Status": status_name,
            "Priority": priority_name,
            "Product_Line": product_line,
            "Ticket": key,
            "Review_Month": review_month,
            "PRD_Review_Month": prd_review_month or review_month,
            "_source": "Jira",
            "_source_tab": "Jira Search",
        })

    df = pd.DataFrame(rows, columns=COMMON_COLS)
    df = df[(df["PM"] != "") & (df["Requirement"] != "")]
    return df.reset_index(drop=True)


def normalize_feature(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COMMON_COLS)

    out = pd.DataFrame()
    out["PM"] = safe_col(df, "PM_PIC").apply(clean_pm_name)
    out["Requirement"] = safe_col(df, "Feature_Name")
    out["Status"] = safe_col(df, "Status")
    out["Priority"] = safe_col(df, "排期优先级")
    out["Product_Line"] = safe_col(df, "Product_line")
    out["Ticket"] = safe_col(df, "SPCB")
    out["_source"] = "Feature"
    out["_source_tab"] = safe_col(df, "_source_tab")

    prd_end = safe_col(df, "PRD_End_date")
    est_prd = safe_col(df, "Est_PRD_Date")
    target_m = safe_col(df, "Target_Month")

    out["PRD_Review_Month"] = prd_end.apply(parse_review_month)

    out["Review_Month"] = out["PRD_Review_Month"].copy()
    mask = out["Review_Month"] == ""
    out.loc[mask, "Review_Month"] = est_prd[mask].apply(parse_review_month)
    mask = out["Review_Month"] == ""
    out.loc[mask, "Review_Month"] = target_m[mask].apply(parse_review_month)

    out = out[(out["PM"] != "") & (out["Requirement"] != "")]
    return out[COMMON_COLS].reset_index(drop=True)


# ── UI Components ──────────────────────────────────────────────────────────────


def render_monthly_total_chart(df_with_month: pd.DataFrame, month_col: str = "Review_Month"):
    """Upper section: monthly total bar chart with count labels."""
    if df_with_month.empty:
        st.info("没有带评审日期的数据")
        return

    monthly_total = (
        df_with_month
        .groupby(month_col)
        .size()
        .reset_index(name="需求数量")
        .sort_values(month_col)
    )
    total_all = int(monthly_total["需求数量"].sum())

    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=monthly_total[month_col],
        y=monthly_total["需求数量"],
        text=monthly_total["需求数量"],
        textposition="outside",
        textfont=dict(size=15, color="#333"),
        marker_color="#4C78A8",
    ))
    fig.update_layout(
        xaxis=dict(
            title="评审月份",
            type="category",
            tickangle=-45,
        ),
        yaxis=dict(title="需求数量"),
        height=400,
        bargap=0.3,
        annotations=[dict(
            text=f"合计: {total_all}",
            xref="paper", yref="paper",
            x=1, y=1.08,
            showarrow=False,
            font=dict(size=16, color="#E45756"),
        )],
    )
    st.plotly_chart(fig, use_container_width=True)


def render_pm_monthly_table(df_with_month: pd.DataFrame, all_pms: list[str], month_col: str = "Review_Month"):
    """Lower section: PM × Month table grouped by team with subtotals.

    all_pms: the full list of selected PMs (from sidebar filter) so that
    PMs with 0 records in the current month range still appear.
    """
    if df_with_month.empty:
        month_cols: list[str] = []
    else:
        month_cols = sorted(df_with_month[month_col].unique().tolist())

    pm_month_counts: dict[str, dict[str, int]] = {}
    if not df_with_month.empty:
        for (pm, month), cnt in df_with_month.groupby(["PM", month_col]).size().items():
            pm_month_counts.setdefault(pm, {})[month] = cnt

    all_groups = list(GROUP_ORDER)
    grouped_pms_set = {pm for pms in PM_GROUPS.values() for pm in pms}
    ungrouped = sorted(set(all_pms) - grouped_pms_set)
    if ungrouped:
        all_groups.append("未分组")

    rows: list[dict] = []
    for group in all_groups:
        if group == "未分组":
            members = ungrouped
        else:
            members = PM_GROUPS.get(group, [])
        visible = [pm for pm in members if pm in all_pms]
        if not visible:
            continue

        for pm in visible:
            row: dict = {"分组": group, "PM": pm}
            counts = pm_month_counts.get(pm, {})
            for c in month_cols:
                row[c] = counts.get(c, 0)
            row["总计"] = sum(counts.get(c, 0) for c in month_cols)
            rows.append(row)

        n_pms = len(visible)
        avg_row: dict = {"分组": group, "PM": "月人均"}
        total_sum = 0
        for c in month_cols:
            col_sum = sum(pm_month_counts.get(pm, {}).get(c, 0) for pm in visible)
            avg_row[c] = round(col_sum / n_pms, 1) if n_pms else 0
            total_sum += col_sum
        avg_row["总计"] = round(total_sum / n_pms, 1) if n_pms else 0
        rows.append(avg_row)

    grand_row: dict = {"分组": "", "PM": "总合计"}
    for c in month_cols:
        grand_row[c] = sum(r[c] for r in rows if r["PM"] not in ("月人均", "总合计"))
    grand_row["总计"] = sum(grand_row.get(c, 0) for c in month_cols)
    rows.append(grand_row)

    if not rows:
        st.info("没有数据")
        return

    result = pd.DataFrame(rows)

    def highlight_rows(row):
        pm = row["PM"]
        if pm == "月人均":
            return ["background-color: #fff3cd; font-weight: bold"] * len(row)
        if pm == "总合计":
            return ["background-color: #d4edda; font-weight: bold"] * len(row)
        return [""] * len(row)

    num_cols = [c for c in month_cols + ["总计"] if c in result.columns]
    styled = (
        result.style
        .apply(highlight_rows, axis=1)
        .format({c: lambda v: f"{v:.1f}" if isinstance(v, float) else f"{v}" for c in num_cols}, subset=num_cols)
    )

    st.dataframe(styled, use_container_width=True, height=max(300, len(rows) * 36 + 60), hide_index=True)


def render_detail_table(df: pd.DataFrame, key_prefix: str = "dt"):
    view = df.copy()

    has_match = "_match" in view.columns

    filter_cols = [1, 1, 1, 1, 2] if has_match else [1, 1, 1, 2]
    cols = st.columns(filter_cols)
    idx = 0
    with cols[idx]:
        f_pm = st.multiselect("PM", sorted(view["PM"].unique()), default=[], key=f"{key_prefix}_pm")
    idx += 1
    with cols[idx]:
        f_month = st.multiselect("评审月份", sorted(view.loc[view["Review_Month"] != "", "Review_Month"].unique()), default=[], key=f"{key_prefix}_month")
    idx += 1
    with cols[idx]:
        f_status = st.multiselect("状态", sorted(view["Status"].unique()), default=[], key=f"{key_prefix}_status")
    idx += 1
    if has_match:
        with cols[idx]:
            match_options = sorted(view["_match"].unique())
            f_match = st.multiselect("数据匹配", match_options, default=[], key=f"{key_prefix}_match")
        idx += 1
    else:
        f_match = []
    with cols[idx]:
        keyword = st.text_input("🔎 关键字搜索（需求名称）", "", key=f"{key_prefix}_keyword")

    if f_pm:
        view = view[view["PM"].isin(f_pm)]
    if f_month:
        view = view[view["Review_Month"].isin(f_month)]
    if f_status:
        view = view[view["Status"].isin(f_status)]
    if f_match and has_match:
        view = view[view["_match"].isin(f_match)]
    if keyword:
        view = view[view["Requirement"].str.contains(keyword, case=False, na=False)]

    st.caption(f"共 **{len(view)}** 条需求")

    display_cols = list(COMMON_COLS)
    rename_map = {
        "PM": "PM",
        "Review_Month": "评审月份",
        "Requirement": "需求名称",
        "Status": "状态",
        "Priority": "优先级",
        "Product_Line": "产品线",
        "Ticket": "Ticket",
        "_source": "数据源",
        "_source_tab": "来源 Tab",
    }
    if has_match:
        display_cols.append("_match")
        rename_map["_match"] = "数据匹配"

    available_cols = [c for c in display_cols if c in view.columns]
    st.dataframe(
        view[available_cols].rename(columns=rename_map),
        use_container_width=True,
        height=600,
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="PM 需求评审分析", layout="wide", page_icon="📊")

    if not check_password():
        st.stop()

    st.title("📊 PM 需求评审月度分析")

    # ── Sidebar: Data Source ──────────────────────────────────────────────
    st.sidebar.header("📂 数据源配置")

    if st.sidebar.button("🔄 刷新数据（清除缓存）"):
        fetch_okr_data.clear()
        fetch_feature_data.clear()
        fetch_jira_issues.clear()
        st.rerun()

    try:
        okr_tabs = fetch_tab_names("OKR")
        okr_quarterly = [t for t in okr_tabs if "OKR" in t and "Statistic" not in t]
    except Exception as e:
        st.sidebar.error(f"获取 OKR tab 列表失败: {e}")
        okr_quarterly = []

    try:
        feat_tabs = fetch_tab_names("Feature")
        feat_quarterly = [t for t in feat_tabs if "feature project" in t.lower()]
    except Exception as e:
        st.sidebar.error(f"获取 Feature tab 列表失败: {e}")
        feat_quarterly = []

    selected_okr = st.sidebar.multiselect(
        "OKR Tabs",
        okr_quarterly,
        default=[t for t in ["Q1 2026 OKR", "Q2 2026 OKR"] if t in okr_quarterly],
    )
    selected_feat = st.sidebar.multiselect(
        "Feature Tabs",
        feat_quarterly,
        default=[t for t in ["26Q1 feature project", "26Q2 feature project"] if t in feat_quarterly],
    )

    st.sidebar.markdown("---")
    enable_jira = st.sidebar.checkbox("🔗 启用 Jira 数据源", value=True, key="enable_jira")
    jira_discover_mode = False
    if enable_jira:
        with st.sidebar.expander("Jira 高级配置", expanded=False):
            jira_projects_input = st.text_input(
                "项目 Key（逗号分隔）",
                value=", ".join(JIRA_PROJECTS),
                key="jira_projects",
            )
            jira_discover_mode = st.checkbox(
                "🔍 字段发现模式（调试用）",
                value=False,
                key="jira_discover",
                help="读取 SPCB-53026 显示所有可用字段，用于配置自定义字段 ID",
            )

    if not selected_okr and not selected_feat and not enable_jira:
        st.warning("请在左侧选择至少一个数据 Tab 或启用 Jira 数据源")
        return

    # ── Load & Normalize ─────────────────────────────────────────────────
    parts: list[pd.DataFrame] = []
    with st.spinner("正在加载数据…"):
        for tab in selected_okr:
            try:
                parts.append(normalize_okr(fetch_okr_data(tab)))
            except Exception as e:
                st.warning(f"OKR [{tab}] 加载失败: {e}")
                print(traceback.format_exc())

        for tab in selected_feat:
            try:
                parts.append(normalize_feature(fetch_feature_data(tab)))
            except Exception as e:
                st.warning(f"Feature [{tab}] 加载失败: {e}")
                print(traceback.format_exc())

    jira_df = pd.DataFrame(columns=COMMON_COLS)
    jira_from_cache = False
    if enable_jira:
        with st.spinner("正在从 Jira 加载数据…"):
            try:
                proj_keys = [p.strip() for p in jira_projects_input.split(",") if p.strip()]
                all_pm_names = [pm for pms in PM_GROUPS.values() for pm in pms]
                jql = build_jira_jql(proj_keys)
                issues, jira_from_cache = fetch_jira_issues(jql)
                jira_df = normalize_jira(issues, allowed_pms=all_pm_names)
                if jira_from_cache:
                    cache_time = _jira_cache_meta()
                    st.info(f"Jira API 不可达，已从本地缓存加载数据（同步时间: {cache_time}）。"
                            f"本地运行 `python sync_jira.py --push` 可更新缓存。")
            except Exception as e:
                st.warning(f"Jira 数据加载失败: {e}")
                print(traceback.format_exc())

    if not parts and jira_df.empty:
        st.error("所有数据源均加载失败，请检查网络与权限")
        return

    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COMMON_COLS)

    # Deduplicate by Ticket ID (keep first occurrence, preserve rows with no ticket)
    has_ticket = df["Ticket"].str.strip().ne("")
    df_with_ticket = df[has_ticket].drop_duplicates(subset="Ticket", keep="first")
    df_no_ticket = df[~has_ticket]
    df = pd.concat([df_with_ticket, df_no_ticket], ignore_index=True)

    # ── Sidebar: Global Filters ──────────────────────────────────────────
    st.sidebar.markdown("---")
    st.sidebar.header("🔍 全局筛选")

    DEFAULT_PMS = [
        "amber.xu", "jialun.lv", "jiamin.qi",
        "kaiyang.chen", "mengchen.tang", "mingzhuo.he",
        "pengbin.feng", "shunxiu.luo", "wenzhen.zang",
        "xi.cen", "xinlei.lv", "xinyuan.yang", "xun.li",
    ]
    all_pms = sorted(df["PM"].unique())
    default_pms = [p for p in DEFAULT_PMS if p in all_pms]
    sel_pms = st.sidebar.multiselect("👤 PM", all_pms, default=default_pms)
    if sel_pms:
        df = df[df["PM"].isin(sel_pms)]

    all_months = sorted(df.loc[df["Review_Month"] != "", "Review_Month"].unique())
    default_months = [m for m in all_months if m.startswith("2026-")]
    sel_months = st.sidebar.multiselect("📅 评审月份", all_months, default=default_months)
    if sel_months:
        df = df[df["Review_Month"].isin(sel_months)]

    all_statuses = sorted(df["Status"].unique())
    sel_statuses = st.sidebar.multiselect("📌 状态", all_statuses, default=[])
    if sel_statuses:
        df = df[df["Status"].isin(sel_statuses)]

    all_sources = sorted(df["_source"].unique())
    sel_sources = st.sidebar.multiselect("📂 数据源", all_sources, default=[])
    if sel_sources:
        df = df[df["_source"].isin(sel_sources)]

    all_prod_lines = sorted(df.loc[df["Product_Line"] != "", "Product_Line"].unique())
    sel_prod = st.sidebar.multiselect("🏷️ 产品线", all_prod_lines, default=[])
    if sel_prod:
        df = df[df["Product_Line"].isin(sel_prod)]

    # ── Split by month availability ──────────────────────────────────────
    df_with_month = df[df["Review_Month"] != ""].copy()

    # ── Active filter summary ────────────────────────────────────────────
    active_filters = []
    if sel_pms:
        active_filters.append(f"PM: {', '.join(sel_pms)}")
    if sel_months:
        active_filters.append(f"月份: {', '.join(sel_months)}")
    if sel_statuses:
        active_filters.append(f"状态: {', '.join(sel_statuses)}")
    if sel_sources:
        active_filters.append(f"数据源: {', '.join(sel_sources)}")
    if sel_prod:
        active_filters.append(f"产品线: {', '.join(sel_prod)}")

    if active_filters:
        st.info("当前筛选条件：" + " ｜ ".join(active_filters))

    # ── Split by PRD review date availability ──────────────────────────
    df_with_prd_month = df[df["PRD_Review_Month"] != ""].copy()

    # ── Build merged detail table with match status ─────────────────────
    def build_merged_detail(gs_df: pd.DataFrame, jira_df: pd.DataFrame) -> pd.DataFrame:
        gs = gs_df.copy()
        jr = jira_df.copy()
        gs["_match"] = ""
        jr["_match"] = ""

        gs_tickets = set(gs.loc[gs["Ticket"].str.strip() != "", "Ticket"])
        jr_tickets = set(jr.loc[jr["Ticket"].str.strip() != "", "Ticket"])
        both_tickets = gs_tickets & jr_tickets

        gs.loc[gs["Ticket"].isin(both_tickets), "_match"] = "Both"
        gs.loc[(gs["Ticket"].str.strip() != "") & (~gs["Ticket"].isin(both_tickets)), "_match"] = "GSheet Only"
        gs.loc[gs["Ticket"].str.strip() == "", "_match"] = "GSheet Only"

        jr_only = jr[~jr["Ticket"].isin(gs_tickets)].copy()
        jr_only["_match"] = "Jira Only"

        merged = pd.concat([gs, jr_only], ignore_index=True)
        return merged

    if enable_jira and not jira_df.empty:
        merged_df = build_merged_detail(df, jira_df)
    else:
        merged_df = df.copy()
        merged_df["_match"] = "GSheet Only"

    # ── Tabs ─────────────────────────────────────────────────────────────
    tab_names = []
    if enable_jira:
        tab_names.append("🔗 From Jira")
    tab_names.extend(["📈 评审需求汇总", "📅 PRD 评审汇总", "📋 需求明细"])

    tabs = st.tabs(tab_names)
    tab_idx = 0

    if enable_jira:
        with tabs[tab_idx]:
            if jira_discover_mode:
                st.subheader("🔍 Jira 字段发现")
                st.caption("读取样本 issue 以发现可用字段名称和自定义字段 ID")
                sample_key = st.text_input("Issue Key", value="SPCB-53026", key="jira_sample_key")
                if st.button("读取字段", key="jira_discover_btn"):
                    with st.spinner("正在读取…"):
                        try:
                            field_map = discover_jira_fields(sample_key)
                            if field_map:
                                st.success(f"发现 {len(field_map)} 个字段")
                                search_kw = st.text_input("搜索字段名", "", key="field_search")
                                display_fields = {
                                    k: v for k, v in sorted(field_map.items())
                                    if not search_kw or search_kw.lower() in k.lower() or search_kw.lower() in str(v).lower()
                                }
                                st.dataframe(
                                    pd.DataFrame(
                                        [(k, v) for k, v in display_fields.items()],
                                        columns=["字段 ID", "示例值"],
                                    ),
                                    use_container_width=True,
                                    height=600,
                                )
                            else:
                                st.warning("未能解析字段信息")
                        except Exception as e:
                            st.error(f"读取失败: {e}")
                st.markdown("---")

            st.subheader("Jira 需求评审汇总")
            st.caption(
                f"数据来源: Jira ({', '.join(JIRA_PROJECTS)}) · "
                f"Epic · Feature Project · POP Link 非空 · "
                f"Highest/High + Medium/Low(Key Project) · "
                f"历史月份: PRD Review End Date + PRD Link 非空 · "
                f"本月及未来: Estimated PRD Sign Off Date"
            )

            if jira_df.empty:
                st.info("暂无 Jira 数据。请确认 Jira Token 已配置（`skynet-base setup token`）且查询条件正确。")
            else:
                jira_filtered = jira_df.copy()
                if sel_pms:
                    jira_filtered = jira_filtered[jira_filtered["PM"].isin(sel_pms)]
                if sel_months:
                    jira_filtered = jira_filtered[jira_filtered["Review_Month"].isin(sel_months)]
                if sel_statuses:
                    jira_filtered = jira_filtered[jira_filtered["Status"].isin(sel_statuses)]
                if sel_prod:
                    jira_filtered = jira_filtered[jira_filtered["Product_Line"].isin(sel_prod)]

                jira_with_month = jira_filtered[jira_filtered["Review_Month"] != ""].copy()

                render_monthly_total_chart(jira_with_month)

                st.markdown("---")

                st.subheader("各 PM 每月评审需求数量（Jira）")
                jira_pm_list = sel_pms if sel_pms else sorted(jira_filtered["PM"].unique().tolist())
                render_pm_monthly_table(jira_with_month, jira_pm_list)

        tab_idx += 1

    with tabs[tab_idx]:
        st.subheader("每月评审需求总量")
        render_monthly_total_chart(df_with_month)

        st.markdown("---")

        st.subheader("各 PM 每月评审需求数量")
        selected_pm_list = sel_pms if sel_pms else sorted(df["PM"].unique().tolist())
        render_pm_monthly_table(df_with_month, selected_pm_list)

    with tabs[tab_idx + 1]:
        st.subheader("每月 PRD 评审需求总量")
        st.caption("仅统计有 PRD Review End Date 的需求")
        render_monthly_total_chart(df_with_prd_month, month_col="PRD_Review_Month")

        st.markdown("---")

        st.subheader("各 PM 每月 PRD 评审需求数量")
        selected_pm_list = sel_pms if sel_pms else sorted(df["PM"].unique().tolist())
        render_pm_monthly_table(df_with_prd_month, selected_pm_list, month_col="PRD_Review_Month")

    with tabs[tab_idx + 2]:
        st.subheader("需求明细（合并）")
        render_detail_table(merged_df, key_prefix="merged_dt")


if __name__ == "__main__":
    main()
