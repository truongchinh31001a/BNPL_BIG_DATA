"""BNPL Command Center: a read-only presentation UI over PostgreSQL serving data."""

from __future__ import annotations

import json
import os
import socket
import time
from contextlib import closing
from datetime import date, datetime, timezone
from uuid import uuid4

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import requests
import streamlit as st
from kafka import KafkaProducer

import queries


POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "bnpl_dw"),
    "user": os.getenv("POSTGRES_USER", "bnpl"),
    "password": os.getenv("POSTGRES_PASSWORD", "bnpl_password"),
    "connect_timeout": 3,
}
HDFS_JMX_URL = os.getenv("HDFS_JMX_URL", "http://namenode:9870/jmx")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_HTTP_URL", "http://spark-master:8080")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bnpl.transactions.raw")
ACCENT = "#22d3a7"
BLUE = "#4f8cff"
AMBER = "#f7b955"
RED = "#ff647c"
INK = "#172033"
GRID = "rgba(75, 94, 122, .14)"


st.set_page_config(
    page_title="BNPL Command Center",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    :root { --accent: #1769e0; --panel: rgba(255, 255, 255, .96); }
    .stApp {
        background:
          radial-gradient(circle at 82% -10%, rgba(79,140,255,.13), transparent 34rem),
          radial-gradient(circle at 4% 18%, rgba(34,211,167,.08), transparent 28rem),
          #f4f7fb;
        color: #172033;
    }
    [data-testid="stSidebar"] { background: #edf3fa; border-right: 1px solid rgba(75,94,122,.12); }
    [data-testid="stMetric"] {
        background: rgba(255,255,255,.96);
        border: 1px solid rgba(75,94,122,.12); border-radius: 16px; padding: 18px;
        box-shadow: 0 10px 28px rgba(38,61,94,.08);
    }
    [data-testid="stMetricValue"] { color: #14213a; font-size: 1.75rem; }
    [data-testid="stMetricLabel"] { color: #5f6f86; }
    .hero {
        padding: 1.35rem 1.55rem; border-radius: 20px; margin-bottom: 1rem;
        color: #fff;
        background: linear-gradient(120deg, #1769e0, #159f91);
        border: 1px solid rgba(23,105,224,.16); box-shadow: 0 16px 38px rgba(30,93,167,.16);
    }
    .hero h1 { margin: 0; font-size: 2rem; letter-spacing: -.035em; }
    .hero p { margin: .4rem 0 0; color: rgba(255,255,255,.84); }
    .eyebrow { color: #c9fff1; font-size: .76rem; font-weight: 700; letter-spacing: .16em; }
    .status-row { display:flex; gap:.55rem; flex-wrap:wrap; margin-top:.85rem; }
    .pill { border:1px solid rgba(255,255,255,.32); background:rgba(255,255,255,.13);
            color:#fff; padding:.28rem .65rem; border-radius:999px; font-size:.78rem; }
    .section-title { font-size: 1.15rem; font-weight: 700; margin: .9rem 0 .25rem; }
    .section-note { color:#66758b; font-size:.88rem; margin-bottom:.7rem; }
    .health-card { padding:1rem; border-radius:14px; background:var(--panel);
                   border:1px solid rgba(75,94,122,.12); min-height:112px;
                   box-shadow:0 9px 24px rgba(38,61,94,.07); }
    .health-card strong { font-size:1.25rem; }
    .flow-step { padding:.8rem .9rem; border-radius:12px; background:#fff;
                 border:1px solid rgba(75,94,122,.12); text-align:center;
                 box-shadow:0 7px 20px rgba(38,61,94,.06); }
    .flow-step b { color:#1769e0; }
    .result-panel { padding:1rem 1.1rem; border-radius:14px; background:#fff;
                    border:1px solid rgba(75,94,122,.12);
                    box-shadow:0 8px 22px rgba(38,61,94,.06); }
    .result-panel h4 { margin:0 0 .45rem; color:#172033; }
    .result-panel p { margin:.2rem 0; color:#5f6f86; }
    .signal { padding:.65rem .8rem; margin:.35rem 0; border-radius:10px;
              background:#f7f9fc; border-left:4px solid #1769e0; }
    .ok { color:#079b72; } .warn { color:#c17a00; } .bad { color:#d9435f; }
    hr { border-color: rgba(75,94,122,.12) !important; }
    div[data-testid="stDataFrame"] { border:1px solid rgba(75,94,122,.12); border-radius:12px; }
    #MainMenu, footer { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)


def fetch_frame(sql: str, params: tuple | None = None) -> pd.DataFrame:
    with closing(psycopg2.connect(**POSTGRES)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [item.name for item in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=columns)


@st.cache_data(ttl=15, show_spinner=False)
def load_data() -> dict[str, pd.DataFrame]:
    return {
        "overview": fetch_frame(queries.OVERVIEW),
        "monthly": fetch_frame(queries.MONTHLY_TREND),
        "providers": fetch_frame(queries.PROVIDER_PERFORMANCE),
        "categories": fetch_frame(queries.CATEGORY_PERFORMANCE),
        "states": fetch_frame(queries.STATE_PERFORMANCE),
        "credit": fetch_frame(queries.CREDIT_BANDS),
        "metrics": fetch_frame(queries.MODEL_METRICS),
        "prediction_summary": fetch_frame(queries.PREDICTION_SUMMARY),
        "prediction_kpis": fetch_frame(queries.PREDICTION_KPIS),
        "recent_predictions": fetch_frame(queries.RECENT_PREDICTIONS),
        "pipeline": fetch_frame(queries.PIPELINE_BATCHES),
        "quality": fetch_frame(queries.DATA_QUALITY),
        "airflow": fetch_frame(queries.AIRFLOW_RUN),
    }


@st.cache_data(ttl=15, show_spinner=False)
def load_hdfs_health() -> dict[str, object]:
    response = requests.get(
        HDFS_JMX_URL,
        params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"},
        timeout=3,
    )
    response.raise_for_status()
    bean = response.json()["beans"][0]
    return {
        "live_nodes": int(bean.get("NumLiveDataNodes", 0)),
        "dead_nodes": int(bean.get("NumDeadDataNodes", 0)),
        "capacity_used": float(bean.get("CapacityUsed", 0)),
        "capacity_total": float(bean.get("CapacityTotal", 0)),
    }


@st.cache_data(ttl=15, show_spinner=False)
def load_spark_health() -> dict[str, object]:
    response = requests.get(f"{SPARK_MASTER_URL.rstrip('/')}/json/", timeout=3)
    response.raise_for_status()
    body = response.json()
    return {
        "workers": int(body.get("aliveworkers", 0)),
        "cores": int(body.get("cores", 0)),
        "cores_used": int(body.get("coresused", 0)),
        "active_apps": len(body.get("activeapps", [])),
        "active_app_names": [app.get("name", "") for app in body.get("activeapps", [])],
        "status": body.get("status", "UNKNOWN"),
    }


def tcp_ok(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def send_demo_event(event: dict[str, object]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        request_timeout_ms=5_000,
    )
    try:
        producer.send(KAFKA_TOPIC, key=str(event["transaction_id"]), value=event).get(timeout=10)
        producer.flush(timeout=10)
    finally:
        producer.close(timeout=5)


def demo_prediction(transaction_id: str) -> pd.DataFrame:
    return fetch_frame(
        """
        SELECT prediction_horizon, model_name, model_version, predicted_default,
            default_probability::double precision AS default_probability,
            risk_level, prediction_timestamp
        FROM ml.predictions
        WHERE transaction_id = %s
        ORDER BY prediction_horizon
        """,
        (transaction_id,),
    )


def money(value: float) -> str:
    value = float(value or 0)
    if value >= 1_000_000_000:
        return f"₦{value / 1_000_000_000:,.2f}B"
    if value >= 1_000_000:
        return f"₦{value / 1_000_000:,.1f}M"
    return f"₦{value:,.0f}"


def plot_style(figure, height: int = 360):
    figure.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=34, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=INK, family="Inter, sans-serif"),
        legend=dict(orientation="h", y=1.12, x=0),
        xaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
        yaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    )
    return figure


def hero(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="hero">
          <div class="eyebrow">BNPL BIG DATA PLATFORM · V2</div>
          <h1>{title}</h1><p>{subtitle}</p>
          <div class="status-row">
            <span class="pill">HDFS · 3 DataNodes</span>
            <span class="pill">Spark · Delta Lake</span>
            <span class="pill">Kafka · KRaft</span>
            <span class="pill">MLlib · Streaming</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section(title: str, note: str) -> None:
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="section-note">{note}</div>', unsafe_allow_html=True)


def overview_page(data: dict[str, pd.DataFrame]) -> None:
    hero("Executive Overview", "Từ raw event đến quyết định rủi ro trong một màn hình điều hành.")
    row = data["overview"].iloc[0]
    columns = st.columns(6)
    columns[0].metric("Giao dịch", f"{int(row.total_transactions):,}")
    columns[1].metric("Tổng giải ngân", money(row.total_bnpl_amount))
    columns[2].metric("Khoản vay TB", money(row.average_loan))
    columns[3].metric("Khách hàng", f"{int(row.total_customers):,}")
    columns[4].metric("Default 30D", f"{row.default_30d_rate:.2%}")
    columns[5].metric("Default 90D", f"{row.default_90d_rate:.2%}")

    section("Tăng trưởng danh mục", "Số lượng giao dịch và giá trị giải ngân theo tháng.")
    monthly = data["monthly"]
    trend = go.Figure()
    trend.add_trace(go.Bar(x=monthly["month"], y=monthly["principal_ngn"], name="Giải ngân", marker_color=BLUE))
    trend.add_trace(go.Scatter(x=monthly["month"], y=monthly["transactions"], name="Giao dịch", mode="lines+markers", yaxis="y2", line=dict(color=ACCENT, width=3)))
    trend.update_layout(yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Giao dịch"), yaxis_title="NGN")
    st.plotly_chart(plot_style(trend, 390), use_container_width=True, config={"displayModeBar": False})

    left, right = st.columns(2)
    with left:
        providers = data["providers"].sort_values("principal_ngn")
        chart = px.bar(providers, x="principal_ngn", y="provider_name", orientation="h", color="default_rate", color_continuous_scale=["#22d3a7", "#f7b955", "#ff647c"], labels={"principal_ngn": "Giải ngân", "provider_name": "", "default_rate": "Default"})
        st.plotly_chart(plot_style(chart), use_container_width=True, config={"displayModeBar": False})
    with right:
        categories = data["categories"]
        chart = px.treemap(categories, path=["merchant_category"], values="transactions", color="default_rate", color_continuous_scale=["#22d3a7", "#f7b955", "#ff647c"])
        chart.update_traces(textinfo="label+value")
        st.plotly_chart(plot_style(chart), use_container_width=True, config={"displayModeBar": False})


def portfolio_page(data: dict[str, pd.DataFrame]) -> None:
    hero("Portfolio Intelligence", "Khám phá hiệu suất theo nhà cung cấp, địa lý và chất lượng tín dụng.")
    left, right = st.columns(2)
    with left:
        section("Provider performance", "Quy mô bong bóng biểu diễn số giao dịch.")
        providers = data["providers"]
        chart = px.scatter(providers, x="average_loan", y="default_rate", size="transactions", color="provider_name", hover_name="provider_name", labels={"average_loan": "Khoản vay trung bình", "default_rate": "Default 90D", "provider_name": "Provider"})
        chart.update_yaxes(tickformat=".1%")
        st.plotly_chart(plot_style(chart), use_container_width=True, config={"displayModeBar": False})
    with right:
        section("Credit risk curve", "Tỷ lệ default theo nhóm điểm tín dụng.")
        credit = data["credit"]
        chart = px.bar(credit, x="credit_score_band", y="default_rate", color="default_rate", color_continuous_scale=["#22d3a7", "#f7b955", "#ff647c"], labels={"credit_score_band": "Credit band", "default_rate": "Default 90D"})
        chart.update_yaxes(tickformat=".1%")
        st.plotly_chart(plot_style(chart), use_container_width=True, config={"displayModeBar": False})

    section("Geographic exposure", "Top bang theo tổng giá trị giải ngân và tỷ lệ default.")
    states = data["states"].copy()
    states["default_rate_label"] = states["default_rate"].map(lambda value: f"{value:.1%}")
    st.dataframe(
        states,
        use_container_width=True,
        hide_index=True,
        column_config={
            "customer_state": "Bang",
            "transactions": st.column_config.NumberColumn("Giao dịch", format="%d"),
            "principal_ngn": st.column_config.NumberColumn("Giải ngân (₦)", format="%.0f"),
            "default_rate": st.column_config.ProgressColumn("Default 90D", min_value=0.0, max_value=max(float(states.default_rate.max()), 0.01), format="%.2f"),
            "default_rate_label": "Tỷ lệ",
        },
    )


def ml_page(data: dict[str, pd.DataFrame]) -> None:
    hero("ML Risk Center", "So sánh model và quan sát quyết định inference theo thời gian gần thực.")
    kpi = data["prediction_kpis"].iloc[0]
    metrics = data["metrics"]
    latest = kpi.latest_prediction
    columns = st.columns(4)
    columns[0].metric("Predictions", f"{int(kpi.total_predictions):,}")
    columns[1].metric("High risk", f"{int(kpi.high_risk):,}")
    columns[2].metric("Xác suất TB", f"{float(kpi.average_probability or 0):.1%}")
    columns[3].metric("Model champion", "GBT", help="Chọn theo ROC-AUC trong từng prediction horizon")

    section("Model leaderboard", "ROC-AUC phù hợp hơn accuracy khi nhãn default mất cân bằng.")
    leaderboard = metrics.melt(
        id_vars=["model_name", "prediction_horizon"],
        value_vars=["roc_auc", "recall", "precision"],
        var_name="metric",
        value_name="score",
    )
    chart = px.bar(leaderboard, x="model_name", y="score", color="metric", facet_col="prediction_horizon", barmode="group", color_discrete_map={"roc_auc": ACCENT, "recall": AMBER, "precision": BLUE}, labels={"model_name": "Model", "score": "Score", "metric": "Metric"})
    chart.update_yaxes(range=[0, 1], tickformat=".0%")
    st.plotly_chart(plot_style(chart, 390), use_container_width=True, config={"displayModeBar": False})

    left, right = st.columns([1, 1.35])
    with left:
        section("Risk distribution", "LOW < 40% · MEDIUM 40–70% · HIGH ≥ 70%.")
        summary = data["prediction_summary"]
        chart = px.bar(summary, x="prediction_horizon", y="predictions", color="risk_level", barmode="stack", color_discrete_map={"LOW": ACCENT, "MEDIUM": AMBER, "HIGH": RED}, category_orders={"risk_level": ["LOW", "MEDIUM", "HIGH"]}, labels={"prediction_horizon": "Horizon", "predictions": "Predictions", "risk_level": "Risk"})
        st.plotly_chart(plot_style(chart), use_container_width=True, config={"displayModeBar": False})
    with right:
        section("Live prediction feed", f"Bản ghi mới nhất: {latest if pd.notna(latest) else 'chưa có'}")
        recent = data["recent_predictions"].copy()
        recent["default_probability"] = recent["default_probability"].map(lambda value: f"{value:.1%}")
        st.dataframe(recent.head(12), use_container_width=True, hide_index=True, height=335)

    with st.expander("Xem metrics chi tiết"):
        st.dataframe(metrics, use_container_width=True, hide_index=True)


PREDICTION_PRESETS = {
    "Hồ sơ cân bằng": {"principal": 120_000, "credit": 640, "rate": 2.5, "tenor": 180, "installments": 6, "first_time": True},
    "Rủi ro thấp": {"principal": 45_000, "credit": 805, "rate": 1.2, "tenor": 90, "installments": 3, "first_time": False},
    "Rủi ro cao": {"principal": 430_000, "credit": 420, "rate": 5.5, "tenor": 360, "installments": 12, "first_time": True},
}


def prediction_gauge(row: pd.Series):
    probability = float(row.default_probability) * 100
    figure = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=probability,
            number={"suffix": "%", "font": {"size": 34}},
            title={"text": f"Default {row.prediction_horizon}"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": RED if probability >= 70 else AMBER if probability >= 40 else ACCENT},
                "steps": [
                    {"range": [0, 40], "color": "rgba(34,211,167,.18)"},
                    {"range": [40, 70], "color": "rgba(247,185,85,.22)"},
                    {"range": [70, 100], "color": "rgba(255,100,124,.20)"},
                ],
                "threshold": {"line": {"color": "#172033", "width": 3}, "value": probability},
            },
        )
    )
    return plot_style(figure, 280)


def business_risk_signals(event: dict[str, object]) -> list[tuple[str, str]]:
    """Return transparent business heuristics, not model feature attribution."""

    signals: list[tuple[str, str]] = []
    credit_score = int(event["credit_score"])
    if credit_score < 580:
        signals.append(("Credit score thấp", f"{credit_score} thuộc nhóm Poor (< 580)."))
    elif credit_score < 670:
        signals.append(("Credit score cần theo dõi", f"{credit_score} thuộc nhóm Fair."))
    else:
        signals.append(("Credit score tích cực", f"{credit_score} từ Good trở lên."))

    principal = float(event["principal_ngn"])
    signals.append(
        (
            "Quy mô khoản vay",
            f"{money(principal)} — "
            + ("nhóm Large." if principal >= 200_000 else "nhóm Medium." if principal >= 50_000 else "nhóm Small."),
        )
    )
    if int(event["tenor_days"]) >= 270:
        signals.append(("Kỳ hạn dài", f"{event['tenor_days']} ngày làm tăng thời gian phơi nhiễm rủi ro."))
    if float(event["interest_rate_monthly"]) >= 4:
        signals.append(("Mức lãi suất cao", f"Giá trị đầu vào {event['interest_rate_monthly']} mỗi tháng."))
    if bool(event["first_time_customer"]):
        signals.append(("Khách hàng lần đầu", "Chưa có lịch sử giao dịch nội bộ trong hồ sơ demo."))
    return signals


def render_prediction_results(
    scored: pd.DataFrame,
    transaction_id: str,
    event: dict[str, object] | None = None,
) -> None:
    st.success(f"Spark MLlib đã trả về {len(scored)} kết quả cho `{transaction_id}`.")
    scored = scored.sort_values("prediction_horizon").reset_index(drop=True)
    maximum = scored.loc[scored["default_probability"].idxmax()]
    overview = st.columns(4)
    overview[0].metric("Rủi ro cao nhất", str(maximum.risk_level))
    overview[1].metric("Xác suất cao nhất", f"{float(maximum.default_probability):.1%}")
    overview[2].metric("Horizon cảnh báo", str(maximum.prediction_horizon))
    overview[3].metric("Serving model", str(maximum.model_name).replace("_", " ").title())

    result_columns = st.columns(max(len(scored), 1))
    for container, (_, row) in zip(result_columns, scored.iterrows()):
        with container:
            st.plotly_chart(prediction_gauge(row), use_container_width=True, config={"displayModeBar": False})
            recommendation = {
                "LOW": "Có thể tiếp tục quy trình duyệt",
                "MEDIUM": "Yêu cầu kiểm tra bổ sung",
                "HIGH": "Chuyển sang thẩm định thủ công",
            }.get(str(row.risk_level), "Cần xem xét")
            st.metric("Risk level", str(row.risk_level), help=recommendation)
            decision = "Có" if bool(row.predicted_default) else "Không"
            st.markdown(
                f"""
                <div class="result-panel">
                  <h4>Quyết định {row.prediction_horizon}</h4>
                  <p><b>Predicted default:</b> {decision}</p>
                  <p><b>Model:</b> {row.model_name} · {row.model_version}</p>
                  <p><b>Khuyến nghị:</b> {recommendation}</p>
                  <p><b>Thời điểm:</b> {row.prediction_timestamp}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

    if len(scored) >= 2:
        probability_by_horizon = scored.set_index("prediction_horizon")["default_probability"]
        if "30D" in probability_by_horizon and "90D" in probability_by_horizon:
            difference = float(probability_by_horizon["90D"] - probability_by_horizon["30D"])
            st.info(
                f"Xác suất 90D {'cao hơn' if difference >= 0 else 'thấp hơn'} 30D "
                f"{abs(difference):.1%}."
            )

    detail = scored[
        [
            "prediction_horizon",
            "default_probability",
            "risk_level",
            "predicted_default",
            "model_name",
            "model_version",
            "prediction_timestamp",
        ]
    ].copy()
    detail["default_probability"] = detail["default_probability"].map(lambda value: f"{value:.2%}")
    detail["predicted_default"] = detail["predicted_default"].map({True: "YES", False: "NO"})
    with st.expander("Chi tiết kỹ thuật của prediction", expanded=False):
        st.dataframe(detail, use_container_width=True, hide_index=True)

    if event:
        section("Hồ sơ được chấm điểm", "Tóm tắt đúng payload đã gửi qua Kafka.")
        principal = float(event["principal_ngn"])
        profile = st.columns(4)
        profile[0].metric("Khoản vay", money(principal))
        profile[1].metric("Credit score", f"{int(event['credit_score'])}")
        profile[2].metric("Kỳ hạn", f"{int(event['tenor_days'])} ngày")
        profile[3].metric("Lãi suất/tháng", f"{float(event['interest_rate_monthly']):.2f}%")

        left, right = st.columns(2)
        with left:
            st.markdown(
                f"""
                <div class="result-panel">
                  <h4>Thông tin giao dịch</h4>
                  <p><b>Customer:</b> {event['customer_id']}</p>
                  <p><b>Provider:</b> {event['provider']}</p>
                  <p><b>Category:</b> {event['merchant_category']}</p>
                  <p><b>State:</b> {event['customer_state']}</p>
                  <p><b>First-time:</b> {'Có' if event['first_time_customer'] else 'Không'}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with right:
            st.markdown("#### Tín hiệu nghiệp vụ")
            for title, explanation in business_risk_signals(event):
                st.markdown(
                    f'<div class="signal"><b>{title}</b><br>{explanation}</div>',
                    unsafe_allow_html=True,
                )
            st.caption(
                "Các tín hiệu trên là rule giải thích nghiệp vụ để thuyết trình, "
                "không phải SHAP hoặc mức đóng góp nhân quả của model."
            )


def prediction_page() -> None:
    hero("Live Risk Prediction", "Nhập một hồ sơ và xem Spark MLlib dự báo default 30D/90D qua pipeline thật.")
    kafka_host, kafka_port = KAFKA_BOOTSTRAP_SERVERS.split(":", 1)
    kafka_online = tcp_ok(kafka_host, int(kafka_port))
    postgres_online = tcp_ok(POSTGRES["host"], POSTGRES["port"])
    try:
        spark = load_spark_health()
        spark_online = (
            int(spark["workers"]) > 0
            and "bnpl-streaming-prediction" in spark["active_app_names"]
        )
    except Exception:
        spark_online = False

    flow = st.columns(4)
    labels = [
        ("1", "Nhập hồ sơ", True),
        ("2", "Kafka event", kafka_online),
        ("3", "Spark MLlib", spark_online),
        ("4", "Prediction", postgres_online),
    ]
    for column, (number, label, healthy) in zip(flow, labels):
        with column:
            state = "✓ sẵn sàng" if healthy else "chưa sẵn sàng"
            css = "ok" if healthy else "warn"
            st.markdown(
                f'<div class="flow-step"><b>{number}. {label}</b><br><span class="{css}">{state}</span></div>',
                unsafe_allow_html=True,
            )

    if not kafka_online or not spark_online:
        st.warning(
            "Live pipeline chưa đủ service. Chạy `docker compose --profile demo "
            "--profile streaming --profile inference up -d`, rồi tải lại trang."
        )

    st.markdown("")
    preset_name = st.selectbox("Kịch bản demo", list(PREDICTION_PRESETS), index=0)
    preset = PREDICTION_PRESETS[preset_name]
    with st.form("dedicated_prediction_form"):
        section("Thông tin khoản vay", "Các trường này được đóng gói thành event theo Kafka schema của hệ thống.")
        form_a, form_b, form_c = st.columns(3)
        with form_a:
            principal = st.number_input("Giá trị khoản vay (₦)", 5_000, 500_000, preset["principal"], 5_000)
            credit_score = st.slider("Credit score", 300, 850, preset["credit"])
            customer_state = st.selectbox("Bang", ["Lagos", "Abuja", "Kano", "Rivers", "Oyo", "Kaduna"])
        with form_b:
            interest_rate = st.number_input("Lãi suất/tháng (%)", 0.0, 10.0, preset["rate"], 0.1)
            tenors = [30, 60, 90, 180, 270, 360]
            tenor_days = st.selectbox("Kỳ hạn (ngày)", tenors, index=tenors.index(preset["tenor"]))
            installments = st.number_input("Số kỳ trả", 1, 12, preset["installments"])
        with form_c:
            provider = st.selectbox("Provider", ["Carbon", "Fairmoney", "Branch", "Aella", "Renmoney"])
            category = st.selectbox("Danh mục", ["electronics", "fashion", "groceries", "health", "home", "education", "travel", "services"])
            first_time = st.toggle("Khách hàng lần đầu", value=preset["first_time"])
        submitted = st.form_submit_button(
            "⚡ Gửi hồ sơ và dự báo ngay",
            use_container_width=True,
            type="primary",
            disabled=not (kafka_online and postgres_online),
        )

    if submitted:
        transaction_id = f"DEMO_{datetime.now(timezone.utc):%Y%m%d%H%M%S}_{uuid4().hex[:6]}"
        event = {
            "transaction_id": transaction_id,
            "purchase_date": date.today().isoformat(),
            "customer_id": f"DEMO_CU_{uuid4().hex[:8]}",
            "merchant_name": "Presentation Demo Merchant",
            "merchant_category": category,
            "customer_state": customer_state,
            "principal_ngn": float(principal),
            "interest_rate_monthly": float(interest_rate),
            "tenor_days": int(tenor_days),
            "num_installments": int(installments),
            "provider": provider,
            "credit_score": int(credit_score),
            "first_time_customer": bool(first_time),
        }
        scored = pd.DataFrame()
        with st.status("Đang gửi hồ sơ vào pipeline...", expanded=True) as status:
            try:
                send_demo_event(event)
                status.write(f"Kafka đã nhận event `{transaction_id}`.")
                status.write("Đang chờ Spark Structured Streaming xử lý...")
                deadline = time.monotonic() + 55
                while time.monotonic() < deadline:
                    scored = demo_prediction(transaction_id)
                    if len(scored) >= 2:
                        break
                    time.sleep(2)
                if len(scored) >= 2:
                    status.update(label="Dự báo hoàn tất", state="complete", expanded=False)
                else:
                    status.update(
                        label="Đã gửi hồ sơ — kết quả chưa sẵn sàng",
                        state="complete",
                        expanded=False,
                    )
            except Exception as error:
                status.update(label="Không thể hoàn tất dự báo", state="error", expanded=True)
                st.error(str(error))

        st.session_state["prediction_transaction_id"] = transaction_id
        st.session_state["prediction_rows"] = scored.to_dict("records")
        st.session_state["prediction_event"] = event

    transaction_id = st.session_state.get("prediction_transaction_id")
    stored_rows = st.session_state.get("prediction_rows", [])
    stored_event = st.session_state.get("prediction_event")
    if transaction_id and stored_rows:
        render_prediction_results(pd.DataFrame(stored_rows), transaction_id, stored_event)
    elif transaction_id:
        st.info(f"Chưa thấy kết quả cho `{transaction_id}`. Spark có thể đang bận xử lý micro-batch.")
        if st.button("↻ Kiểm tra lại kết quả", use_container_width=True):
            refreshed = demo_prediction(transaction_id)
            if refreshed.empty:
                st.warning("Kết quả chưa sẵn sàng, thử lại sau vài giây.")
            else:
                st.session_state["prediction_rows"] = refreshed.to_dict("records")
                st.rerun()


def health_card(title: str, value: str, detail: str, status: str = "ok") -> None:
    st.markdown(
        f'<div class="health-card"><span class="{status}">● {title}</span><br>'
        f'<strong>{value}</strong><br><small>{detail}</small></div>',
        unsafe_allow_html=True,
    )


def platform_page(data: dict[str, pd.DataFrame]) -> None:
    hero("Platform Health", "Một điểm nhìn cho HDFS, Spark, Kafka, Airflow và chất lượng dữ liệu.")
    try:
        hdfs = load_hdfs_health()
    except Exception:
        hdfs = {"live_nodes": 0, "dead_nodes": 0, "capacity_used": 0, "capacity_total": 0}
    try:
        spark = load_spark_health()
    except Exception:
        spark = {"workers": 0, "cores": 0, "cores_used": 0, "active_apps": 0, "status": "UNREACHABLE"}
    kafka = tcp_ok("kafka", 9092)
    airflow = data["airflow"]
    airflow_state = str(airflow.iloc[0].state).upper() if not airflow.empty else "NO RUN"

    columns = st.columns(4)
    with columns[0]:
        health_card("HDFS", f"{hdfs['live_nodes']} / 3 DataNodes", f"Dead nodes: {hdfs['dead_nodes']}", "ok" if hdfs["live_nodes"] == 3 else "bad")
    with columns[1]:
        health_card("Spark", f"{spark['workers']} workers", f"Cores {spark['cores_used']}/{spark['cores']} · Apps {spark['active_apps']}", "ok" if spark["workers"] > 0 else "bad")
    with columns[2]:
        health_card("Kafka KRaft", "ONLINE" if kafka else "OFFLINE", "bnpl.transactions.raw", "ok" if kafka else "warn")
    with columns[3]:
        health_card("Airflow", airflow_state, "bnpl_batch_pipeline", "ok" if airflow_state == "SUCCESS" else "warn")

    left, right = st.columns([1.25, 1])
    with left:
        section("Medallion pipeline", "Trạng thái xử lý của từng batch dữ liệu.")
        st.dataframe(data["pipeline"], use_container_width=True, hide_index=True, height=350)
    with right:
        section("Data quality gate", "Metrics mới nhất được Spark ghi nhận.")
        quality = data["quality"]
        chart = px.bar(quality.head(15), x="metric_value", y="metric_name", orientation="h", color="layer", labels={"metric_value": "Value", "metric_name": "Metric", "layer": "Layer"})
        st.plotly_chart(plot_style(chart, 350), use_container_width=True, config={"displayModeBar": False})

    section("Kiến trúc đang demo", "Serving UI không truy cập raw data; mọi truy vấn nghiệp vụ đi qua PostgreSQL.")
    st.code(
        "Kafka / Hugging Face  →  HDFS Bronze  →  Spark + Delta Silver/Gold\n"
        "                                             ↓\n"
        "                               PostgreSQL Serving  →  Streamlit",
        language="text",
    )


with st.sidebar:
    st.markdown("### ◈ BNPL Command Center")
    st.caption("Enterprise data platform demo")
    page = st.radio(
        "Điều hướng",
        [
            "Executive Overview",
            "Portfolio Intelligence",
            "ML Risk Center",
            "Live Risk Prediction",
            "Platform Health",
        ],
        label_visibility="collapsed",
    )
    st.divider()
    if st.button("↻ Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Serving source: PostgreSQL · Cache: 15 giây")
    st.caption(datetime.now(timezone.utc).strftime("UTC %H:%M · %d/%m/%Y"))

try:
    dashboard_data = load_data()
except Exception as error:
    st.error("Không thể kết nối PostgreSQL serving layer.")
    st.exception(error)
    st.stop()

if page == "Executive Overview":
    overview_page(dashboard_data)
elif page == "Portfolio Intelligence":
    portfolio_page(dashboard_data)
elif page == "ML Risk Center":
    ml_page(dashboard_data)
elif page == "Live Risk Prediction":
    prediction_page()
else:
    platform_page(dashboard_data)
