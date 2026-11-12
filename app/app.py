import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
from databricks import sql

st.set_page_config(page_title="How clean is India's air?", page_icon="🌫️", layout="wide")

# Bigger, readable chart text everywhere
pio.templates["aqi"] = go.layout.Template(layout=dict(
    font=dict(size=15, color="#1B2631"),
    title=dict(font=dict(size=19)),
))
pio.templates.default = "plotly_white+aqi"

SAFE_LIMIT = 60  # India's 24-hour safe limit for PM2.5 (µg/m³)
PIPELINE_LIVE_SINCE = pd.Timestamp("2026-11-06", tz="UTC")  # after setup and testing

# Official CPCB categories for PM2.5, with plain-language meaning and advice
LEVELS = [
    {"name": "Good", "low": 0, "high": 30, "color": "#00B050", "emoji": "😊",
     "meaning": "The air is clean.",
     "advice": "Enjoy outdoor activities as usual."},
    {"name": "Satisfactory", "low": 30, "high": 60, "color": "#92D050", "emoji": "🙂",
     "meaning": "The air is acceptable for most people.",
     "advice": "Very sensitive people may feel slight breathing discomfort."},
    {"name": "Moderate", "low": 60, "high": 90, "color": "#E6B800", "emoji": "😐",
     "meaning": "The air is above the safe limit.",
     "advice": "People with asthma, lung or heart problems, children and older adults "
               "should cut down on long or heavy outdoor exercise."},
    {"name": "Poor", "low": 90, "high": 120, "color": "#FF9900", "emoji": "😷",
     "meaning": "Most people may feel breathing discomfort if outdoors for long.",
     "advice": "Limit long outdoor activities. Sensitive groups should stay indoors more."},
    {"name": "Very Poor", "low": 120, "high": 250, "color": "#E8282B", "emoji": "😨",
     "meaning": "Breathing this air for long periods can cause breathing illness.",
     "advice": "Avoid outdoor exercise. Wear a well-fitting N95 mask if you must go out."},
    {"name": "Severe", "low": 250, "high": 100_000, "color": "#8B0000", "emoji": "🚨",
     "meaning": "The air affects even healthy people, and seriously affects people who are ill.",
     "advice": "Stay indoors as much as possible and keep windows closed."},
]
COLORS = {lvl["name"]: lvl["color"] for lvl in LEVELS}

# Plain names for the pipeline's quality checks
CHECK_NAMES = {
    "row_count": "Enough readings arrived",
    "missing_avg_pct": "Few stations missing values",
    "negative_values": "No impossible (negative) values",
    "unknown_pollutant": "All pollutant names recognised",
    "duplicate_readings": "No duplicate readings",
    "data_age_hours": "Data is fresh (under 3 hours old)",
}

# ---------- Styling ----------
st.markdown("""
<style>
.block-container {padding-top: 3.5rem; max-width: 1200px;}
h1, h2, h3 {color: #1F3A5F;}
.status-card {border-radius: 18px; padding: 28px 32px; color: white;
              box-shadow: 0 6px 20px rgba(0,0,0,0.12); margin-bottom: 8px;}
.status-card .city {font-size: 1.1rem; opacity: 0.9;}
.status-card .level {font-size: 2.6rem; font-weight: 700; line-height: 1.1;}
.status-card .value {font-size: 1.2rem; margin-top: 6px;}
.status-card .meaning {font-size: 1.1rem; margin-top: 14px;}
.info-box {background: #FFFFFF; border-radius: 14px; padding: 18px 22px;
           border: 1px solid #E3E8EF; height: 100%;}
.info-box .label {color: #5D6D7E; font-size: 0.9rem;}
.info-box .big {font-size: 1.6rem; font-weight: 700; color: #1F3A5F;}
.small-note {color: #5D6D7E; font-size: 0.9rem;}
</style>
""", unsafe_allow_html=True)


# ---------- Data access ----------
@st.cache_resource(show_spinner=False)
def get_connection():
    """Open ONE Databricks connection and reuse it for every query (much faster).

    Retries patiently because a sleeping serverless warehouse can take a minute or two
    to start after a quiet period.
    """
    last_error = None
    for attempt in range(1, 4):
        try:
            return sql.connect(
                server_hostname=st.secrets["DATABRICKS_HOST"],
                http_path=st.secrets["DATABRICKS_HTTP_PATH"],
                access_token=st.secrets["DATABRICKS_TOKEN"],
                _retry_stop_after_attempts_duration=180,  # wait up to 3 minutes per attempt
            )
        except Exception as error:
            last_error = error
            print(f"[connection error] attempt {attempt}: {type(error).__name__}: {str(error)[:300]}")
            time.sleep(10 * attempt)
    raise last_error


def _fetch(query, params):
    with get_connection().cursor() as cursor:
        cursor.execute(query, params or {})
        rows = cursor.fetchall()
        columns = [c[0] for c in cursor.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=1800, show_spinner="Getting the latest data...")
def run_query(query, params=None):
    """Run a SQL query on Databricks and return a pandas DataFrame (cached 30 min)."""
    try:
        return _fetch(query, params)
    except Exception:
        # The saved connection may have timed out: open a fresh one and retry once
        get_connection.clear()
        return _fetch(query, params)


def level_for(pm25):
    """Return the CPCB level (dictionary) for a PM2.5 value, or None if missing."""
    if pd.isna(pm25):
        return None
    for lvl in LEVELS:
        if lvl["low"] <= pm25 < lvl["high"]:
            return lvl
    return LEVELS[-1]


def as_ist(series):
    """Live times are IST but stored with a UTC label; drop the label."""
    return pd.to_datetime(series).dt.tz_localize(None)


def times_limit(pm25):
    """Describe a PM2.5 value against the safe limit in plain words."""
    ratio = pm25 / SAFE_LIMIT
    if ratio <= 1:
        return "Within the safe limit"
    return f"{ratio:.1f}× the safe limit"


@st.cache_data(ttl=10800, show_spinner="Writing today's briefing...")
def ai_briefing(facts, reading_label):
    """Ask Gemini to turn pre-computed facts into a short plain-English briefing.

    Cached for 3 hours per reading time, so the API is called at most once per update.
    reading_label is part of the cache key: a new data update gives a new briefing.
    """
    from google import genai

    client = genai.Client(api_key=st.secrets["GEMINI_API_KEY"])
    prompt = (
        "You write a short daily air-quality briefing for ordinary people in India.\n"
        "Use ONLY the facts below. Do not add numbers, causes, places, or predictions "
        "that are not listed. Write 4 short sentences in simple, calm language, "
        "no bullet points, no headings. End with one practical tip for the public.\n\n"
        f"FACTS:\n{facts}"
    )
    response = client.models.generate_content(
        model=st.secrets.get("GEMINI_MODEL", "gemini-3.5-flash-lite"),
        contents=prompt,
    )
    return response.text.strip()


def build_facts(live_df, reading_time):
    """Collect the key numbers the briefing is allowed to use."""
    reliable_df = live_df[live_df["stations_reporting"] >= 2]
    counts = live_df["level"].value_counts()
    worst = reliable_df.sort_values("pm25", ascending=False).head(5)
    cleanest = reliable_df.sort_values("pm25").head(3)
    lines = [
        f"- Reading time: {reading_time:%d %B %Y, %I:%M %p} IST",
        f"- Cities reporting: {live_df['city'].nunique()}",
        f"- Safe limit for fine dust (PM2.5): {SAFE_LIMIT} µg/m³",
        f"- Cities above the safe limit (cities with 2+ stations): "
        f"{int((reliable_df['pm25'] > SAFE_LIMIT).sum())} of {len(reliable_df)}",
        "- Number of cities in each category: "
        + ", ".join(f"{lvl['name']} {int(counts.get(lvl['name'], 0))}" for lvl in LEVELS),
        "- Most polluted cities (PM2.5 µg/m³): "
        + ", ".join(f"{r.city} {r.pm25:.0f}" for r in worst.itertuples()),
        "- Cleanest cities (PM2.5 µg/m³): "
        + ", ".join(f"{r.city} {r.pm25:.0f}" for r in cleanest.itertuples()),
    ]
    return "\n".join(lines)


def info_box(label, big, note=""):
    st.markdown(
        f'<div class="info-box"><div class="label">{label}</div>'
        f'<div class="big">{big}</div><div class="small-note">{note}</div></div>',
        unsafe_allow_html=True,
    )


# ---------- Load live data ----------
try:
    live = run_query("""
        SELECT city, state, reading_time, stations_reporting, pm25, pm10
        FROM workspace.aqi.gold_live_city_hourly
        WHERE reading_time = (SELECT MAX(reading_time) FROM workspace.aqi.gold_live_city_hourly)
    """)
except Exception as error:
    print(f"[data load error] {type(error).__name__}: {str(error)[:300]}")
    st.title("🌫️ How clean is India's air right now?")
    st.warning(
        "**The data service is waking up.** It goes to sleep when nobody has used it "
        "for a while, and can take a minute or two to start. Please try again shortly."
    )
    if st.button("🔄 Try again"):
        get_connection.clear()
        run_query.clear()
        st.rerun()
    st.stop()
live["reading_time"] = as_ist(live["reading_time"])
live = live[live["pm25"].notna()].copy()
live["level"] = live["pm25"].apply(lambda v: level_for(v)["name"])
latest = live["reading_time"].max()
now_ist = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
hours_old = max((now_ist - latest).total_seconds() / 3600, 0)

# ---------- Header ----------
st.title("🌫️ How clean is India's air right now?")
st.markdown(
    f"Live readings from **{int(live['stations_reporting'].sum())} government monitoring "
    f"stations** in **{live['city'].nunique()} cities**. "
    f"Last updated **{latest:%d %b, %I:%M %p}** "
    f"({hours_old:.0f} hour{'s' if round(hours_old) != 1 else ''} ago)."
)

# ---------- AI briefing (placeholder: filled in at the END, so the page loads first) ----------
briefing_slot = st.empty()

tab_today, tab_compare, tab_forecast, tab_trust = st.tabs(
    ["🏙️ My city today", "🗺️ Compare cities", "🔮 Can we warn in advance?", "✅ Can I trust this data?"]
)

# ---------- Tab 1: My city today ----------
with tab_today:
    cities = sorted(live["city"].unique())
    default = cities.index("Delhi") if "Delhi" in cities else 0
    city = st.selectbox("Choose your city", cities, index=default)

    row = live[live["city"] == city].iloc[0]
    lvl = level_for(row["pm25"])
    place = city if row["state"] == city else f"{city}, {row['state']}"

    left, right = st.columns([3, 2])
    with left:
        st.markdown(f"""
        <div class="status-card" style="background: {lvl['color']};">
            <div class="city">{place} · {row['reading_time']:%I:%M %p}</div>
            <div class="level">{lvl['emoji']} {lvl['name']}</div>
            <div class="value">Fine dust (PM2.5): <b>{row['pm25']:.0f} µg/m³</b> ·
                {times_limit(row['pm25'])}</div>
            <div class="meaning">{lvl['meaning']}</div>
        </div>
        """, unsafe_allow_html=True)
    with right:
        st.markdown("#### What should I do?")
        st.info(lvl["advice"])
        stations = int(row["stations_reporting"])
        st.markdown(
            f"<span class='small-note'>Based on the average of <b>{stations}</b> "
            f"monitoring station{'s' if stations != 1 else ''} in {city}."
            f"{' A single station may not represent the whole city.' if stations == 1 else ''}"
            f"</span>",
            unsafe_allow_html=True,
        )

    # Simple gauge: where this city sits on the scale
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=float(row["pm25"]),
        number={"suffix": " µg/m³"},
        gauge={
            "axis": {"range": [0, 400]},
            "bar": {"color": "#1F3A5F", "thickness": 0.25},
            "steps": [{"range": [l["low"], min(l["high"], 400)], "color": l["color"]}
                      for l in LEVELS],
            "threshold": {"line": {"color": "black", "width": 3}, "value": SAFE_LIMIT},
        },
        title={"text": "Where today's level sits (black line = safe limit)"},
    ))
    fig.update_layout(height=280, margin=dict(t=60, b=10, l=30, r=30))
    st.plotly_chart(fig, use_container_width=True)

    trend = run_query("""
        SELECT reading_time, pm25
        FROM workspace.aqi.gold_live_city_hourly
        WHERE city = :city AND pm25 IS NOT NULL
        ORDER BY reading_time
    """, {"city": city})
    if len(trend) > 1:
        trend["reading_time"] = as_ist(trend["reading_time"])
        fig = px.line(trend, x="reading_time", y="pm25", markers=True,
                      labels={"reading_time": "", "pm25": "Fine dust (µg/m³)"},
                      title=f"Is the air in {city} getting better or worse?")
        fig.add_hline(y=SAFE_LIMIT, line_dash="dash", line_color="#2E8B57",
                      annotation_text="Safe limit")
        fig.update_traces(line_color="#1F3A5F")
        fig.update_layout(height=320)
        st.plotly_chart(fig, use_container_width=True)

# ---------- Tab 2: Compare cities ----------
with tab_compare:
    reliable = live[live["stations_reporting"] >= 2]
    unsafe = int((reliable["pm25"] > SAFE_LIMIT).sum())

    c1, c2, c3 = st.columns(3)
    with c1:
        info_box("Cities with unsafe air right now",
                 f"{unsafe} of {len(reliable)}",
                 "Cities with 2 or more stations")
    with c2:
        worst = reliable.sort_values("pm25", ascending=False).iloc[0]
        info_box("Most polluted (2+ stations)", worst["city"],
                 f"{worst['pm25']:.0f} µg/m³ · {times_limit(worst['pm25'])}")
    with c3:
        best = reliable.sort_values("pm25").iloc[0]
        info_box("Cleanest (2+ stations)", best["city"],
                 f"{best['pm25']:.0f} µg/m³ · {times_limit(best['pm25'])}")

    st.markdown("### The 15 most polluted cities right now")
    top = reliable.sort_values("pm25", ascending=False).head(15).sort_values("pm25")
    fig = px.bar(top, x="pm25", y="city", orientation="h", color="level",
                 color_discrete_map=COLORS, text=top["pm25"].round(0),
                 labels={"pm25": "Fine dust, PM2.5 (µg/m³)", "city": "", "level": "Air quality"},
                 hover_data={"state": True, "stations_reporting": True})
    fig.add_vline(x=SAFE_LIMIT, line_dash="dash", line_color="#2E8B57",
                  annotation_text="Safe limit (60)")
    fig.update_traces(textposition="outside")
    fig.update_layout(height=560)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### How many cities are in each category?")
    counts = (live["level"].value_counts()
              .reindex([l["name"] for l in LEVELS]).fillna(0).reset_index())
    counts.columns = ["level", "cities"]
    fig = px.bar(counts, x="level", y="cities", color="level", color_discrete_map=COLORS,
                 text="cities", labels={"level": "", "cities": "Number of cities"})
    fig.update_layout(showlegend=False, height=360)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("See every city"):
        st.dataframe(
            live.sort_values("pm25", ascending=False)[
                ["city", "state", "level", "pm25", "stations_reporting"]
            ].rename(columns={"city": "City", "state": "State", "level": "Air quality",
                              "pm25": "PM2.5 (µg/m³)", "stations_reporting": "Stations"}),
            use_container_width=True, hide_index=True,
        )

# ---------- Tab 3: Forecast ----------
with tab_forecast:
    fc = run_query("""
        SELECT date, actual_aqi_tomorrow, baseline_pred, xgb_pred
        FROM workspace.aqi.gold_forecast_delhi_2019
        ORDER BY date
    """)
    fc["date"] = pd.to_datetime(fc["date"])
    bad = fc["actual_aqi_tomorrow"] > 200
    warned = int((bad & (fc["xgb_pred"] > 200)).sum())
    missed_model = int((bad & (fc["xgb_pred"] <= 200)).sum())
    missed_simple = int((bad & (fc["baseline_pred"] <= 200)).sum())

    st.markdown("### Can we warn people a day before the air turns bad?")
    st.markdown(
        "We trained a computer model on Delhi's air from 2015–2018, then tested it on "
        "**every day of 2019**, which it had never seen. Each evening, it predicts "
        "tomorrow's air quality."
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        info_box("Unhealthy days in 2019", f"{int(bad.sum())}", "Days with AQI above 200 (Poor or worse)")
    with c2:
        info_box("The model warned about", f"{warned} of {int(bad.sum())}",
                 f"That's {warned / bad.sum():.0%} of unhealthy days")
    with c3:
        info_box("Unhealthy days with no warning", f"{missed_model}",
                 f"vs {missed_simple} if we just assumed tomorrow = today")

    fig = go.Figure()
    fig.add_hrect(y0=200, y1=max(700, fc["actual_aqi_tomorrow"].max() + 20),
                  fillcolor="#E8282B", opacity=0.07, line_width=0,
                  annotation_text="Unhealthy zone", annotation_position="top left")
    fig.add_trace(go.Scatter(x=fc["date"], y=fc["actual_aqi_tomorrow"], name="What actually happened",
                             line=dict(color="#1F3A5F", width=2)))
    fig.add_trace(go.Scatter(x=fc["date"], y=fc["xgb_pred"], name="What the model predicted",
                             line=dict(color="#E07B39", width=2)))
    fig.update_layout(title="Predicted vs actual air quality, Delhi 2019", height=420,
                      yaxis_title="Air Quality Index (AQI)", xaxis_title="",
                      legend=dict(orientation="h", y=1.08))
    st.plotly_chart(fig, use_container_width=True)

    st.success(
        "**In plain words:** when the orange line climbs into the red zone, the model is warning "
        "that tomorrow will be unhealthy. It caught almost every bad day, giving people time to "
        "plan: keep children indoors, avoid outdoor exercise, or carry a mask."
    )
    st.markdown(
        "**What helps it predict?** Mostly *today's* level of fine dust (PM2.5). "
        "Air pollution usually builds up and clears over several days, so today is a strong clue "
        "about tomorrow."
    )

# ---------- Tab 4: Can I trust this data? ----------
with tab_trust:
    st.markdown("### Can I trust this data?")
    st.markdown(
        "Every 3 hours, an automated system collects the newest readings, cleans them, "
        "and runs **6 automatic checks** before anything appears here."
    )

    runs = run_query("""
        SELECT run_id, run_started_utc, status, records_fetched, readings_inserted, message
        FROM workspace.aqi.pipeline_runs
        ORDER BY run_started_utc DESC
        LIMIT 30
    """)
    if runs.empty:
        st.info("No automated runs recorded yet.")
    else:
        runs["run_started_utc"] = pd.to_datetime(runs["run_started_utc"])
        last = runs.iloc[0]
        last_ist = last["run_started_utc"].tz_convert("Asia/Kolkata")
        live_runs = runs[runs["run_started_utc"] >= PIPELINE_LIVE_SINCE]
        ok_rate = (live_runs["status"] != "failed").mean() if len(live_runs) else 0
        status_word = {"success": "✅ All good", "success_with_warnings": "⚠️ Worked, with warnings",
                       "failed": "❌ Failed"}.get(last["status"], last["status"])

        c1, c2, c3 = st.columns(3)
        with c1:
            info_box("Last update", f"{last_ist:%d %b, %I:%M %p}", "Indian time")
        with c2:
            info_box("Last update result", status_word)
        with c3:
            info_box("Updates that worked", f"{ok_rate:.0%}",
                     f"{len(live_runs)} updates since the system went live (6 Nov)")

        checks = run_query("""
            SELECT check_name, passed
            FROM workspace.aqi.data_quality_results
            WHERE run_id = :run_id
        """, {"run_id": last["run_id"]})
        st.markdown("#### Today's automatic checks")
        if checks.empty:
            st.caption("No checks recorded for the latest update.")
        else:
            passed = int(checks["passed"].astype(bool).sum())
            st.markdown(f"**{passed} of {len(checks)} checks passed.**")
            for _, chk in checks.iterrows():
                icon = "✅" if bool(chk["passed"]) else "❌"
                st.markdown(f"{icon} {CHECK_NAMES.get(chk['check_name'], chk['check_name'])}")

    with st.expander("Known limitations (we're honest about these)"):
        st.markdown(
            "- City values are averages of the stations that reported; cities with one station "
            "may not represent the whole city.\n"
            "- Updates pause when the computer running the collector is offline; the next "
            "update catches up.\n"
            "- The forecast is tested on Delhi only and doesn't use weather data (wind, rain), "
            "so it can underestimate sudden extreme spikes.\n"
            "- Live readings are hourly, while the safe limit is defined for a 24-hour average, "
            "so 'times the safe limit' is an indication, not an official exceedance.\n"
            "- Earlier test runs during setup (before 6 Nov) are not counted in the success rate."
        )

# ---------- Glossary ----------
st.divider()
with st.expander("📖 What do these terms mean?"):
    st.markdown(
        "- **PM2.5 (fine dust):** tiny particles in the air, small enough to go deep into the "
        "lungs. The main cause of unhealthy air in Indian cities.\n"
        "- **µg/m³:** micrograms per cubic metre, i.e. how much fine dust is in the air.\n"
        "- **Safe limit (60 µg/m³):** India's official limit for a 24-hour average. Above this, "
        "air is considered unhealthy.\n"
        "- **AQI (Air Quality Index):** a single number from 0 to 500 summarising air quality. "
        "Above 200 is 'Poor' or worse.\n"
        "- **Monitoring station:** a government device that measures pollution at one location."
    )
st.caption(
    "Built by Janvi Sanghavi · Data: Central Pollution Control Board (CPCB) via data.gov.in · "
    "Historic analysis: 2015–2020 (Kaggle)"
)

# ---------- Fill in the AI briefing last ----------
with briefing_slot.container():
    if "GEMINI_API_KEY" in st.secrets:
        try:
            briefing = ai_briefing(build_facts(live, latest), f"{latest:%Y-%m-%d %H:%M}")
            with st.container(border=True):
                st.markdown("#### 📰 Today's air in brief")
                st.markdown(briefing)
                st.caption("Written by AI (Google Gemini) using only the numbers on this page. "
                           "Check the charts below for exact values.")
        except Exception as error:
            st.caption("Today's briefing is unavailable right now. The data below is unaffected.")
            print(f"[briefing error] {type(error).__name__}: {str(error)[:300]}")
    else:
        st.caption("AI briefing is turned off (no Gemini key configured).")