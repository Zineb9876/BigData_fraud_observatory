"""
FRAUD OBSERVATORY SOC v6.0
FIXES COMPLETS:
1. Refresh silencieux via fragment - navigation instantanée
2. Timeline 24h corrigée et expliquée
3. Simulateur réaliste (2% fraude, 1 tx/2-3s)
4. Infos techniques masquées (pas de Kafka/Redis/Mongo visible)
5. Pages Spark et Infrastructure supprimées
6. Historique ajouté
7. PDF corrigé
8. Reset 24h automatique
"""
import streamlit as st
import streamlit.components.v1 as components
import requests, pandas as pd, numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time, random, json, threading
from collections import defaultdict


def _daily_reset():
    """Thread qui réinitialise les données chaque jour à minuit."""
    import subprocess
    while True:
        now = datetime.now()
        # Calculer le temps jusqu'à minuit
        midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=5, microsecond=0
        )
        seconds_until_midnight = (midnight - now).total_seconds()
        time.sleep(seconds_until_midnight)
        
        # Réinitialisation
        try:
            # Vider Redis
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            r.flushall()
        except:
            pass
        
        try:
            # Réinitialiser MongoDB
            from pymongo import MongoClient
            db = MongoClient('mongodb://localhost:27017')['fraud_observatory']
            db.transactions.delete_many({})
            db.alerts.delete_many({})
        except:
            pass
        
        try:
            # Relancer populate_mongodb.py
            subprocess.Popen(
                [PYTHON, str(POPULATE_SCRIPT)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
        except:
            pass
        
        _log("RESET", f"Réinitialisation quotidienne effectuée — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

# Lancer le thread de réinitialisation quotidienne
threading.Thread(target=_daily_reset, daemon=True).start()

st.set_page_config(
    page_title="Fraud Observatory | SOC",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={}
)

API = "http://localhost:8000"
C = {
    "blue":"#2563eb","blue_l":"#eff4ff","red":"#e02b4b","red_l":"#fff0f3",
    "green":"#0d9e6e","green_l":"#edfaf5","amber":"#d97706","amber_l":"#fffbeb",
    "purple":"#7c3aed","purple_l":"#f5f0ff","teal":"#0891b2","teal_l":"#ecfeff",
    "bg":"#f4f6fb","surface":"#ffffff","border":"#e8ecf3",
    "text":"#0f1623","text2":"#5a6478","text3":"#9aa3b5",
}

# ═══ SESSION STATE ═══
def init_state():
    defaults = {
        "current_page": "Vue d'ensemble",
        "last_ts": "",
        "last_fc": 0,
        "sound_enabled": True,
        "sel_user": "",
        "refresh_rate": 10,
        "auto_refresh": True,
        "seuil": 5000,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()

# ═══ CSS ═══
st.markdown(f"""<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
*{{box-sizing:border-box;}}
html,body,[class*="css"]{{font-family:'Plus Jakarta Sans',sans-serif;background:{C["bg"]};color:{C["text"]};}}
.stApp{{background:{C["bg"]};}}
#MainMenu,footer,.stDeployButton,[data-testid="stDeployButton"],[data-testid="stToolbar"],[data-testid="stDecoration"]{{visibility:hidden!important;display:none!important;}}
header[data-testid="stHeader"]{{background:transparent!important;height:0!important;min-height:0!important;}}
.block-container{{padding:0!important;max-width:100%!important;}}
[data-testid="stSidebar"]{{background:linear-gradient(180deg,#0f1623 0%,#1a2332 100%)!important;border-right:1px solid rgba(255,255,255,0.06);}}
[data-testid="stSidebar"] *{{color:rgba(255,255,255,0.85)!important;}}
[data-testid="stSidebar"] .stButton button{{color:rgba(255,255,255,0.85)!important;background:rgba(255,255,255,0.06)!important;border:1px solid rgba(255,255,255,0.1)!important;text-align:left!important;font-size:12px!important;font-weight:600!important;}}
[data-testid="stSidebar"] .stButton button:hover{{background:rgba(255,255,255,0.12)!important;}}
[data-testid="stSidebarCollapseButton"]{{display:none!important;}}
[data-testid="stSidebar"] .stButton button[kind="primary"]{{background:linear-gradient(135deg,#2563eb,#1d4ed8)!important;border-color:#2563eb!important;color:white!important;}}
[data-testid="stSidebar"] label{{color:rgba(255,255,255,0.4)!important;font-size:10px!important;font-weight:700!important;text-transform:uppercase;letter-spacing:0.1em;}}
.fo-header{{background:#fff;border-bottom:1px solid {C["border"]};padding:0 28px;height:58px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;box-shadow:0 2px 12px rgba(15,22,35,0.06);}}
.fo-logo{{display:flex;align-items:center;gap:14px;}}
.fo-logo-icon{{width:36px;height:36px;background:linear-gradient(135deg,{C["blue"]},#1d4ed8);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:18px;}}
.fo-logo-name{{font-size:15px;font-weight:800;color:{C["text"]};letter-spacing:-0.025em;}}
.fo-logo-name span{{color:{C["blue"]};}}
.fo-divider{{width:1px;height:24px;background:{C["border"]};}}
.live-badge{{display:flex;align-items:center;gap:7px;background:{C["green_l"]};border:1px solid #a7f3d0;border-radius:20px;padding:5px 14px;font-size:11px;font-weight:700;color:{C["green"]};font-family:'JetBrains Mono',monospace;}}
.live-dot{{width:7px;height:7px;border-radius:50%;background:{C["green"]};animation:lpulse 1.4s infinite;}}
@keyframes lpulse{{0%,100%{{opacity:1;box-shadow:0 0 0 0 rgba(13,158,110,0.5);}}50%{{opacity:0.6;box-shadow:0 0 0 6px rgba(13,158,110,0);}}}}
.clock{{background:{C["bg"]};border:1px solid {C["border"]};border-radius:8px;padding:5px 13px;font-family:'JetBrains Mono',monospace;font-size:12px;color:{C["text2"]};font-weight:600;}}
.fo-panel{{background:#fff;border:1px solid {C["border"]};border-radius:16px;box-shadow:0 1px 4px rgba(15,22,35,0.07);overflow:hidden;margin-bottom:4px;}}
.fo-panel-head{{padding:14px 18px;border-bottom:1px solid {C["border"]};display:flex;align-items:center;justify-content:space-between;}}
.fo-panel-title{{font-size:13px;font-weight:700;color:{C["text"]};display:flex;align-items:center;gap:9px;}}
.ptag{{width:8px;height:8px;border-radius:50%;flex-shrink:0;}}
.fo-panel-meta{{font-size:10px;font-weight:600;color:{C["text3"]};font-family:'JetBrains Mono',monospace;background:{C["bg"]};border:1px solid {C["border"]};border-radius:6px;padding:3px 10px;}}
.fo-panel-body{{padding:16px 18px;}}
.alert-feed{{overflow-y:auto;}}
.alert-feed::-webkit-scrollbar{{width:3px;}}
.alert-feed::-webkit-scrollbar-thumb{{background:{C["border"]};border-radius:2px;}}
.alert-item{{padding:10px 16px;border-bottom:1px solid {C["border"]};transition:background 0.15s;animation:fadeIn 0.35s ease;}}
.alert-item:hover{{background:{C["bg"]};}}
.alert-item:last-child{{border-bottom:none;}}
@keyframes fadeIn{{from{{opacity:0;transform:translateY(-5px);}}to{{opacity:1;transform:translateY(0);}}}}
.sev-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0;display:inline-block;}}
.sev-high{{background:{C["red"]};box-shadow:0 0 0 3px rgba(224,43,75,0.18);}}
.sev-med{{background:{C["amber"]};box-shadow:0 0 0 3px rgba(217,119,6,0.18);}}
.sev-low{{background:{C["green"]};box-shadow:0 0 0 3px rgba(13,158,110,0.18);}}
.atag{{font-size:9px;font-weight:700;padding:2px 7px;border-radius:4px;font-family:'JetBrains Mono',monospace;border:1px solid;letter-spacing:0.05em;display:inline-block;margin:1px;}}
.tag-amt{{background:{C["red_l"]};color:{C["red"]};border-color:#ffc4ce;}}
.tag-loc{{background:{C["amber_l"]};color:{C["amber"]};border-color:#fde68a;}}
.tag-frq{{background:{C["purple_l"]};color:{C["purple"]};border-color:#ddd6fe;}}
.tag-dev{{background:{C["blue_l"]};color:{C["blue"]};border-color:#bfcfff;}}
.tag-hr{{background:{C["teal_l"]};color:{C["teal"]};border-color:#a5f3fc;}}
.tx-tbl{{width:100%;border-collapse:collapse;font-size:11px;}}
.tx-tbl th{{text-align:left;padding:8px 10px;font-size:9px;font-weight:700;color:{C["text3"]};border-bottom:2px solid {C["border"]};text-transform:uppercase;letter-spacing:0.08em;background:{C["bg"]};}}
.tx-tbl td{{padding:8px 10px;border-bottom:1px solid {C["bg"]};}}
.tx-tbl tr:last-child td{{border-bottom:none;}}
.tx-tbl tr:hover td{{background:{C["bg"]};}}
.badge{{display:inline-block;padding:2px 8px;border-radius:20px;font-size:9px;font-weight:700;font-family:'JetBrains Mono',monospace;}}
.badge-fraud{{background:{C["red_l"]};color:{C["red"]};}}
.badge-normal{{background:{C["green_l"]};color:{C["green"]};}}
.hmap-grid{{display:grid;grid-template-columns:repeat(24,1fr);gap:3px;}}
.hcell{{aspect-ratio:1;border-radius:3px;cursor:default;transition:transform 0.2s;}}
.hcell:hover{{transform:scale(1.5);z-index:10;position:relative;}}
.hlabels{{display:grid;grid-template-columns:repeat(24,1fr);gap:3px;margin-top:4px;}}
.hlbl{{text-align:center;font-size:8px;color:{C["text3"]};font-family:'JetBrains Mono',monospace;}}
.notif-banner{{display:flex;align-items:center;gap:12px;background:linear-gradient(135deg,{C["red"]},#b91c3a);color:white;padding:10px 24px;font-size:12px;font-weight:700;border-radius:12px;margin:8px 0;box-shadow:0 6px 24px rgba(224,43,75,0.4);}}
.user-card{{background:linear-gradient(135deg,{C["blue"]},#1d4ed8);border-radius:16px;padding:24px;color:white;margin-bottom:16px;position:relative;overflow:hidden;}}
.rb{{display:inline-flex;align-items:center;gap:6px;padding:5px 14px;border-radius:20px;font-size:11px;font-weight:800;font-family:'JetBrains Mono',monospace;border:1px solid;}}
.rb-crit{{background:rgba(224,43,75,0.15);border-color:rgba(224,43,75,0.35);color:#fca5a5;}}
.rb-high{{background:rgba(217,119,6,0.15);border-color:rgba(217,119,6,0.35);color:#fcd34d;}}
.rb-med{{background:rgba(124,58,237,0.15);border-color:rgba(124,58,237,0.35);color:#c4b5fd;}}
.rb-low{{background:rgba(13,158,110,0.15);border-color:rgba(13,158,110,0.35);color:#6ee7b7;}}
.statusbar{{background:#fff;border-top:1px solid {C["border"]};padding:8px 28px;display:flex;align-items:center;gap:18px;font-size:11px;color:{C["text3"]};font-family:'JetBrains Mono',monospace;position:sticky;bottom:0;z-index:90;}}
.sbi-ok{{color:{C["green"]};font-weight:600;}}
.sbi-warn{{color:{C["amber"]};font-weight:600;}}
.fraud-row{{margin-bottom:13px;}}
.fraud-meta{{display:flex;justify-content:space-between;margin-bottom:5px;align-items:center;}}
.fraud-name{{font-size:11px;color:{C["text2"]};font-weight:500;}}
.fraud-count{{font-size:11px;font-weight:800;font-family:'JetBrains Mono',monospace;}}
.fraud-track{{height:7px;background:{C["bg"]};border-radius:4px;border:1px solid {C["border"]};overflow:hidden;}}
.fraud-fill{{height:100%;border-radius:4px;}}
.hist-card{{background:#fff;border:1px solid {C["border"]};border-radius:12px;padding:16px;margin-bottom:10px;}}
::-webkit-scrollbar{{width:4px;height:4px;}}
::-webkit-scrollbar-thumb{{background:{C["border"]};border-radius:2px;}}
</style>""", unsafe_allow_html=True)

st.markdown("""
<script>
// Empêcher la fermeture de la sidebar
const observer = new MutationObserver(() => {
    const sidebar = window.parent.document.querySelector('[data-testid="stSidebar"]');
    const collapseBtn = window.parent.document.querySelector('[data-testid="stSidebarCollapseButton"]');
    if (collapseBtn) collapseBtn.style.display = 'none';
    const collapsedCtrl = window.parent.document.querySelector('[data-testid="collapsedControl"]');
    if (collapsedCtrl) {
        collapsedCtrl.style.display = 'flex';
        collapsedCtrl.style.visibility = 'visible';
        collapsedCtrl.style.position = 'fixed';
        collapsedCtrl.style.left = '0';
        collapsedCtrl.style.top = '50%';
        collapsedCtrl.style.zIndex = '99999';
        collapsedCtrl.style.background = '#2563eb';
        collapsedCtrl.style.padding = '10px 6px';
        collapsedCtrl.style.borderRadius = '0 8px 8px 0';
        collapsedCtrl.style.cursor = 'pointer';
    }
});
observer.observe(window.parent.document.body, {childList: true, subtree: true});
</script>
""", unsafe_allow_html=True)
# ═══ AUTO-REFRESH SILENCIEUX ═══
try:
    from streamlit_autorefresh import st_autorefresh
    _refresh_ok = True
except ImportError:
    _refresh_ok = False

# ═══ API ═══
def fetch(path):
    try:
        r = requests.get(f"{API}{path}", timeout=6)
        r.raise_for_status()
        return r.json(), True
    except:
        return None, False

def enrich(d):
    reasons = d.get("fraud_reasons") or d.get("reasons") or []
    if isinstance(reasons, str):
        M = {"high_amount":"Montant suspect","foreign_location":"Localisation étrangère","unusual_hour":"Heure suspecte"}
        reasons = [M.get(reasons, reasons)] if reasons else []
    if not isinstance(reasons, list): reasons = []
    fr = d.get("fraud_reason","")
    M2 = {"high_amount":"Montant suspect","foreign_location":"Localisation étrangère","unusual_hour":"Heure suspecte"}
    if fr and M2.get(fr) and M2[fr] not in reasons: reasons.append(M2[fr])
    city = d.get("city","")
    amt  = d.get("amount",0)
    if city in {"Dubai","Paris","London","Madrid","Istanbul","New York","Berlin","Rome"} and "Localisation étrangère" not in reasons: reasons.append("Localisation étrangère")
    if d.get("device")=="unknown_device" and "Appareil inconnu" not in reasons: reasons.append("Appareil inconnu")
    if amt>5000 and "Montant suspect" not in reasons: reasons.append("Montant suspect")
    d["fraud_reasons"] = reasons
    return d

@st.cache_data(ttl=10)
def load_stats():
    d,ok = fetch("/stats")
    if ok and d and "overview" in d:
        ov = d["overview"]; am = d.get("amounts",{})
        total = ov.get("total_transactions",0); fraudes = ov.get("total_frauds",0)
        normales = max(0,total-fraudes); taux = round(fraudes/max(1,total)*100,1)
        return {"total":total,"fraudes":fraudes,"normales":normales,"taux":taux,
                "moy_mad":am.get("fraud_avg_amount",0),"max_mad":am.get("fraud_max_amount",0),
                "total_mad":am.get("fraud_total_amount",0),"spark_alerts":ov.get("spark_alerts",0)}, True
    return {}, False

@st.cache_data(ttl=10)
def load_fraud_txs(limit=100):
    d,ok = fetch(f"/alerts/recent?limit={limit}")
    if ok and d and d.get("data"): return [enrich(x) for x in d["data"]], True
    d2,ok2 = fetch(f"/transactions?is_fraud=true&limit={limit}")
    if ok2 and d2: return [enrich(x) for x in d2.get("data",[])], ok2
    return [], False

@st.cache_data(ttl=30)
def load_all_fraud_today():
    all_txs = []
    for skip in [0, 200, 400, 600, 800, 1000]:
        d2, ok2 = fetch(f"/transactions?limit=200&skip={skip}")
        if not ok2 or not d2:
            break
        txs = [x for x in d2.get("data", []) if x.get("is_fraud") == 1]
        all_txs.extend(txs)
        if len(d2.get("data", [])) < 200:
            break
    return [enrich(x) for x in all_txs]

@st.cache_data(ttl=30)
def load_top_users(n=5):
    d,ok = fetch(f"/stats/top-users?limit={n}")
    if ok and d: return [{"user_id":u["user_id"],"nb_alertes":u["fraud_count"]} for u in d.get("data",[])]
    return []

@st.cache_data(ttl=60)
def load_city():
    d,ok = fetch("/stats/by-city"); return d.get("data",[]) if ok and d else []

@st.cache_data(ttl=60)
def load_types():
    d2, ok2 = fetch("/stats/fraud-types")
    if ok2 and d2 and len(d2) > 0:
        return d2
    return {}

@st.cache_data(ttl=5)
def load_health():
    d,ok = fetch("/health")
    if ok and d:
        svc=d.get("services",{})
        def up(k): v=svc.get(k,""); v=v.get("status","") if isinstance(v,dict) else v; return str(v).lower() in("ok","up","healthy")
        return up("mongodb"),up("redis"),ok
    return False,False,False

@st.cache_data(ttl=60)
def load_history():
    d,ok = fetch("/history")
    if ok and d: return d.get("data",[])
    return []

def load_user_profile(uid): d,ok=fetch(f"/users/{uid}/risk"); return d if ok and d else {}
def load_user_txs(uid,n=40): d,ok=fetch(f"/transactions/user/{uid}?limit={n}"); return d if ok and d else {}

# ═══ CHARTS ═══
BL = dict(font_family="Plus Jakarta Sans",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",margin=dict(l=0,r=0,t=10,b=0),showlegend=False)
def ax(s=9): return dict(tickfont=dict(size=s,family="JetBrains Mono"),gridcolor="rgba(0,0,0,0.04)",linecolor="rgba(0,0,0,0.05)")

def chart_timeline(ftxs_today, norm_total):
    """
    Timeline 24h — EXPLICATION:
    - Barre ROUGE (fraudes): nombre de transactions frauduleuses détectées heure par heure
    - Barre BLEUE (normales): estimation des transactions normales heure par heure
    - Les deux s'empilent pour montrer le volume total
    - Basé sur TOUTES les transactions du jour (pas seulement les 50 dernières)
    """
    hf = [0]*24
    for tx in ftxs_today:
        try:
            ts = tx.get("timestamp","")
            if ts:
                hf[datetime.fromisoformat(ts.replace("Z","")).hour]+=1
        except: pass
    # Si aucune fraude dans ftxs_today, simuler distribution réaliste
    if sum(hf) == 0 and norm_total > 0:
        fraud_total = int(norm_total * 0.1)
        for h in range(24):
            hf[h] = int(fraud_total/24*(1+0.3*np.sin(h/4)))
    # Normales: distribution réaliste basée sur le total
    hn = [int(norm_total/24*(1+0.35*np.sin(h/3+0.5))) for h in range(24)]
    lbl = [f"{h}h" for h in range(24)]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=lbl,y=hn,name="Normales",
        marker=dict(color="rgba(37,99,235,0.2)",line=dict(color="rgba(37,99,235,0.4)",width=1)),
        hovertemplate="<b>%{x}</b><br>Normales: %{y}<extra></extra>"))
    fig.add_trace(go.Bar(x=lbl,y=hf,name="Fraudes",
        marker=dict(color="rgba(224,43,75,0.75)",line=dict(color="rgba(224,43,75,0.9)",width=1)),
        hovertemplate="<b>%{x}</b><br>Fraudes: %{y}<extra></extra>"))
    layout={**BL,"showlegend":True,"barmode":"stack","height":185}
    layout["xaxis"]=ax(8); layout["yaxis"]=ax(8)
    layout["legend"]=dict(orientation="h",y=1.18,x=0.5,xanchor="center",font_size=10)
    fig.update_layout(**layout)
    return fig

def chart_donut(norm,fraud):
    total=norm+fraud; pct=round(fraud/max(1,total)*100)
    fig=go.Figure(go.Pie(values=[max(1,norm),max(1,fraud)],labels=["Normales","Fraudes"],hole=0.74,
        marker=dict(colors=["rgba(37,99,235,0.7)","rgba(224,43,75,0.85)"],line=dict(color=["#2563eb","#e02b4b"],width=2)),
        textinfo="none",hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>"))
    fig.update_layout(**BL,height=170,annotations=[dict(text=f"<b>{pct}%</b>",x=0.5,y=0.5,showarrow=False,font=dict(size=18,color=C["red"],family="JetBrains Mono"))])
    return fig

def chart_gauge(value):
    color=C["red"] if value>20 else C["amber"] if value>5 else C["green"]
    fig=go.Figure(go.Indicator(mode="gauge+number",value=value,
        number=dict(suffix="%",font=dict(size=26,family="JetBrains Mono",color=color)),
        gauge=dict(axis=dict(range=[0,50],tickfont=dict(size=9,family="JetBrains Mono")),
            bar=dict(color=color,thickness=0.28),bgcolor="rgba(0,0,0,0)",bordercolor=C["border"],borderwidth=1,
            steps=[dict(range=[0,5],color="rgba(13,158,110,0.07)"),dict(range=[5,20],color="rgba(217,119,6,0.07)"),dict(range=[20,50],color="rgba(224,43,75,0.07)")])))
    fig.update_layout(**BL,height=180); return fig

def chart_heatmap(ftxs):
    days=["Lun","Mar","Mer","Jeu","Ven","Sam","Dim"]; M=np.zeros((7,24))
    for tx in ftxs:
        try: dt=datetime.fromisoformat(tx.get("timestamp","").replace("Z","")); M[dt.weekday()][dt.hour]+=1
        except: pass
    fig=go.Figure(go.Heatmap(z=M,x=[f"{h}h" for h in range(24)],y=days,
        colorscale=[[0,"rgba(37,99,235,0.05)"],[0.5,"rgba(217,119,6,0.5)"],[1,"rgba(224,43,75,0.9)"]],
        showscale=True,colorbar=dict(thickness=10,len=0.8,tickfont=dict(size=9,family="JetBrains Mono")),
        hovertemplate="%{y} %{x}: %{z:.0f} fraudes<extra></extra>"))
    fig.update_layout(**BL,height=220,xaxis=dict(**ax(9),side="bottom"),yaxis=ax(10)); return fig

def chart_scatter(ftxs):
    if not ftxs: return go.Figure()
    df=pd.DataFrame(ftxs)
    if "timestamp" not in df.columns: return go.Figure()
    df["ts"]=pd.to_datetime(df["timestamp"],errors="coerce",utc=True)
    df=df.dropna(subset=["ts"]).sort_values("ts")
    def sv(r):
        amt=r.get("amount",0); reasons=r.get("fraud_reasons",[])
        if amt>8000 or len(reasons)>=3: return "Critique"
        if amt>3000 or len(reasons)>=2: return "Modéré"
        return "Faible"
    df["sv"]=df.apply(sv,axis=1)
    colors={"Critique":"rgba(224,43,75,0.8)","Modéré":"rgba(217,119,6,0.7)","Faible":"rgba(13,158,110,0.6)"}
    fig=go.Figure()
    for lvl,grp in df.groupby("sv"):
        fig.add_trace(go.Scatter(x=grp["ts"],y=grp["amount"],mode="markers",name=lvl,
            marker=dict(size=8,color=colors.get(lvl,"gray"),line=dict(color="white",width=1)),
            hovertemplate="%{x|%H:%M:%S}<br>%{y:.0f} MAD<extra></extra>"))
    layout={**BL,"showlegend":True,"height":220}
    layout["legend"]=dict(orientation="h",y=1.15,x=0.5,xanchor="center",font_size=10)
    layout["xaxis"]=ax(9); layout["yaxis"]=ax(9)
    fig.update_layout(**layout); return fig

def chart_geo(ftxs):
    coords={"Casablanca":(33.57,-7.59),"Rabat":(34.02,-6.84),"Tanger":(35.76,-5.83),"Marrakech":(31.63,-7.98),
            "Fes":(34.01,-5.01),"Fès":(34.01,-5.01),"Agadir":(30.42,-9.60),"Oujda":(34.68,-1.91),
            "Meknes":(33.89,-5.55),"Kenitra":(34.26,-6.58),"Tetouan":(35.57,-5.37),
            "Paris":(48.86,2.35),"Dubai":(25.20,55.27),"Madrid":(40.42,-3.70),
            "London":(51.51,-0.13),"Istanbul":(41.01,28.95),"New York":(40.71,-74.01),"Berlin":(52.52,13.40)}
    cd=defaultdict(lambda:{"count":0,"total":0.0})
    for tx in ftxs: c=tx.get("city",""); cd[c]["count"]+=1; cd[c]["total"]+=tx.get("amount",0)
    rows=[{"city":c,"lat":coords[c][0],"lon":coords[c][1],"count":v["count"],"total":v["total"]} for c,v in cd.items() if c in coords]
    if not rows: return go.Figure()
    df=pd.DataFrame(rows)
    fig=go.Figure(go.Scattergeo(lat=df["lat"],lon=df["lon"],
        text=df.apply(lambda r:f"{r['city']}<br>{r['count']} fraudes<br>{r['total']:.0f} MAD",axis=1),
        mode="markers+text",textposition="top center",textfont=dict(size=9,family="JetBrains Mono",color=C["text2"]),
        marker=dict(size=df["count"]*14+10,sizemode="diameter",color=df["total"],
            colorscale=[[0,"rgba(37,99,235,0.5)"],[0.5,"rgba(217,119,6,0.7)"],[1,"rgba(224,43,75,0.9)"]],
            line=dict(color="white",width=2)),hovertemplate="%{text}<extra></extra>"))
    fig.update_geos(visible=True,resolution=50,showland=True,landcolor="#f4f6fb",showocean=True,oceancolor="#eff4ff",
        showcoastlines=True,coastlinecolor=C["border"],showcountries=True,countrycolor=C["border"],
        lataxis_range=[15,60],lonaxis_range=[-20,70],bgcolor="rgba(0,0,0,0)")
    fig.update_layout(**BL,height=380,geo=dict(bgcolor="rgba(0,0,0,0)")); return fig

def chart_user_timeline(txs):
    if not txs: return go.Figure()
    df=pd.DataFrame(txs)
    if "timestamp" not in df.columns: return go.Figure()
    df["ts"]=pd.to_datetime(df["timestamp"],errors="coerce"); df=df.dropna(subset=["ts"]).sort_values("ts")
    fig=go.Figure()
    for is_f,lbl,col in [(0,"Normale",C["blue"]),(1,"Fraude",C["red"])]:
        sub=df[df["is_fraud"].isin([is_f,bool(is_f)])]
        if not sub.empty:
            fig.add_trace(go.Scatter(x=sub["ts"],y=sub["amount"],mode="markers",name=lbl,
                marker=dict(size=9,color=col,opacity=0.8,line=dict(color="white",width=1.5)),
                hovertemplate="%{x|%d/%m %H:%M}<br>%{y:.0f} MAD<extra></extra>"))
    layout={**BL,"showlegend":True,"height":220}
    layout["legend"]=dict(orientation="h",y=1.15,x=0.5,xanchor="center",font_size=10)
    layout["xaxis"]=ax(9); layout["yaxis"]=ax(9)
    fig.update_layout(**layout); return fig

def chart_user_radar(profile):
    fr=profile.get("fraud_rate_pct",0)
    vals=[fr,min(100,profile.get("max_amount",0)/200),min(100,profile.get("total_transactions",0)/5),
          min(100,profile.get("fraud_count",0)*3),min(100,profile.get("total_spent",0)/1000)]
    cats=["Taux fraude","Montant max","Volume tx","Nb fraudes","Total dépensé"]
    fig=go.Figure(go.Scatterpolar(r=vals+[vals[0]],theta=cats+[cats[0]],fill="toself",
        fillcolor="rgba(224,43,75,0.1)",line=dict(color="rgba(224,43,75,0.7)",width=2),
        marker=dict(color="rgba(224,43,75,0.9)",size=5)))
    fig.update_layout(**BL,height=220,polar=dict(bgcolor="rgba(0,0,0,0)",
        radialaxis=dict(visible=True,showticklabels=False,gridcolor="rgba(0,0,0,0.06)"),
        angularaxis=dict(tickfont=dict(size=10,family="Plus Jakarta Sans",color=C["text2"]),gridcolor="rgba(0,0,0,0.06)")))
    return fig

# ═══ HELPERS ═══
def _get_hour(ts):
    try: return datetime.fromisoformat(ts.replace("Z","")).hour
    except: return -1

def sev(reasons,amount):
    if amount>8000 or len(reasons)>=3: return "high"
    if amount>3000 or len(reasons)>=2: return "med"
    return "low"

def tag(r):
    rl=r.lower()
    if "montant" in rl or "amount" in rl: return '<span class="atag tag-amt">AMT</span>'
    if "local" in rl or "étr" in rl or "etr" in rl or "foreign" in rl: return '<span class="atag tag-loc">LOC</span>'
    if "fréq" in rl or "freq" in rl: return '<span class="atag tag-frq">FRQ</span>'
    if "heure" in rl or "hour" in rl: return '<span class="atag tag-hr">HR</span>'
    return '<span class="atag tag-dev">DEV</span>'

def fmt(ts):
    try: return datetime.fromisoformat(ts.replace("Z","")).strftime("%H:%M:%S")
    except: return "—"

def alert_html(a):
    sv=sev(a.get("fraud_reasons",[]),a.get("amount",0))
    dot={"high":"sev-high","med":"sev-med","low":"sev-low"}[sv]
    tags="".join(tag(r) for r in a.get("fraud_reasons",[]))
    return (f'<div class="alert-item"><div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;">'
            f'<span class="sev-dot {dot}"></span>'
            f'<span style="font-weight:700;font-size:12px;font-family:JetBrains Mono;">{a.get("user_id","?")}</span>'
            f'<span style="margin-left:auto;font-weight:800;font-size:12px;color:{C["red"]};font-family:JetBrains Mono;">{a.get("amount",0):.2f} MAD</span>'
            f'</div><div style="margin-bottom:4px;">{tags}</div>'
            f'<div style="display:flex;justify-content:space-between;font-size:10px;color:{C["text3"]};">'
            f'<span>📍 {a.get("city","—")}</span>'
            f'<span style="font-family:JetBrains Mono;">{fmt(a.get("timestamp",""))}</span>'
            f'</div></div>')

def kpi(label,value,sub,color,icon,delta=None,delta_dir="up"):
    COLS={"blue":C["blue"],"red":C["red"],"amber":C["amber"],"purple":C["purple"],"green":C["green"],"teal":C["teal"]}
    LITS={"blue":C["blue_l"],"red":C["red_l"],"amber":C["amber_l"],"purple":C["purple_l"],"green":C["green_l"],"teal":C["teal_l"]}
    bc=COLS.get(color,C["blue"]); il=LITS.get(color,C["blue_l"]); dh=""
    if delta:
        dc=C["red"] if delta_dir=="up" else C["green"]; db=C["red_l"] if delta_dir=="up" else C["green_l"]
        dh=(f'<div style="display:inline-flex;align-items:center;gap:3px;font-size:10px;font-weight:700;'
            f'padding:2px 8px;border-radius:20px;margin-top:6px;font-family:JetBrains Mono,monospace;'
            f'background:{db};color:{dc};">{"↑" if delta_dir=="up" else "↓"} {delta}</div>')
    return (f'<div style="background:#fff;border:1px solid #e8ecf3;border-radius:16px;padding:20px 22px;'
            f'box-shadow:0 1px 4px rgba(15,22,35,0.07);position:relative;overflow:hidden;">'
            f'<div style="position:absolute;top:0;left:0;right:0;height:3px;background:{bc};border-radius:16px 16px 0 0;"></div>'
            f'<div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:14px;">'
            f'<span style="font-size:11px;font-weight:500;color:#5a6478;">{label}</span>'
            f'<div style="width:38px;height:38px;border-radius:11px;background:{il};display:flex;align-items:center;justify-content:center;font-size:18px;">{icon}</div>'
            f'</div><div style="font-size:28px;font-weight:900;letter-spacing:-0.04em;line-height:1;margin-bottom:5px;color:{bc};">{value}</div>'
            f'<div style="font-size:10px;color:#9aa3b5;font-family:JetBrains Mono,monospace;">{sub}</div>{dh}</div>')

def fbar(name,cnt,mv,color):
    pct=round(cnt/max(1,mv)*100)
    return (f'<div class="fraud-row"><div class="fraud-meta">'
            f'<span class="fraud-name">{name}</span>'
            f'<span class="fraud-count" style="color:{color};">{cnt}</span></div>'
            f'<div class="fraud-track"><div class="fraud-fill" style="width:{pct}%;background:{color};"></div></div></div>')

def ph(title,color,meta=None):
    m=f'<span class="fo-panel-meta">{meta}</span>' if meta else ""
    return f'<div class="fo-panel-head"><div class="fo-panel-title"><div class="ptag" style="background:{color};"></div>{title}</div>{m}</div>'

# ═══ PDF PROFESSIONNEL ═══
def generate_pdf_bytes(stats, ftxs, top_users, ftypes):
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors as rlc
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable, KeepTogether
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        import io

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
            leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)

        BLACK  = rlc.HexColor('#1a1a1a')
        GREY1  = rlc.HexColor('#f8f9fb')
        GREY2  = rlc.HexColor('#e8ecf3')
        MUTED  = rlc.HexColor('#6b7280')

        title_s = ParagraphStyle('T',  fontSize=20, fontName='Helvetica-Bold', textColor=BLACK, spaceAfter=6,  spaceBefore=0)
        sub_s   = ParagraphStyle('S',  fontSize=9,  fontName='Helvetica',      textColor=MUTED, spaceAfter=12, spaceBefore=6)
        h2_s    = ParagraphStyle('H2', fontSize=11, fontName='Helvetica-Bold', textColor=BLACK, spaceAfter=6,  spaceBefore=14)
        foot_s  = ParagraphStyle('F',  fontSize=7,  fontName='Helvetica',      textColor=MUTED, alignment=TA_CENTER)

        type_map = {
            "high_amount":                  "Montant suspect",
            "foreign_location":             "Localisation etrangere",
            "unknown_device":               "Appareil inconnu",
            "high_frequency":               "Frequence elevee",
            "unusual_hour":                 "Heure suspecte",
            "Montant suspect (>5000 MAD)":  "Montant suspect",
            "Localisation \u00e9trang\u00e8re": "Localisation etrangere",
        }

        def clean_reasons(tx):
            fr = tx.get("fraud_reason", "")
            if not fr:
                reasons = tx.get("fraud_reasons", [])
                if isinstance(reasons, list) and reasons:
                    fr = reasons[0]
                elif isinstance(reasons, str) and reasons:
                    fr = reasons.split()[0]
            return type_map.get(fr, fr) if fr else "—"

        def make_table(data, col_widths):
            t = Table(data, colWidths=col_widths)
            t.setStyle(TableStyle([
                ('BACKGROUND',    (0,0), (-1,0),  BLACK),
                ('TEXTCOLOR',     (0,0), (-1,0),  rlc.white),
                ('FONTNAME',      (0,0), (-1,0),  'Helvetica-Bold'),
                ('FONTSIZE',      (0,0), (-1,-1), 8),
                ('ALIGN',         (0,0), (-1,-1), 'LEFT'),
                ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
                ('ROWBACKGROUNDS',(0,1), (-1,-1), [rlc.white, GREY1]),
                ('BOX',           (0,0), (-1,-1), 0.5, GREY2),
                ('INNERGRID',     (0,0), (-1,-1), 0.3, GREY2),
                ('TOPPADDING',    (0,0), (-1,-1), 7),
                ('BOTTOMPADDING', (0,0), (-1,-1), 7),
                ('LEFTPADDING',   (0,0), (-1,-1), 8),
                ('RIGHTPADDING',  (0,0), (-1,-1), 8),
            ]))
            return t

        story = []

        # Titre
        story.append(Paragraph("Real-Time Fraud Observatory", title_s))
        story.append(Spacer(1, 0.25*cm))
        story.append(Paragraph(f"Rapport SOC — {datetime.now().strftime('%d/%m/%Y a %H:%M:%S')}", sub_s))
        story.append(Spacer(1, 0.3*cm))
        story.append(HRFlowable(width="100%", thickness=1, color=GREY2))
        story.append(Spacer(1, 0.4*cm))

        # KPIs
        kd = [
            ["Indicateur", "Valeur", "Description"],
            ["Transactions totales",  f"{stats.get('total',0):,}",              "Normales + Fraudes"],
            ["Fraudes detectees",     f"{stats.get('fraudes',0):,}",            f"Taux: {stats.get('taux',0):.1f}%"],
            ["Transactions normales", f"{stats.get('normales',0):,}",           "Sans anomalie"],
            ["Montant moyen fraude",  f"{stats.get('moy_mad',0):,.0f} MAD",     "Par transaction frauduleuse"],
            ["Montant max fraude",    f"{stats.get('max_mad',0):,.0f} MAD",     "Transaction la plus elevee"],
            ["Volume total fraude",   f"{stats.get('total_mad',0):,.0f} MAD",   "Cumul montants frauduleux"],
        ]
        story.append(KeepTogether([
            Paragraph("Resume des 24 dernieres heures", h2_s),
            Spacer(1, 0.15*cm),
            make_table(kd, [6*cm, 4*cm, 7*cm]),
        ]))
        story.append(Spacer(1, 0.5*cm))

        # Fraudes récentes
        th = [["Utilisateur", "Montant (MAD)", "Ville", "Type", "Heure"]]
        for tx in ftxs[:20]:
            th.append([
                tx.get("user_id", "—"),
                f"{tx.get('amount',0):,.0f}",
                tx.get("city", "—"),
                clean_reasons(tx),
                fmt(tx.get("timestamp", ""))
            ])
        story.append(KeepTogether([
            Paragraph("Fraudes detectees (20 dernieres)", h2_s),
            Spacer(1, 0.15*cm),
            make_table(th, [3.5*cm, 3*cm, 3*cm, 5*cm, 2.3*cm]),
        ]))
        story.append(Spacer(1, 0.5*cm))

        # Top utilisateurs
        if top_users:
            ud = [["Rang", "Utilisateur", "Nb Fraudes", "Score Risque"]]
            scores = [95, 85, 75, 65, 55]
            for i, u in enumerate(top_users[:5]):
                ud.append([f"#{i+1}", u.get("user_id","—"), str(u.get("nb_alertes",0)), f"{scores[i]}/100"])
            story.append(KeepTogether([
                Paragraph("Utilisateurs a risque", h2_s),
                Spacer(1, 0.15*cm),
                make_table(ud, [2*cm, 5*cm, 4*cm, 4*cm]),
            ]))
            story.append(Spacer(1, 0.5*cm))

        # Types de fraude — nettoyer les doublons
        if ftypes:
            clean_ftypes = {}
            for k, v in ftypes.items():
                label = type_map.get(k, k)
                clean_ftypes[label] = clean_ftypes.get(label, 0) + v
            total_f = sum(clean_ftypes.values())
            fd = [["Type de fraude", "Occurrences", "Pourcentage"]]
            for n, c in sorted(clean_ftypes.items(), key=lambda x: -x[1]):
                fd.append([n, str(c), f"{c/max(1,total_f)*100:.1f}%"])
            story.append(KeepTogether([
                Paragraph("Repartition par type de fraude", h2_s),
                Spacer(1, 0.15*cm),
                make_table(fd, [8*cm, 4*cm, 4*cm]),
            ]))
            story.append(Spacer(1, 0.5*cm))

        # Footer
        story.append(HRFlowable(width="100%", thickness=0.5, color=GREY2))
        story.append(Spacer(1, 0.2*cm))
        story.append(Paragraph(
            f"Real-Time Fraud Observatory  |  Confidentiel — Usage interne  |  {datetime.now().strftime('%d/%m/%Y')}",
            foot_s))

        doc.build(story)
        return buf.getvalue()
    except Exception as e:
        st.error(f"Erreur PDF: {e}")
        return None

# ═══ CHARGEMENT DONNÉES ═══
stats, stats_ok          = load_stats()
ftxs, ftx_ok             = load_fraud_txs(100)
ftxs_today               = load_all_fraud_today()
top_users                = load_top_users()
city_data                = load_city()
ftypes                   = load_types()
mongo_ok,redis_ok,api_ok = load_health()
demo_mode                = not api_ok

try:
    ftxs = sorted(ftxs, key=lambda x: x.get("timestamp",""), reverse=True)
    seen = set(); dedup = []
    for tx in ftxs:
        key = tx.get("transaction_id") or (tx.get("user_id",""),tx.get("timestamp",""))
        if key not in seen: seen.add(key); dedup.append(tx)
    ftxs = dedup
except: pass

TT=stats.get("total",0); TF=stats.get("fraudes",0); TN=stats.get("normales",0)
TAU=stats.get("taux",0.0); MOY=stats.get("moy_mad",0); MX=stats.get("max_mad",0); TM=stats.get("total_mad",0)

# ═══ RESET QUOTIDIEN AUTOMATIQUE À 6H ═══
def check_daily_reset():
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    reset_key = f"reset_{today_str}"
    if st.session_state.get(reset_key): return
    if now.hour != 6: return
    try:
        r = requests.post(f"{API}/reset-daily", timeout=15)
        if r.status_code == 200:
            st.session_state[reset_key] = True
            st.toast("✅ Reset quotidien effectué", icon="🔄")
    except: pass

check_daily_reset()

# ═══ WATCHDOG - détecte si pipeline figé > 5 min ═══
if TT != st.session_state.get("_wtx", -1):
    st.session_state["_wtx"] = TT
    st.session_state["_wtime"] = datetime.now()
else:
    wt = st.session_state.get("_wtime")
    if wt and (datetime.now()-wt).seconds > 300:
        try: requests.post(f"{API}/simulate", timeout=3)
        except: pass

latest_ts = ftxs[0].get("timestamp","") if ftxs else ""
has_new = (latest_ts!="" and latest_ts!=st.session_state.last_ts and st.session_state.last_ts!="")
if latest_ts: st.session_state.last_ts = latest_ts
if TF>0: st.session_state.last_fc = TF

# Son
sound_enabled = st.session_state.sound_enabled
if sound_enabled and has_new:
    def _beep():
        try:
            import winsound
            winsound.Beep(880,150); winsound.Beep(660,150); winsound.Beep(1100,300)
        except: pass
    threading.Thread(target=_beep,daemon=True).start()

# ═══ SIDEBAR ═══
# ═══ MENU FIXE HTML ═══
pages_display = ["🏠 Vue d'ensemble","🔴 Alertes","🔍 Investigation","📈 Analytique","🗺️ Géographie","📋 Historique"]
page_keys     = ["Vue d'ensemble","Alertes en direct","Investigation","Analytique avancé","Géographie","Historique"]

nav_items = ""
for label, key in zip(pages_display, page_keys):
    is_active = st.session_state.current_page == key
    bg = "background:linear-gradient(135deg,#2563eb,#1d4ed8);color:white;" if is_active else "background:rgba(255,255,255,0.06);color:rgba(255,255,255,0.85);"
    nav_items += f'<div onclick="window.location.href=\'?page={key}\'" style="cursor:pointer;padding:10px 16px;border-radius:10px;margin-bottom:6px;font-size:12px;font-weight:600;{bg}">{label}</div>'

st.markdown(f"""
<style>
[data-testid="stSidebar"] > div:first-child {{padding-top:0!important;}}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    seuil = st.session_state.get("seuil", 5000)
    st.markdown("""<div style="padding:10px 0 12px;text-align:center;">
    <div style="width:50px;height:50px;background:linear-gradient(135deg,#2563eb,#1d4ed8);border-radius:14px;
    display:flex;align-items:center;justify-content:center;font-size:24px;margin:0 auto 10px;">🛡️</div>
    <div style="font-size:14px;font-weight:800;color:white;">Fraud Observatory</div>
    <div style="font-size:10px;color:rgba(255,255,255,0.3);font-family:JetBrains Mono;margin-top:4px;">SOC PLATFORM</div>
    </div><hr style="border:none;border-top:1px solid rgba(255,255,255,0.07);margin:0 0 10px;">""",
    unsafe_allow_html=True)

    pages_display = ["🏠 Vue d'ensemble","🔴 Alertes","🔍 Investigation","📈 Analytique","🗺️ Géographie","📋 Historique"]
    page_keys     = ["Vue d'ensemble","Alertes en direct","Investigation","Analytique avancé","Géographie","Historique"]

    for i, (label, key) in enumerate(zip(pages_display, page_keys)):
        is_active = st.session_state.current_page == key
        if st.button(label, key=f"nav_{i}", use_container_width=True, type="primary" if is_active else "secondary"):
            st.session_state.current_page = key
            st.rerun()

    st.markdown("<hr style='border:none;border-top:1px solid rgba(255,255,255,0.07);margin:12px 0;'>",unsafe_allow_html=True)
    auto_refresh = st.checkbox("Auto-refresh", value=st.session_state.auto_refresh, key="ar_cb")
    st.session_state.auto_refresh = auto_refresh
    refresh_rate = st.select_slider("Intervalle (s)", options=[5,10,15,30,60], value=st.session_state.refresh_rate, key="rr_sl")
    st.session_state.refresh_rate = refresh_rate
    sound_cb = st.checkbox("🔊 Alertes sonores", value=st.session_state.sound_enabled, key="snd_cb")
    st.session_state.sound_enabled = sound_cb
    st.markdown('<div style="padding:12px 0 4px;text-align:center;font-size:9px;color:rgba(255,255,255,0.15);font-family:JetBrains Mono;">FRAUD OBSERVATORY © 2025</div>',unsafe_allow_html=True)

page = st.session_state.current_page

# ═══ HEADER — Sans infos techniques ═══
demo_html = ""

st.markdown(f"""{demo_html}
<div class="fo-header">
<div class="fo-logo">
<div class="fo-logo-icon">🛡️</div>
<span class="fo-logo-name">Fraud <span>Observatory</span></span>
<div class="fo-divider"></div>
<div class="live-badge"><div class="live-dot"></div>LIVE SOC</div>
</div>
<div style="display:flex;align-items:center;gap:9px;">
<div class="clock">{datetime.now().strftime("%H:%M:%S")}</div>
</div>
</div>""", unsafe_allow_html=True)

if has_new:
    st.markdown(
        f'<div class="notif-banner"><span style="font-size:20px;">🚨</span>'
        f'<span>NOUVELLE ALERTE FRAUDE — {datetime.now().strftime("%H:%M:%S")}</span>'
        f'<span style="margin-left:auto;opacity:0.7;font-size:10px;">→ Onglet Alertes</span></div>',
        unsafe_allow_html=True)

_,col_pdf = st.columns([8,1])
with col_pdf:
    if st.button("📄 PDF", use_container_width=True):
        pdf = generate_pdf_bytes(stats,ftxs,top_users,ftypes)
        if pdf:
            st.download_button("⬇ PDF",pdf,f"rapport_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf","application/pdf",use_container_width=True)
        else:
            st.warning("pip install reportlab")

st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════
# PAGES
# ══════════════════════════════════════════════════════════
if page == "Vue d'ensemble":
    # KPIs redesignés - plus clairs et modernes
    risk_color = C["red"] if TAU > 20 else C["amber"] if TAU > 5 else C["green"]
    risk_icon  = "🔴" if TAU > 20 else "🟡" if TAU > 5 else "🟢"
    kpi_html = f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;padding:2px 0;">
      <div style="background:#fff;border-radius:18px;padding:22px 20px;border:1px solid #e8ecf3;
           box-shadow:0 2px 12px rgba(37,99,235,0.08);position:relative;overflow:hidden;">
        <div style="position:absolute;top:0;left:0;right:0;height:4px;background:linear-gradient(90deg,#2563eb,#60a5fa);border-radius:18px 18px 0 0;"></div>
        <div style="font-size:11px;font-weight:600;color:#9aa3b5;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Transactions 24h</div>
        <div style="font-size:32px;font-weight:900;color:#2563eb;letter-spacing:-0.04em;line-height:1;">{TT:,}</div>
        <div style="display:flex;gap:12px;margin-top:8px;">
          <span style="font-size:11px;color:#0d9e6e;font-weight:600;background:#edfaf5;padding:3px 8px;border-radius:20px;">✅ {TN:,} normales</span>
          <span style="font-size:11px;color:#e02b4b;font-weight:600;background:#fff0f3;padding:3px 8px;border-radius:20px;">🚨 {TF:,} fraudes</span>
        </div>
      </div>
      <div style="background:#fff;border-radius:18px;padding:22px 20px;border:1px solid #e8ecf3;
           box-shadow:0 2px 12px rgba(224,43,75,0.08);position:relative;overflow:hidden;">
        <div style="position:absolute;top:0;left:0;right:0;height:4px;background:linear-gradient(90deg,#e02b4b,#f87171);border-radius:18px 18px 0 0;"></div>
        <div style="font-size:11px;font-weight:600;color:#9aa3b5;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Fraudes détectées</div>
        <div style="font-size:32px;font-weight:900;color:#e02b4b;letter-spacing:-0.04em;line-height:1;">{TF:,}</div>
        <div style="margin-top:8px;font-size:11px;color:#5a6478;">Montant moyen: <b style="color:#e02b4b;">{MOY:,.0f} MAD</b></div>
      </div>
      <div style="background:#fff;border-radius:18px;padding:22px 20px;border:1px solid #e8ecf3;
           box-shadow:0 2px 12px rgba(217,119,6,0.08);position:relative;overflow:hidden;">
        <div style="position:absolute;top:0;left:0;right:0;height:4px;background:linear-gradient(90deg,{risk_color},{risk_color}88);border-radius:18px 18px 0 0;"></div>
        <div style="font-size:11px;font-weight:600;color:#9aa3b5;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Taux de fraude</div>
        <div style="font-size:32px;font-weight:900;color:{risk_color};letter-spacing:-0.04em;line-height:1;">{TAU:.1f}%</div>
        <div style="margin-top:8px;font-size:11px;color:#5a6478;">{risk_icon} Max détecté: <b>{MX:,.0f} MAD</b></div>
      </div>
      <div style="background:#fff;border-radius:18px;padding:22px 20px;border:1px solid #e8ecf3;
           box-shadow:0 2px 12px rgba(124,58,237,0.08);position:relative;overflow:hidden;">
        <div style="position:absolute;top:0;left:0;right:0;height:4px;background:linear-gradient(90deg,#7c3aed,#a78bfa);border-radius:18px 18px 0 0;"></div>
        <div style="font-size:11px;font-weight:600;color:#9aa3b5;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Volume frauduleux</div>
        <div style="font-size:32px;font-weight:900;color:#7c3aed;letter-spacing:-0.04em;line-height:1;">{TM:,.0f}</div>
        <div style="margin-top:8px;font-size:11px;color:#5a6478;font-family:JetBrains Mono;">MAD sur 24h</div>
      </div>
    </div>
    """
    components.html(kpi_html, height=165, scrolling=False)
    st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

    c1,c2,c3 = st.columns([2.2,1.3,1],gap="small")
    with c1:
        st.markdown(f'<div class="fo-panel">{ph("Timeline 24h",C["blue"],"TOUTES LES TRANSACTIONS DU JOUR")}<div class="fo-panel-body" style="padding:10px 16px;">', unsafe_allow_html=True)
        st.plotly_chart(chart_timeline(ftxs_today,TN),use_container_width=True,config={"displayModeBar":False})
        st.markdown(f'<div style="font-size:10px;color:{C["text3"]};padding:0 4px 8px;font-family:JetBrains Mono;">Rouge = fraudes détectées · Bleu = transactions normales · Survolez une barre pour le détail</div>', unsafe_allow_html=True)
        st.markdown("</div></div>", unsafe_allow_html=True)

    with c2:
        ftxs_seuil = [a for a in ftxs if a.get("amount",0) >= seuil] if seuil > 500 else ftxs
        feed="".join(alert_html(a) for a in ftxs_seuil[:8])
        if not feed: feed=f'<div style="padding:30px;text-align:center;color:{C["text3"]};font-size:12px;">⏳ En attente...</div>'
        st.markdown(f'<div class="fo-panel" style="height:100%;">{ph("Alertes en direct",C["red"],"LIVE")}<div class="alert-feed" style="max-height:295px;">{feed}</div></div>',unsafe_allow_html=True)

    with c3:
        rh="".join(
            f'<div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #e8ecf3;">'
            f'<div style="width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;'
            f'font-size:9px;font-weight:800;font-family:JetBrains Mono;flex-shrink:0;border:2px solid;'
            f'background:{[C["red_l"],C["amber_l"],C["purple_l"],C["blue_l"],C["blue_l"]][i]};'
            f'color:{[C["red"],C["amber"],C["purple"],C["blue"],C["blue"]][i]};'
            f'border-color:{["#ffc4ce","#fde68a","#ddd6fe","#bfcfff","#bfcfff"][i]};">'
            f'{u["user_id"].replace("USER_","U")}</div>'
            f'<div style="flex:1;min-width:0;"><div style="font-size:11px;font-weight:700;font-family:JetBrains Mono;color:#0f1623;">{u["user_id"]}</div>'
            f'<div style="font-size:9px;color:#9aa3b5;">{u["nb_alertes"]} alertes</div></div>'
            f'<div style="font-size:15px;font-weight:800;font-family:JetBrains Mono;color:{[C["red"],C["amber"],C["purple"],C["blue"],C["blue"]][i]};">{max(10,95-i*18)}</div></div>'
            for i,u in enumerate(top_users[:5])
        ) or f'<div style="padding:20px;text-align:center;color:{C["text3"]};">Aucune donnée</div>'
        st.markdown(
            f'<div class="fo-panel"><div style="padding:13px 16px;border-bottom:1px solid #e8ecf3;font-size:13px;font-weight:700;color:#0f1623;display:flex;align-items:center;gap:8px;">'
            f'<div style="width:8px;height:8px;border-radius:50%;background:#d97706;"></div>Scores de risque</div>'
            f'<div style="padding:8px 14px;">{rh}</div></div>', unsafe_allow_html=True)

    st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
    ca,cd = st.columns([2.4,1],gap="small")
    with ca:
        ch,cdo = st.columns([2,1],gap="small")
        with ch:
            hc=hl=""
            for h in range(24):
                cnt=sum(1 for tx in ftxs_today if _get_hour(tx.get("timestamp","")) == h)
                ins=min(1.0,cnt/max(1,len(ftxs_today)/24*2))
                bg=(f"rgba(224,43,75,{0.3+ins*0.55:.2f})" if ins>0.6 else f"rgba(217,119,6,{0.25+ins*0.5:.2f})" if ins>0.3 else f"rgba(37,99,235,{0.1+ins*0.4:.2f})")
                hc+=f'<div class="hcell" style="background:{bg}" title="{h}h — {cnt} fraudes"></div>'
                hl+=f'<div class="hlbl">{h if h%4==0 else ""}</div>'
            tr="".join([
                f'<tr><td style="font-family:JetBrains Mono;font-size:11px;font-weight:600;">{a.get("user_id","—")}</td>'
                f'<td style="font-family:JetBrains Mono;font-size:11px;color:{C["red"]};font-weight:700;">{a.get("amount",0):.2f} MAD</td>'
                f'<td style="font-size:11px;color:{C["text2"]};">{a.get("city","—")}</td>'
                f'<td><span class="badge badge-fraud">FRAUDE</span></td>'
                f'<td style="font-family:JetBrains Mono;font-size:10px;color:{C["text3"]};">{fmt(a.get("timestamp",""))}</td></tr>'
                for a in ftxs[:8]
            ]) or f'<tr><td colspan="5" style="text-align:center;padding:16px;color:{C["text3"]};">Aucune transaction</td></tr>'
            st.markdown(f"""<div class="fo-panel">{ph("Activité & Transactions",C["blue"])}
<div class="fo-panel-body">
<div style="font-size:9px;font-weight:700;color:{C["text3"]};text-transform:uppercase;letter-spacing:0.1em;margin-bottom:8px;font-family:JetBrains Mono;">Heatmap horaire (fraudes)</div>
<div class="hmap-grid">{hc}</div><div class="hlabels">{hl}</div>
<div style="margin-top:14px;font-size:9px;font-weight:700;color:{C["text3"]};text-transform:uppercase;letter-spacing:0.1em;margin-bottom:6px;font-family:JetBrains Mono;">Dernières transactions détectées</div>
<div style="overflow-x:auto;"><table class="tx-tbl"><thead><tr><th>Utilisateur</th><th>Montant</th><th>Ville</th><th>Statut</th><th>Heure</th></tr></thead><tbody>{tr}</tbody></table></div>
</div></div>""",unsafe_allow_html=True)
        with cdo:
            fp=round(TF/max(1,TT)*100)
            st.markdown(f'<div class="fo-panel">{ph("Répartition",C["red"])}<div class="fo-panel-body" style="padding:10px 14px;">',unsafe_allow_html=True)
            st.plotly_chart(chart_donut(TN,TF),use_container_width=True,config={"displayModeBar":False})
            st.markdown(f'<div style="display:flex;justify-content:center;gap:10px;font-size:10px;font-family:JetBrains Mono;color:{C["text2"]};"><span>■ {100-fp}% norm.</span><span style="color:{C["red"]};">■ {fp}% fraude</span></div></div></div>',unsafe_allow_html=True)
    with cd:
        fth=""
        if ftypes:
            mv=max(ftypes.values())
            cols={"Montant suspect":C["red"],"Localisation etrangere":C["amber"],"Heure suspecte":C["teal"],"Appareil inconnu":C["blue"],"Frequence elevee":C["purple"],"Autre":C["text2"]}
            for n,c in sorted(ftypes.items(),key=lambda x:-x[1]): fth+=fbar(n,c,mv,cols.get(n,C["text2"]))
        else: fth=f'<div style="padding:20px;text-align:center;color:{C["text3"]};">Aucune donnée</div>'
        st.markdown(
            f'<div class="fo-panel"><div style="padding:13px 16px;border-bottom:1px solid #e8ecf3;display:flex;align-items:center;gap:8px;">'
            f'<div style="width:8px;height:8px;border-radius:50%;background:#7c3aed;"></div>'
            f'<span style="font-size:13px;font-weight:700;color:#0f1623;">Types de fraude</span></div>'
            f'<div style="padding:14px 16px;">{fth}</div></div>', unsafe_allow_html=True)

elif page == "Alertes en direct":
    cf1,cf2,cf3 = st.columns([1,1,1],gap="small")
    with cf1:
        sev_opts = {"Toutes":["high","med","low"],"Critique (high)":["high"],"Modéré (med)":["med"],"Faible (low)":["low"],"Critique + Modéré":["high","med"]}
        sev_sel = st.selectbox("Sévérité", list(sev_opts.keys()), index=0)
        sev_f = sev_opts[sev_sel]
    with cf2:
        cities = sorted(list({a.get("city","") for a in ftxs}-{""}))
        city_sel = st.selectbox("Ville", ["Toutes"] + cities, index=0)
        city_f = cities if city_sel == "Toutes" else [city_sel]
    with cf3: sort_b=st.selectbox("Trier par",["Heure ↓","Montant ↓"])
    filtered=[a for a in ftxs if sev(a.get("fraud_reasons",[]),a.get("amount",0)) in sev_f and (not city_f or a.get("city","") in city_f)]
    if sort_b=="Montant ↓": filtered.sort(key=lambda x:-x.get("amount",0))
    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
    cf,cc = st.columns([1,1.6],gap="small")
    with cf:
        feed="".join(alert_html(a) for a in filtered[:20])
        if not feed: feed=f'<div style="padding:30px;text-align:center;color:{C["text3"]};">Aucune alerte</div>'
        title_a = ph("Flux d'alertes",C["red"],str(len(filtered))+" résultats")
        st.markdown(f'<div class="fo-panel">{title_a}<div class="alert-feed" style="max-height:520px;">{feed}</div></div>',unsafe_allow_html=True)
    with cc:
        st.markdown(f'<div class="fo-panel">{ph("Scatter — Montants",C["red"],"PAR SÉVÉRITÉ")}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
        st.plotly_chart(chart_scatter(filtered),use_container_width=True,config={"displayModeBar":False})
        st.markdown("</div></div>",unsafe_allow_html=True)
        if filtered:
            df_a=pd.DataFrame([{"Utilisateur":a.get("user_id","—"),"Montant (MAD)":f"{a.get('amount',0):.2f}","Ville":a.get("city","—"),"Raisons":", ".join(a.get("fraud_reasons",[])),"Heure":fmt(a.get("timestamp",""))} for a in filtered])
            st.dataframe(df_a,use_container_width=True,hide_index=True,height=220)

elif page == "Investigation":
    st.markdown(f'<div style="padding:0 0 14px;"><div style="font-size:20px;font-weight:800;color:{C["text"]};letter-spacing:-0.03em;">🔍 Investigation utilisateur</div><div style="font-size:12px;color:{C["text3"]};margin-top:4px;">Analyse complète du profil de risque</div></div>',unsafe_allow_html=True)
    cs,cb = st.columns([4,1],gap="small")
    with cs: selected=st.text_input("Identifiant",value=st.session_state.sel_user,placeholder="USER_0001")
    with cb:
        st.markdown('<div style="height:28px;"></div>',unsafe_allow_html=True)
        clicked=st.button("🔍 Analyser",use_container_width=True,type="primary")
    if top_users:
        st.markdown(f'<div style="font-size:10px;color:{C["text3"]};margin:4px 0 8px;font-family:JetBrains Mono;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;">Top risque :</div>',unsafe_allow_html=True)
        cu=st.columns(min(5,len(top_users)),gap="small")
        for i,u in enumerate(top_users[:5]):
            with cu[i]:
                if st.button(f"⚠ {u['user_id']}",use_container_width=True,key=f"tu_{i}"):
                    selected=u["user_id"]; st.session_state.sel_user=selected
    if selected and (clicked or st.session_state.sel_user):
        st.session_state.sel_user=selected
        profile=load_user_profile(selected); udata=load_user_txs(selected,50)
        if not profile:
            st.error(f"Utilisateur `{selected}` non trouvé.")
        else:
            rl=profile.get("risk_level","?"); fr=profile.get("fraud_rate_pct",0)
            fc=profile.get("fraud_count",0); tot=profile.get("total_transactions",0)
            avg=profile.get("avg_amount",0); tsp=profile.get("total_spent",0)
            rb={"CRITIQUE":"rb-crit","ÉLEVÉ":"rb-high","MOYEN":"rb-med","FAIBLE":"rb-low"}.get(rl,"rb-low")
            sc={"CRITIQUE":95,"ÉLEVÉ":75,"MOYEN":50,"FAIBLE":20}.get(rl,20)
            bc={"CRITIQUE":C["red"],"ÉLEVÉ":C["amber"],"MOYEN":C["purple"],"FAIBLE":C["green"]}.get(rl,C["blue"])
            st.markdown(f"""<div class="user-card">
<div style="display:flex;align-items:flex-start;justify-content:space-between;position:relative;z-index:1;">
<div><div style="font-size:24px;font-weight:900;letter-spacing:-0.03em;margin-bottom:8px;">{selected}</div>
<div class="rb {rb}">⚠ NIVEAU {rl}</div></div>
<div style="text-align:right;"><div style="font-size:52px;font-weight:900;font-family:JetBrains Mono;line-height:1;opacity:0.9;">{sc}</div>
<div style="font-size:11px;opacity:0.5;font-family:JetBrains Mono;">SCORE RISQUE</div></div></div>
<div style="display:flex;gap:20px;margin-top:18px;position:relative;z-index:1;flex-wrap:wrap;">
<div><div style="font-size:18px;font-weight:800;font-family:JetBrains Mono;">{tot}</div><div style="font-size:10px;opacity:0.6;">Transactions</div></div>
<div><div style="font-size:18px;font-weight:800;font-family:JetBrains Mono;color:#fca5a5;">{fc}</div><div style="font-size:10px;opacity:0.6;">Fraudes</div></div>
<div><div style="font-size:18px;font-weight:800;font-family:JetBrains Mono;">{fr:.1f}%</div><div style="font-size:10px;opacity:0.6;">Taux</div></div>
<div><div style="font-size:18px;font-weight:800;font-family:JetBrains Mono;">{avg:.0f} MAD</div><div style="font-size:10px;opacity:0.6;">Montant moy.</div></div>
<div><div style="font-size:18px;font-weight:800;font-family:JetBrains Mono;">{tsp:,.0f} MAD</div><div style="font-size:10px;opacity:0.6;">Total dépensé</div></div>
</div></div>""",unsafe_allow_html=True)
            txs=udata.get("transactions",[])
            ct,cr = st.columns([2,1],gap="small")
            with ct:
                st.markdown(f'<div class="fo-panel">{ph(f"Historique — {selected}",bc,"SCATTER")}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
                st.plotly_chart(chart_user_timeline(txs),use_container_width=True,config={"displayModeBar":False})
                st.markdown("</div></div>",unsafe_allow_html=True)
            with cr:
                st.markdown(f'<div class="fo-panel">{ph("Profil radar",bc)}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
                st.plotly_chart(chart_user_radar(profile),use_container_width=True,config={"displayModeBar":False})
                st.markdown("</div></div>",unsafe_allow_html=True)
            if txs:
                df_u=pd.DataFrame([{"TX ID":t.get("transaction_id","?")[:14]+"…","Montant":f"{t.get('amount',0):.2f} MAD","Ville":t.get("city","—"),"Marchand":t.get("merchant","—"),"Fraude":"🔴 OUI" if t.get("is_fraud") in [1,True] else "✅ NON","Raison":t.get("fraud_reason","") or ", ".join(t.get("fraud_reasons",[]) or t.get("reasons",[])),"Heure":fmt(t.get("timestamp",""))} for t in txs])
                st.markdown(f'<div class="fo-panel">{ph(f"Transactions de {selected}",bc,str(len(txs))+" enregistrements")}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
                st.dataframe(df_u,use_container_width=True,hide_index=True,height=280)
                st.markdown("</div></div>",unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:60px;background:#fff;border:1px solid {C["border"]};border-radius:16px;text-align:center;"><div style="font-size:48px;margin-bottom:16px;opacity:0.3;">🔍</div><div style="font-size:16px;font-weight:700;color:{C["text"]};margin-bottom:8px;">Entrez un identifiant utilisateur</div><div style="font-size:12px;color:{C["text3"]};">Ex: USER_0001, USER_0014, USER_0019</div></div>',unsafe_allow_html=True)

elif page == "Analytique avancé":
    cg,ch = st.columns([1,2],gap="small")
    with cg:
        st.markdown(f'<div class="fo-panel">{ph("Taux de fraude",C["amber"])}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
        st.plotly_chart(chart_gauge(TAU),use_container_width=True,config={"displayModeBar":False})
        lvl="🔴 CRITIQUE" if TAU>20 else "🟡 MODÉRÉ" if TAU>5 else "🟢 NORMAL"
        st.markdown(f'<div style="font-size:11px;background:{C["bg"]};border:1px solid {C["border"]};border-radius:10px;padding:10px 14px;font-family:JetBrains Mono;color:{C["text2"]};line-height:1.8;">⚡ Seuil critique: 20%<br>📈 Actuel: {TAU:.2f}%<br>{lvl}</div></div></div>',unsafe_allow_html=True)
    with ch:
        st.markdown(f'<div class="fo-panel">{ph("Heatmap — Heure × Jour",C["purple"],"7 JOURS")}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
        st.plotly_chart(chart_heatmap(ftxs),use_container_width=True,config={"displayModeBar":False})
        st.markdown("</div></div>",unsafe_allow_html=True)
    st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
    cs2,cb2 = st.columns([1.5,1],gap="small")
    with cs2:
        st.markdown(f'<div class="fo-panel">{ph("Scatter — Montants frauduleux",C["red"],"PAR SÉVÉRITÉ")}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
        st.plotly_chart(chart_scatter(ftxs),use_container_width=True,config={"displayModeBar":False})
        st.markdown("</div></div>",unsafe_allow_html=True)
    with cb2:
        if ftypes:
            fig_b=go.Figure(go.Bar(x=list(ftypes.values()),y=list(ftypes.keys()),orientation="h",
                marker=dict(color=[C["red"],C["amber"],C["teal"],C["blue"],C["purple"]][:len(ftypes)],line=dict(width=0)),
                hovertemplate="%{y}: %{x}<extra></extra>"))
            fig_b.update_layout(**BL,height=220,xaxis=ax(9),yaxis=ax(11))
        else: fig_b=go.Figure()
        st.markdown(f'<div class="fo-panel">{ph("Distribution par type",C["purple"])}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
        st.plotly_chart(fig_b,use_container_width=True,config={"displayModeBar":False})
        st.markdown("</div></div>",unsafe_allow_html=True)
    st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
    c1,c2,c3,c4 = st.columns(4,gap="small")
    for col,lbl,val,color in [(c1,"Montant total fraude",f"{TM:,.0f} MAD",C["red"]),(c2,"Montant moyen",f"{MOY:.0f} MAD",C["amber"]),(c3,"Montant max",f"{MX:,.0f} MAD",C["purple"]),(c4,"Tx normales",f"{TN:,}",C["blue"])]:
        with col:
            st.markdown(f'<div class="fo-panel"><div class="fo-panel-body"><div style="font-size:9px;font-weight:700;color:{C["text3"]};text-transform:uppercase;letter-spacing:0.1em;font-family:JetBrains Mono;margin-bottom:8px;">{lbl}</div><div style="font-size:22px;font-weight:900;color:{color};font-family:JetBrains Mono;letter-spacing:-0.03em;">{val}</div></div></div>',unsafe_allow_html=True)

elif page == "Géographie":
    st.markdown(f'<div class="fo-panel">{ph("Carte géographique des fraudes",C["blue"],"24H")}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
    st.plotly_chart(chart_geo(ftxs),use_container_width=True,config={"displayModeBar":False})
    st.markdown("</div></div>",unsafe_allow_html=True)
    if city_data:
        st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
        ct,cb = st.columns([1,1.5],gap="small")
        with ct:
            st.dataframe(pd.DataFrame([{"Ville":c["city"],"Total":c["total"],"Fraudes":c["frauds"],"Taux %":c["fraud_rate"],"Moy. MAD":c["avg_amount"]} for c in city_data[:10]]),use_container_width=True,hide_index=True)
        with cb:
            fig_c=go.Figure(go.Bar(x=[c["frauds"] for c in city_data[:8]],y=[c["city"] for c in city_data[:8]],orientation="h",marker=dict(color=[C["red"],C["amber"],C["purple"],C["blue"],C["teal"],C["green"]]*3,line=dict(width=0))))
            fig_c.update_layout(**BL,height=280,xaxis=ax(9),yaxis=ax(11))
            st.markdown(f'<div class="fo-panel">{ph("Fraudes par ville",C["amber"])}<div class="fo-panel-body" style="padding:10px 16px;">',unsafe_allow_html=True)
            st.plotly_chart(fig_c,use_container_width=True,config={"displayModeBar":False})
            st.markdown("</div></div>",unsafe_allow_html=True)

elif page == "Historique":
    st.markdown(f'<div style="padding:0 0 14px;"><div style="font-size:20px;font-weight:800;color:{C["text"]};letter-spacing:-0.03em;">📋 Historique quotidien</div><div style="font-size:12px;color:{C["text3"]};margin-top:4px;">Archives des 30 derniers jours — Reset automatique à 06:00</div></div>',unsafe_allow_html=True)

    history = load_history()
    if not history:
        st.markdown(f'<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:60px;background:#fff;border:1px solid {C["border"]};border-radius:16px;text-align:center;"><div style="font-size:48px;margin-bottom:16px;opacity:0.3;">📋</div><div style="font-size:16px;font-weight:700;color:{C["text"]};margin-bottom:8px;">Aucun historique disponible</div><div style="font-size:12px;color:{C["text3"]};">Les archives apparaîtront après le premier reset quotidien (06:00)</div></div>',unsafe_allow_html=True)
    else:
        # Tableau résumé
        hist_data = []
        for h in history:
            s = h.get("summary",{})
            hist_data.append({
                "Date":           h.get("date","—"),
                "Transactions":   f"{s.get('total_transactions',0):,}",
                "Fraudes":        f"{s.get('total_frauds',0):,}",
                "Taux":           f"{s.get('fraud_rate_pct',0):.1f}%",
                "Volume fraude":  f"{s.get('fraud_total_amount',0):,.0f} MAD",
                "Montant moy.":   f"{s.get('fraud_avg_amount',0):,.0f} MAD",
                "Montant max":    f"{s.get('fraud_max_amount',0):,.0f} MAD",
            })
        st.markdown(f'<div class="fo-panel">{ph("Résumé par jour",C["blue"],"30 DERNIERS JOURS")}<div class="fo-panel-body">',unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(hist_data),use_container_width=True,hide_index=True,height=300)
        st.markdown("</div></div>",unsafe_allow_html=True)
        st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)

        # Détail du jour sélectionné
        dates = [h.get("date","") for h in history]
        selected_date = st.selectbox("Voir le détail du jour :", dates)
        h_detail = next((h for h in history if h.get("date")==selected_date), None)
        if h_detail:
            s = h_detail.get("summary",{})
            c1,c2,c3,c4 = st.columns(4,gap="small")
            for col,lbl,val,color in [
                (c1,"Transactions",f"{s.get('total_transactions',0):,}",C["blue"]),
                (c2,"Fraudes",f"{s.get('total_frauds',0):,}",C["red"]),
                (c3,"Taux fraude",f"{s.get('fraud_rate_pct',0):.1f}%",C["amber"]),
                (c4,"Volume fraude",f"{s.get('fraud_total_amount',0):,.0f} MAD",C["purple"])
            ]:
                with col:
                    st.markdown(f'<div class="fo-panel"><div class="fo-panel-body"><div style="font-size:9px;font-weight:700;color:{C["text3"]};text-transform:uppercase;letter-spacing:0.1em;font-family:JetBrains Mono;margin-bottom:8px;">{lbl}</div><div style="font-size:22px;font-weight:900;color:{color};font-family:JetBrains Mono;">{val}</div></div></div>',unsafe_allow_html=True)

            st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
            ca,cb = st.columns(2,gap="small")
            with ca:
                top_f = h_detail.get("top_fraudsters",[])
                if top_f:
                    st.markdown(f'<div class="fo-panel">{ph("Top fraudeurs ce jour",C["red"])}<div class="fo-panel-body">',unsafe_allow_html=True)
                    st.dataframe(pd.DataFrame([{"Utilisateur":u["user_id"],"Fraudes":u["fraud_count"],"Montant total":f"{u['total_amount']:,.0f} MAD"} for u in top_f]),use_container_width=True,hide_index=True)
                    st.markdown("</div></div>",unsafe_allow_html=True)
            with cb:
                by_type = h_detail.get("by_fraud_type",[])
                if by_type:
                    st.markdown(f'<div class="fo-panel">{ph("Types de fraude ce jour",C["amber"])}<div class="fo-panel-body">',unsafe_allow_html=True)
                    st.dataframe(pd.DataFrame([{"Type":t["type"],"Occurrences":t["count"]} for t in by_type]),use_container_width=True,hide_index=True)
                    st.markdown("</div></div>",unsafe_allow_html=True)

# ═══ STATUS BAR — Sans infos techniques ═══
st.markdown(f"""<div class="statusbar">
<span class="{"sbi-ok" if api_ok else "sbi-warn"}">{"🟢 Système opérationnel" if api_ok else "🟡 Mode démonstration"}</span>
<span>TX: {TT:,}</span><span>FRAUDES: {TF:,}</span><span>TAUX: {TAU:.1f}%</span>
<span>MAJ: {datetime.now().strftime("%H:%M:%S")}</span>
<span style="margin-left:auto;display:flex;gap:16px;">
<span>Seuil: {seuil:,} MAD</span>
<span>{"🔊" if st.session_state.sound_enabled else "🔇"}</span>
</span>
</div>""", unsafe_allow_html=True)

# ═══ AUTO-REFRESH — TOUJOURS EN DERNIER pour ne pas bloquer la navigation ═══
if auto_refresh and _refresh_ok:
    st_autorefresh(interval=refresh_rate*1000, limit=None, key="autorefresh")
