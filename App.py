
# app.py — Tableau de Bord Suivi Fabrication (TOR, synchronisation, filtres, couleurs)
# Exécuter : streamlit run App.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from datetime import datetime
import os

# -------------------------------------------------
# 0) Configuration & thème
# -------------------------------------------------
st.set_page_config(page_title="Suivi Fabrication", layout="wide")
st.title("📊 Tableau de Bord — Suivi Fabrication Structure Métallique")

# Couleurs par Étape (pour cohérence visuelle)
STEP_COLORS = {
    "Préparation": "#1f77b4",  # bleu
    "Assemblage": "#ff7f0e",   # orange
    "Traitement de surface": "#2ca02c",  # vert
    "Finalisation": "#d62728",  # rouge
    "None": "#7f7f7f"
}

# Étapes et pondérations (pour RowProgress% global)
STEPS_ORDER = ["Préparation", "Assemblage", "Traitement de surface", "Finalisation"]
STEP_RANK = {s: i for i, s in enumerate(STEPS_ORDER)}  # ordre pour logique TOR
PROGRESS_MAP = {
    "Préparation": 0.25,
    "Assemblage": 0.60,
    "Traitement de surface": 0.85,
    "Finalisation": 1.00,
    "None": 0.00
}



import os
import streamlit as st

# -------------------------------
# Helper: récupérer le secret admin
# -------------------------------
def _get_admin_secret() -> str:
    # 1) variable d'environnement (simple en local)
    env_val = os.getenv("ADMIN_PASSWORD")
    if env_val:
        return env_val
    # 2) secrets (Cloud ou secrets.toml)
    try:
        return str(st.secrets["ADMIN_PASSWORD"])
    except Exception:
        return ""  # aucun secret → mode public par défaut

DEBUG_PASSWORD_HINTS = os.getenv("DEBUG_PASSWORD_HINTS", "false").lower() == "true"

# -------------------------------
# État et bandeau d'accès
# -------------------------------
st.sidebar.header("🔒 Accès")
if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False

_admin_secret = _get_admin_secret()
st.sidebar.caption(f"Mode actuel : {'Admin ✅' if st.session_state['is_admin'] else 'Public'}")

# -------------------------------
# Rendu conditionnel :
# - mode public → formulaire (avec bouton de validation)
# - mode admin  → bouton de déconnexion seulement
# -------------------------------
if not st.session_state["is_admin"]:
    if not _admin_secret:
        st.sidebar.warning(
            "Aucun mot de passe admin n’est configuré.\n"
            "→ Local : ADMIN_PASSWORD (env var) ou .streamlit/secrets.toml\n"
            "→ Cloud : Settings → Secrets → ADMIN_PASSWORD"
        )

    with st.sidebar.form(key="admin_form", clear_on_submit=False):
        pwd_try = st.text_input(
            "Mot de passe admin",
            type="password",
            help="Astuce : vérifie majuscules/minuscules et les espaces.",
            placeholder="Saisir le mot de passe"
        )
        activate = st.form_submit_button("Activer le mode admin")

    if activate:
        entered = (pwd_try or "").strip()
        expected = _admin_secret.strip()
        if expected and entered == expected:
            st.session_state["is_admin"] = True
            st.sidebar.success("Mode admin activé ✅")
            st.rerun()  # ✅ API stable
        else:
            st.session_state["is_admin"] = False
            if not _admin_secret:
                st.sidebar.error("Mot de passe incorrect ❌ — aucun secret ADMIN_PASSWORD n’est configuré.")
            else:
                st.sidebar.error("Mot de passe incorrect ❌ — vérifie la casse et les espaces.")
                if DEBUG_PASSWORD_HINTS:
                    st.sidebar.info(f"Indice (non sensible) : longueur saisie = {len(entered)} caractères.")
else:
    # Mode admin : on n'affiche PAS le champ mot de passe,
    # seulement un bouton de déconnexion
    if st.sidebar.button("Se déconnecter"):
        st.session_state["is_admin"] = False
        st.sidebar.info("Mode public activé")
        st.rerun()  # ✅ API stable

# Flag unique pour la suite
is_admin = st.session_state["is_admin"]


# -------------------------------------------------
# 1) Chargement des données
# -------------------------------------------------
DEFAULT_XLSX = "Structural_data.xlsx"  # même dossier que l'application


# NOTE: Les fonctions @st.cache_data ne doivent pas muter le df d'entrée in-place.
# Toujours retourner un nouveau DataFrame (copy / assign) pour éviter les incohérences de cache.

@st.cache_data(show_spinner=False)
def load_excel(path_or_buffer):
    # lit la première feuille automatiquement (évite les erreurs de nom)
    return pd.read_excel(path_or_buffer, engine="openpyxl")

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Colonnes attendues dans le fichier
    required = ["PHASE", "ASSEMBLY NO.", "PART NO.", "TOT MASS (Kg)"]
    for c in required:
        if c not in df.columns:
            st.error(f"⚠️ Colonne manquante dans Excel : '{c}'")
            st.stop()
    # Harmonisation
    df["PHASE"] = df["PHASE"].astype(str)
    df["TOT MASS (Kg)"] = pd.to_numeric(df["TOT MASS (Kg)"], errors="coerce").fillna(0.0)
    # Colonnes d'application (si absentes)
    if "Etape" not in df.columns:
        df["Etape"] = "None"
    if "RowProgress%" not in df.columns:
        df["RowProgress%"] = 0.0
    if "CompletedMass_Row" not in df.columns:
        df["CompletedMass_Row"] = 0.0
    return df

# Charger fichier du dossier ou via upload
st.sidebar.header("🛠 Données")

if is_admin:
    uploaded = st.sidebar.file_uploader(
        "Importer un Excel (.xlsx)", type=["xlsx"],
        help="Optionnel : sinon 'Structural_data.xlsx' sera utilisé."
    )
else: 
    uploaded = None  # pas d'upload en mode public

try:
    if uploaded is not None:
        df_loaded = load_excel(uploaded)
        source_label = f"Fichier importé : {uploaded.name}"
        current_source_key = f"upload::{uploaded.name}"
    else:
        df_loaded = load_excel(DEFAULT_XLSX)
        source_label = f"Fichier local : {DEFAULT_XLSX}"
        current_source_key = f"local::{DEFAULT_XLSX}"
except Exception as e:
    st.error(f"❌ Échec de chargement : {e}")
    st.stop()

df_loaded = ensure_columns(df_loaded)
st.caption(f"✅ {source_label}")

# -------------------------------------------------
# 1.b) Initialisation état partagé (SESSION)
# -------------------------------------------------
if "df" not in st.session_state:
    st.session_state["df"] = df_loaded.copy()
    st.session_state["source_key"] = current_source_key
elif st.session_state.get("source_key") != current_source_key:
    st.session_state["df"] = df_loaded.copy()
    st.session_state["source_key"] = current_source_key

if "refresh_needed" not in st.session_state:
    st.session_state["refresh_needed"] = False

# -------------------------------------------------
# 2) Fonctions utilitaires (TOR & calculs)
# -------------------------------------------------
def recompute_progress(df: pd.DataFrame) -> pd.DataFrame:
    """Recalcule RowProgress% (pondéré par Étape) et CompletedMass_Row (masse × RowProgress%)."""
    out = df.copy()
    out["RowProgress%"] = out["Etape"].map(PROGRESS_MAP).fillna(0.0)
    out["CompletedMass_Row"] = out["TOT MASS (Kg)"] * out["RowProgress%"]
    return out


@st.cache_data(show_spinner=False)
def step_advancement(df: pd.DataFrame) -> pd.DataFrame:
    """Avancement par étape TOR."""
    total_mass = df["TOT MASS (Kg)"].sum()
    rows = []
    for step in STEPS_ORDER:
        treated_mass = df.loc[
            df["Etape"].map(lambda s: STEP_RANK.get(s, -1)) >= STEP_RANK[step],
            "TOT MASS (Kg)"
        ].sum()
        pct = (treated_mass / total_mass) * 100 if total_mass > 0 else 0.0
        rows.append({"Etape": step, "CompletedMass": treated_mass, "Avancement%": pct})
    return pd.DataFrame(rows)

@st.cache_data(show_spinner=False)
def phase_advancement(df: pd.DataFrame) -> pd.DataFrame:
    """Avancement par PHASE."""
    rows = []
    for phase in sorted(df["PHASE"].unique()):
        phase_total_mass = df.loc[df["PHASE"] == phase, "TOT MASS (Kg)"].sum()
        treated_mass = df.loc[df["PHASE"] == phase, "CompletedMass_Row"].sum()
        pct = (treated_mass / phase_total_mass) * 100 if phase_total_mass > 0 else 0.0
        rows.append({"PHASE": phase, "CompletedMass": treated_mass, "Avancement%": pct})
    return pd.DataFrame(rows)

@st.cache_data(show_spinner=False)
def assembly_table(df: pd.DataFrame) -> pd.DataFrame:
    """Vue par assemblage."""
    agg = df.groupby(["PHASE", "ASSEMBLY NO."]).agg(
        AssemblyMass=("TOT MASS (Kg)", "sum"),
        EtapeRank=("Etape", lambda s: min([STEP_RANK.get(x, -1) for x in s]) if len(s) else -1)
    ).reset_index()
    inv_rank = {v: k for k, v in STEP_RANK.items()}
    agg["EtapeAsm"] = agg["EtapeRank"].map(inv_rank).fillna("None")
    return agg[["PHASE", "ASSEMBLY NO.", "AssemblyMass", "EtapeAsm"]]

# Première recomputation
st.session_state["df"] = recompute_progress(st.session_state["df"])

# -------------------------------------------------
# 3) Onglets principaux
# -------------------------------------------------

if is_admin:
    tab_edit, tab_kpi, tab_graph, tab_export = st.tabs(["✏️ Édition", "📈 KPI", "📊 Graphiques", "📤 Export"])
else:
    tab_kpi, tab_graph = st.tabs(["📈 KPI", "📊 Graphiques"])

# -------------------------------------------------
# ✏️ 3.1 Édition
# -------------------------------------------------
if is_admin:
    with tab_edit:
        st.subheader("Mise à jour des Étapes (TOR) — synchronisée")
        phases = sorted(st.session_state["df"]["PHASE"].unique())
        ph_sel = st.multiselect("Filtrer par PHASE", options=phases, default=phases)
        search_asm = st.text_input("🔍 Rechercher Assemblage / Pièce (contient)", value="")
    
        def filter_view(_df: pd.DataFrame) -> pd.DataFrame:
            _view = _df[_df["PHASE"].isin(ph_sel)]
            if search_asm.strip():
                pat = search_asm.strip().lower()
                mask = (
                        _view["ASSEMBLY NO."].astype(str).str.lower().str.contains(pat, na=False)
                        | _view["PART NO."].astype(str).str.lower().str.contains(pat, na=False)
                )
                _view = _view[mask]
            return _view
    
        sub_tab_items, sub_tab_asm = st.tabs(["Tableau Normal", "Tableau par Assemblage"])
    
        # --- Tableau Normal (pièces)
        with sub_tab_items:
            st.markdown("**Éditer l’étape par pièce** (triable, filtrable)")
            view_items = filter_view(st.session_state["df"])
            edit_cols = ["PHASE", "ASSEMBLY NO.", "PART NO.", "TOT MASS (Kg)", "Etape"]
            df_edit_items = view_items[edit_cols].copy()
            updated_items = st.data_editor(
                df_edit_items,
                key="edit_items",
                hide_index=True,
                use_container_width=True,
                column_config={
                    "PHASE": st.column_config.TextColumn("PHASE", disabled=True),
                    "ASSEMBLY NO.": st.column_config.TextColumn("ASSEMBLY NO.", disabled=True),
                    "PART NO.": st.column_config.TextColumn("PART NO.", disabled=True),
                    "TOT MASS (Kg)": st.column_config.NumberColumn("TOT MASS (Kg)", disabled=True),
                    "Etape": st.column_config.SelectboxColumn(options=STEPS_ORDER + ["None"], required=True)
                }
            )
            if st.button("🔄 Synchroniser (pièces)", key="sync_items_btn"):
                key_cols = ["ASSEMBLY NO.", "PART NO."]
                # updated_map = updated_items[key_cols + ["Etape"]].drop_duplicates()
                updated_map = (
                    updated_items[key_cols + ["Etape"]]
                    .drop_duplicates()
                    .assign(Etape=lambda s: s["Etape"].fillna("None"))
                )
    
                base = st.session_state["df"].drop(columns=["Etape"])
                st.session_state["df"] = (
                    base.merge(updated_map, on=key_cols, how="left")
                    .assign(Etape=lambda x: x["Etape"].fillna("None"))
                )
    
                st.session_state["df"] = recompute_progress(st.session_state["df"])
                st.success("✅ Synchronisation effectuée (pièces → global)")
                st.session_state["refresh_needed"] = True
                st.rerun()
    
        # --- Tableau par Assemblage
        with sub_tab_asm:
            st.markdown("**Éditer l’étape par Assemblage** (écrase toutes les pièces — logique stricte)")
            df_asm = assembly_table(st.session_state["df"])
            df_asm_view = df_asm[df_asm["PHASE"].isin(ph_sel)].copy()
            if search_asm.strip():
                pat = search_asm.strip().lower()
                mask_asm = df_asm_view["ASSEMBLY NO."].astype(str).str.lower().str.contains(pat, na=False)
                df_asm_view = df_asm_view[mask_asm]
    
            df_asm_view = df_asm_view.rename(columns={"EtapeAsm": "Etape"})
            df_edit_asm = df_asm_view[["PHASE", "ASSEMBLY NO.", "AssemblyMass", "Etape"]].copy()
            updated_asm = st.data_editor(
                df_edit_asm,
                key="edit_asm",
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Etape": st.column_config.SelectboxColumn(options=STEPS_ORDER + ["None"], required=True),
                    "AssemblyMass": st.column_config.NumberColumn("Masse Assemblage (Kg)", disabled=True)
                }
            )
            if st.button("🔄 Synchroniser (assemblage)", key="sync_asm_btn"):
                asm_step_map = updated_asm[["ASSEMBLY NO.", "Etape"]].drop_duplicates()
                for _, row in asm_step_map.iterrows():
                    asm = row["ASSEMBLY NO."]
                    step = row["Etape"]
                    st.session_state["df"].loc[st.session_state["df"]["ASSEMBLY NO."] == asm, "Etape"] = step
                st.session_state["df"] = recompute_progress(st.session_state["df"])
                st.success("✅ Synchronisation effectuée (assemblage → pièces)")
                st.session_state["refresh_needed"] = True
                st.rerun()  # Force la réexécution de l'app avec les données mises à jour
            if st.button("🔄 Actualiser le tableau des assemblages", key="refresh_asm_btn"):
                df_asm = assembly_table(st.session_state["df"])
                df_asm_view = df_asm[df_asm["PHASE"].isin(ph_sel)].copy()
                st.info("✅ Tableau des assemblages mis à jour")

# -------------------------------------------------
# 📈 3.2 KPI
# -------------------------------------------------
with tab_kpi:
    st.subheader("Indicateurs Globaux")
    total_mass = float(st.session_state["df"]["TOT MASS (Kg)"].sum())
    completed_global_mass = float(st.session_state["df"]["CompletedMass_Row"].sum())
    progress_global = (completed_global_mass / total_mass) * 100 if total_mass > 0 else 0.0
    k1, k2, k3 = st.columns(3)
    k1.metric("Masse Totale (Kg)", f"{total_mass:,.2f}")
    k2.metric("Masse Terminée (Kg)", f"{completed_global_mass:,.2f}")
    k3.metric("Avancement Global", f"{progress_global:.2f}%")

    gauge_color = "green" if progress_global >= 80 else ("orange" if progress_global >= 50 else "red")
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=progress_global,
        title={'text': "Avancement Global (%)"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': gauge_color},
            'steps': [
                {'range': [0, 50], 'color': '#ffd6d6'},
                {'range': [50, 80], 'color': '#ffe9b5'},
                {'range': [80, 100], 'color': '#d6f5d6'},
            ]
        }
    ))
    st.plotly_chart(fig_gauge, use_container_width=True)
    st.divider()
    st.subheader("Avancement par Étape (TOR)")
    df_steps = step_advancement(st.session_state["df"])
    st.dataframe(
        df_steps.rename(columns={
            "Etape": "Étape",
            "CompletedMass": "Masse traitée (Kg)",
            "Avancement%": "Avancement (%)"
        }),
        use_container_width=True
    )
    fig_bar_steps = px.bar(
        df_steps,
        x="Etape", y="Avancement%",
        color="Etape",
        color_discrete_map=STEP_COLORS,
        text="Avancement%",
        title="Avancement par Étape (%)"
    )
    fig_bar_steps.update_traces(texttemplate="%{text:.2f}%", textposition="outside", marker_line_color="#333", marker_line_width=0.)
    fig_bar_steps.update_yaxes(title="%", range=[0, 100])
    st.plotly_chart(fig_bar_steps, use_container_width=True)
    st.divider()
    st.subheader("Avancement par PHASE")
    df_phase = phase_advancement(st.session_state["df"])
    st.dataframe(
        df_phase.rename(columns={
            "PHASE": "Phase",
            "CompletedMass": "Masse traitée (Kg)",
            "Avancement%": "Avancement (%)"
        }),
        use_container_width=True
    )
    fig_bar_phase = px.bar(
        df_phase,
        x="PHASE", y="Avancement%",
        color="PHASE",
        text="Avancement%",
        title="Avancement par PHASE (%)"
    )
    fig_bar_phase.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig_bar_phase.update_yaxes(title="%", range=[0, 100])
    st.plotly_chart(fig_bar_phase, use_container_width=True)

# -------------------------------------------------
# 📊 3.3 Graphiques
# -------------------------------------------------
with tab_graph:
    st.subheader("Diagramme S — Progression cumulée par Étape (TOR)")
    df_steps = step_advancement(st.session_state["df"]).copy()
    # df_steps["Avancement%"] est déjà cumulatif par étape
    df_steps["Cumul_Masse"] = df_steps["CompletedMass"]  # déjà cumulatif (par construction)

    fig_s = go.Figure()
    fig_s.add_trace(go.Scatter(
        x=df_steps["Etape"], y=df_steps["Avancement%"],
        mode="lines+markers", name="Avancement cumulé (%)",
        line=dict(width=3, color=STEP_COLORS.get("Préparation", "#1f77b4"))
    ))
    fig_s.add_trace(go.Bar(
        x=df_steps["Etape"], y=df_steps["Cumul_Masse"],
        name="Masse cumulée (Kg)", marker_color="#9ecae1", opacity=0.6, yaxis="y2"
    ))
    fig_s.update_layout(
        title="Diagramme S — % cumulé & masse cumulée",
        yaxis=dict(title="% cumulé", range=[0, 100]),
        yaxis2=dict(title="Masse (Kg)", overlaying="y", side="right"),
        legend=dict(orientation="h")
    )
    st.plotly_chart(fig_s, use_container_width=True)

# -------------------------------------------------
# 📤 3.4 Export
# -------------------------------------------------
if is_admin:
    with tab_export:
        st.subheader("Exporter le fichier modifié")
        c1, c2 = st.columns(2)
        with c1:
            confirm_overwrite = st.checkbox("Je confirme l'écrasement du fichier local", value=False)
            if st.button("💾 Enregistrer (backup + écrasement du fichier local)", type="primary", disabled=not confirm_overwrite):
                backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                try:
                    st.session_state["df"].to_excel(backup_name, index=False, engine="openpyxl")
                    st.session_state["df"].to_excel(DEFAULT_XLSX, index=False, engine="openpyxl")
                    st.success(f"✅ Sauvegardé. Backup créé : {backup_name} — Fichier local mis à jour : {DEFAULT_XLSX}")
                except Exception as e:
                    st.error(f"❌ Échec de la sauvegarde : {e}")
    
        with c2:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as w:
                st.session_state["df"].to_excel(w, index=False, sheet_name="Données")
            buffer.seek(0)
            st.download_button(
                label="⬇️ Télécharger (Excel modifié)",
                data=buffer,
                file_name=f"Suivi_Fabrication_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

