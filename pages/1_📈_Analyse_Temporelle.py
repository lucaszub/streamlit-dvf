import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Analyse Temporelle - DVF",
    page_icon="📈",
    layout="wide"
)

# Fonction pour créer la connexion Snowflake
@st.cache_resource
def get_snowflake_connection():
    """Crée et retourne une connexion Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"]
        )
        return conn
    except Exception as e:
        st.error(f"Erreur de connexion à Snowflake: {e}")
        return None

# Fonction pour exécuter une requête
@st.cache_data(ttl=600)
def run_query(_conn, query):
    """Exécute une requête et retourne un DataFrame"""
    try:
        cursor = _conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        cursor.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors de l'exécution de la requête: {e}")
        return pd.DataFrame()

# Fonction pour charger les communes disponibles
@st.cache_data(ttl=3600)
def get_communes(_conn):
    """Récupère la liste des communes"""
    query = """
    SELECT DISTINCT c.COMMUNE, c.CODE_DEPARTEMENT
    FROM VALFONC_ANALYTICS.GOLD.DIM_COMMUNE c
    INNER JOIN VALFONC_ANALYTICS.GOLD.FACT_MUTATION f ON c.COMMUNE_ID = f.COMMUNE_ID
    ORDER BY c.COMMUNE
    """
    return run_query(_conn, query)

# Fonction pour charger les départements
@st.cache_data(ttl=3600)
def get_departements(_conn):
    """Récupère la liste des départements"""
    query = """
    SELECT DISTINCT c.CODE_DEPARTEMENT
    FROM VALFONC_ANALYTICS.GOLD.DIM_COMMUNE c
    INNER JOIN VALFONC_ANALYTICS.GOLD.FACT_MUTATION f ON c.COMMUNE_ID = f.COMMUNE_ID
    ORDER BY c.CODE_DEPARTEMENT
    """
    return run_query(_conn, query)

def get_postal(_conn):
    """Récupère la liste des départements"""
    query = """
    SELECT DISTINCT P.CODE_POSTAL 
    FROM VALFONC_ANALYTICS.GOLD.DIM_CODE_POSTAL P
    INNER JOIN VALFONC_ANALYTICS.GOLD.FACT_MUTATION AS M ON P.CODE_POSTAL_ID = M.CODE_POSTAL_ID
    ORDER BY P.CODE_POSTAL
    """
    return run_query(_conn, query)

# Fonction pour récupérer les données temporelles
@st.cache_data(ttl=600)
def get_temporal_data(_conn, period_type, commune=None, departement=None, type_local=None, start_date=None, end_date=None):
    """
    Récupère les données agrégées par période avec le prix médian

    Args:
        period_type: 'year', 'quarter', 'month'
        commune: filtre par commune
        departement: filtre par département
        type_local: filtre par type de local
        start_date: date de début
        end_date: date de fin
    """

    # Définir le format de période selon le type
    if period_type == "year":
        date_format = "YEAR(f.DATE_MUTATION)"
        period_label = "ANNEE"
    elif period_type == "quarter":
        date_format = "CONCAT(YEAR(f.DATE_MUTATION), '-Q', QUARTER(f.DATE_MUTATION))"
        period_label = "TRIMESTRE"
    else:  # month
        date_format = "TO_CHAR(f.DATE_MUTATION, 'YYYY-MM')"
        period_label = "MOIS"

    query = f"""
    SELECT
        {date_format} as PERIODE,
        COUNT(*) as NOMBRE_TRANSACTIONS,
        MEDIAN(f.VALEUR_FONCIERE) as PRIX_MEDIAN,
        AVG(f.VALEUR_FONCIERE) as PRIX_MOYEN,
        MIN(f.VALEUR_FONCIERE) as PRIX_MIN,
        MAX(f.VALEUR_FONCIERE) as PRIX_MAX,
        MEDIAN(f.SURFACE_REELLE_BATI) as SURFACE_MEDIANE,
        AVG(f.SURFACE_REELLE_BATI) as SURFACE_MOYENNE
    FROM VALFONC_ANALYTICS.GOLD.FACT_MUTATION f
    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_COMMUNE c ON f.COMMUNE_ID = c.COMMUNE_ID
    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_TYPE_LOCAL t ON f.TYPE_LOCAL_ID = t.TYPE_LOCAL_ID
    WHERE 1=1
        AND f.VALEUR_FONCIERE > 0
        AND f.DATE_MUTATION IS NOT NULL
    """

    if commune:
        query += f" AND c.COMMUNE = '{commune}'"
    if departement:
        query += f" AND c.CODE_DEPARTEMENT = '{departement}'"
    if type_local and type_local != "Tous":
        query += f" AND t.TYPE_LOCAL = '{type_local}'"
    if start_date:
        query += f" AND f.DATE_MUTATION >= '{start_date}'"
    if end_date:
        query += f" AND f.DATE_MUTATION <= '{end_date}'"

    query += f"""
    GROUP BY {date_format}
    ORDER BY PERIODE
    """

    return run_query(_conn, query)

# Fonction pour récupérer les données par type de bien
@st.cache_data(ttl=600)
def get_data_by_type(_conn, period_type, commune=None, departement=None, start_date=None, end_date=None):
    """Récupère les données agrégées par période et type de bien"""

    if period_type == "year":
        date_format = "YEAR(f.DATE_MUTATION)"
    elif period_type == "quarter":
        date_format = "CONCAT(YEAR(f.DATE_MUTATION), '-Q', QUARTER(f.DATE_MUTATION))"
    else:
        date_format = "TO_CHAR(f.DATE_MUTATION, 'YYYY-MM')"

    query = f"""
    SELECT
        {date_format} as PERIODE,
        t.TYPE_LOCAL,
        COUNT(*) as NOMBRE_TRANSACTIONS,
        MEDIAN(f.VALEUR_FONCIERE) as PRIX_MEDIAN
    FROM VALFONC_ANALYTICS.GOLD.FACT_MUTATION f
    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_COMMUNE c ON f.COMMUNE_ID = c.COMMUNE_ID
    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_TYPE_LOCAL t ON f.TYPE_LOCAL_ID = t.TYPE_LOCAL_ID
    WHERE 1=1
        AND f.VALEUR_FONCIERE > 0
        AND f.DATE_MUTATION IS NOT NULL
        AND t.TYPE_LOCAL IN ('MAISON', 'APPARTEMENT')
    """

    if commune:
        query += f" AND c.COMMUNE = '{commune}'"
    if departement:
        query += f" AND c.CODE_DEPARTEMENT = '{departement}'"
    if start_date:
        query += f" AND f.DATE_MUTATION >= '{start_date}'"
    if end_date:
        query += f" AND f.DATE_MUTATION <= '{end_date}'"

    query += f"""
    GROUP BY {date_format}, t.TYPE_LOCAL
    ORDER BY PERIODE, t.TYPE_LOCAL
    """

    return run_query(_conn, query)

# Interface principale
def main():
    st.title("📈 Analyse Temporelle des Valeurs Foncières")
    st.markdown("Analyse de l'évolution des prix médians par période")
    st.markdown("---")

    # Connexion à Snowflake
    conn = get_snowflake_connection()

    if conn is None:
        st.warning("⚠️ Impossible de se connecter à Snowflake. Veuillez vérifier votre configuration dans .streamlit/secrets.toml")
        return

    # Barre latérale avec filtres
    st.sidebar.header("🔍 Filtres")

    # Sélection du type de période
    st.sidebar.subheader("Granularité temporelle")
    period_type = st.sidebar.radio(
        "Période d'analyse",
        options=["month", "quarter", "year"],
        format_func=lambda x: {"month": "📅 Mois", "quarter": "📊 Trimestre", "year": "📆 Année"}[x],
        index=1
    )

    # Filtres géographiques
    st.sidebar.subheader("Localisation")

    # Charger les départements
    departements_df = get_departements(conn)
    departements_list = ["Tous"] + sorted(departements_df["CODE_DEPARTEMENT"].tolist())
    selected_departement = st.sidebar.selectbox("Département", departements_list)
    
    # Charger les codes postaux
    code_postal_df = get_postal(conn)
    code_postal_list = ["Tous"] + sorted(code_postal_df["CODE_POSTAL"].tolist())
    selected_code_postal = st.sidebar.selectbox("Code Postal", code_postal_list)

    # Filtre commune (filtre par département ET/OU code postal)
    selected_commune = None
    # Récupérer toutes les communes disponibles
    communes_df = get_communes(conn)

    # Appliquer filtre département si nécessaire
    if selected_departement != "Tous":
        communes_df = communes_df[communes_df["CODE_DEPARTEMENT"] == selected_departement]

        # Récupérer les codes postaux valides pour le département sélectionné
        query_cp_dept = f"""
        SELECT DISTINCT p.CODE_POSTAL
        FROM VALFONC_ANALYTICS.GOLD.DIM_CODE_POSTAL p
        INNER JOIN VALFONC_ANALYTICS.GOLD.FACT_MUTATION m ON p.CODE_POSTAL_ID = m.CODE_POSTAL_ID
        INNER JOIN VALFONC_ANALYTICS.GOLD.DIM_COMMUNE c ON m.COMMUNE_ID = c.COMMUNE_ID
        WHERE c.CODE_DEPARTEMENT = '{selected_departement}'
        ORDER BY p.CODE_POSTAL
        """
        cp_dept_df = run_query(conn, query_cp_dept)
        valid_cp = set(cp_dept_df["CODE_POSTAL"].tolist()) if not cp_dept_df.empty else set()

        # Si le code postal sélectionné n'appartient pas au département, on l'ignore (évite le mélange 35 vs 45)
        if selected_code_postal != "Tous" and selected_code_postal not in valid_cp:
            st.sidebar.warning("Le code postal sélectionné n'appartient pas au département choisi. Le filtre code postal est ignoré.")
            selected_code_postal_effective = "Tous"
        else:
            selected_code_postal_effective = selected_code_postal
    else:
        selected_code_postal_effective = selected_code_postal

    # Appliquer filtre code postal si nécessaire (obtenir les communes liées au code postal)
    if selected_code_postal_effective != "Tous":
        query_communes_par_cp = f"""
        SELECT DISTINCT c.COMMUNE, c.CODE_DEPARTEMENT
        FROM VALFONC_ANALYTICS.GOLD.DIM_CODE_POSTAL p
        INNER JOIN VALFONC_ANALYTICS.GOLD.FACT_MUTATION m ON p.CODE_POSTAL_ID = m.CODE_POSTAL_ID
        INNER JOIN VALFONC_ANALYTICS.GOLD.DIM_COMMUNE c ON m.COMMUNE_ID = c.COMMUNE_ID
        WHERE p.CODE_POSTAL = '{selected_code_postal_effective}'
        ORDER BY c.COMMUNE
        """
        communes_par_cp = run_query(conn, query_communes_par_cp)
        # Intersecter avec les communes déjà filtrées par département (si applicable)
        if not communes_df.empty:
            communes_df = communes_df[communes_df["COMMUNE"].isin(communes_par_cp["COMMUNE"].tolist())]
        else:
            communes_df = communes_par_cp

    # Construire la liste des communes à afficher
    if not communes_df.empty:
        communes_list = ["Toutes"] + sorted(communes_df["COMMUNE"].tolist())
        selected_commune = st.sidebar.selectbox("Commune", communes_list)
        if selected_commune == "Toutes":
            selected_commune = None
    else:
        # Pas de communes disponibles pour les filtres choisis
        st.sidebar.info("Aucune commune disponible pour le filtre sélectionné")
        selected_commune = None

    # Filtre type de bien
    st.sidebar.subheader("Type de bien")
    types_locaux = ["Tous", "MAISON", "APPARTEMENT", "LOCAL INDUSTRIEL. COMMERCIAL OU ASSIMILÉ"]
    selected_type = st.sidebar.selectbox("Type de bien", types_locaux)

    # Filtre date
    st.sidebar.subheader("Période")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Date début", value=None)
    with col2:
        end_date = st.date_input("Date fin", value=None)

    # Bouton d'analyse
    if st.sidebar.button("🔎 Analyser", type="primary"):
        departement_filter = None if selected_departement == "Tous" else selected_departement

        with st.spinner("Chargement des données..."):
            df = get_temporal_data(
                conn,
                period_type,
                selected_commune,
                departement_filter,
                selected_type,
                start_date,
                end_date
            )

        if df.empty:
            st.warning("Aucune transaction trouvée avec ces critères")
            return

        # Conversion des types
        df["PRIX_MEDIAN"] = pd.to_numeric(df["PRIX_MEDIAN"], errors="coerce")
        df["PRIX_MOYEN"] = pd.to_numeric(df["PRIX_MOYEN"], errors="coerce")
        df["NOMBRE_TRANSACTIONS"] = pd.to_numeric(df["NOMBRE_TRANSACTIONS"], errors="coerce")

        # Métriques globales
        st.header("📊 Vue d'ensemble")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_transactions = df["NOMBRE_TRANSACTIONS"].sum()
            st.metric("Total transactions", f"{int(total_transactions):,}")

        with col2:
            prix_median_global = df["PRIX_MEDIAN"].median()
            st.metric("Prix médian global", f"{prix_median_global:,.0f} €")

        with col3:
            prix_moyen_global = df["PRIX_MOYEN"].mean()
            st.metric("Prix moyen global", f"{prix_moyen_global:,.0f} €")

        with col4:
            nb_periodes = len(df)
            period_label = {"month": "mois", "quarter": "trimestres", "year": "années"}[period_type]
            st.metric("Nombre de périodes", f"{nb_periodes} {period_label}")

        st.markdown("---")

        # Graphique principal : Évolution du prix médian
        st.header("📈 Évolution du prix médian")

        fig_median = go.Figure()

        fig_median.add_trace(go.Scatter(
            x=df["PERIODE"],
            y=df["PRIX_MEDIAN"],
            mode='lines+markers',
            name='Prix médian',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8)
        ))

        fig_median.add_trace(go.Scatter(
            x=df["PERIODE"],
            y=df["PRIX_MOYEN"],
            mode='lines+markers',
            name='Prix moyen',
            line=dict(color='#ff7f0e', width=2, dash='dash'),
            marker=dict(size=6)
        ))

        fig_median.update_layout(
            xaxis_title="Période",
            yaxis_title="Prix (€)",
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=500
        )

        st.plotly_chart(fig_median, use_container_width=True)

        # Graphiques secondaires
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📊 Nombre de transactions")
            fig_transactions = px.bar(
                df,
                x="PERIODE",
                y="NOMBRE_TRANSACTIONS",
                labels={"PERIODE": "Période", "NOMBRE_TRANSACTIONS": "Nombre de transactions"}
            )
            fig_transactions.update_traces(marker_color='#2ca02c')
            st.plotly_chart(fig_transactions, use_container_width=True)

        with col2:
            st.subheader("📉 Variation du prix médian")
            df_variation = df.copy()
            df_variation["VARIATION_PCT"] = df_variation["PRIX_MEDIAN"].pct_change() * 100

            fig_variation = go.Figure()
            colors = ['red' if x < 0 else 'green' for x in df_variation["VARIATION_PCT"]]

            fig_variation.add_trace(go.Bar(
                x=df_variation["PERIODE"],
                y=df_variation["VARIATION_PCT"],
                marker_color=colors,
                name='Variation (%)'
            ))

            fig_variation.update_layout(
                xaxis_title="Période",
                yaxis_title="Variation (%)",
                showlegend=False
            )

            st.plotly_chart(fig_variation, use_container_width=True)

        st.markdown("---")

        # Analyse par type de bien
        st.header("🏘️ Comparaison par type de bien")

        with st.spinner("Chargement de la comparaison par type..."):
            df_by_type = get_data_by_type(
                conn,
                period_type,
                selected_commune,
                departement_filter,
                start_date,
                end_date
            )

        if not df_by_type.empty:
            df_by_type["PRIX_MEDIAN"] = pd.to_numeric(df_by_type["PRIX_MEDIAN"], errors="coerce")

            fig_types = px.line(
                df_by_type,
                x="PERIODE",
                y="PRIX_MEDIAN",
                color="TYPE_LOCAL",
                markers=True,
                labels={"PERIODE": "Période", "PRIX_MEDIAN": "Prix médian (€)", "TYPE_LOCAL": "Type de bien"}
            )

            fig_types.update_layout(
                hovermode='x unified',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=500
            )

            st.plotly_chart(fig_types, use_container_width=True)

            # Statistiques par type
            st.subheader("📋 Statistiques par type de bien")

            stats_by_type = df_by_type.groupby("TYPE_LOCAL").agg({
                "NOMBRE_TRANSACTIONS": "sum",
                "PRIX_MEDIAN": "median"
            }).reset_index()

            stats_by_type.columns = ["Type de bien", "Total transactions", "Prix médian"]
            stats_by_type["Prix médian"] = stats_by_type["Prix médian"].apply(lambda x: f"{x:,.0f} €")
            stats_by_type["Total transactions"] = stats_by_type["Total transactions"].apply(lambda x: f"{int(x):,}")

            st.dataframe(stats_by_type, use_container_width=True, hide_index=True)

        st.markdown("---")

        # Tableau détaillé
        st.header("📋 Données détaillées")

        df_display = df.copy()
        df_display["PRIX_MEDIAN"] = df_display["PRIX_MEDIAN"].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "N/A")
        df_display["PRIX_MOYEN"] = df_display["PRIX_MOYEN"].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "N/A")
        df_display["PRIX_MIN"] = df_display["PRIX_MIN"].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "N/A")
        df_display["PRIX_MAX"] = df_display["PRIX_MAX"].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "N/A")
        df_display["NOMBRE_TRANSACTIONS"] = df_display["NOMBRE_TRANSACTIONS"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")

        df_display.columns = ["Période", "Nombre transactions", "Prix médian", "Prix moyen", "Prix min", "Prix max", "Surface médiane", "Surface moyenne"]

        st.dataframe(df_display, use_container_width=True, height=400)

        # Export CSV
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Télécharger les données (CSV)",
            data=csv,
            file_name=f"dvf_analyse_temporelle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
