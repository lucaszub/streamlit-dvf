import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Analyse DVF - Valeurs Foncières",
    page_icon="🏠",
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

# Fonction pour charger les rues d'une commune
@st.cache_data(ttl=3600)
def get_rues(_conn, commune):
    """Récupère la liste des rues pour une commune donnée"""
    query = f"""
    SELECT DISTINCT a.VOIE, a.TYPE_DE_VOIE
    FROM VALFONC_ANALYTICS.GOLD.DIM_ADDRESS a
    INNER JOIN VALFONC_ANALYTICS.GOLD.FACT_MUTATION f ON a.ADDRESS_ID = f.ADDRESS_ID
    WHERE a.COMMUNE = '{commune}'
    AND a.VOIE IS NOT NULL
    ORDER BY a.VOIE
    """
    return run_query(_conn, query)

# Fonction pour récupérer les données de mutations
@st.cache_data(ttl=600)
def get_mutations(_conn, commune=None, rue=None, type_local=None):
    """Récupère les données de mutations avec filtres"""
    query = """
    SELECT
        f.DATE_MUTATION,
        c.COMMUNE,
        c.CODE_DEPARTEMENT,
        a.VOIE,
        a.TYPE_DE_VOIE,
        a.NO_VOIE,
        a.CODE_POSTAL,
        t.TYPE_LOCAL,
        f.NATURE_MUTATION,
        f.VALEUR_FONCIERE,
        f.SURFACE_REELLE_BATI,
        f.SURFACE_TERRAIN,
        f.NOMBRE_PIECES_PRINCIPALES
    FROM VALFONC_ANALYTICS.GOLD.FACT_MUTATION f
    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_COMMUNE c ON f.COMMUNE_ID = c.COMMUNE_ID
    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_ADDRESS a ON f.ADDRESS_ID = a.ADDRESS_ID
    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_TYPE_LOCAL t ON f.TYPE_LOCAL_ID = t.TYPE_LOCAL_ID
    WHERE 1=1
    """

    if commune:
        query += f" AND c.COMMUNE = '{commune}'"
    if rue:
        query += f" AND a.VOIE = '{rue}'"
    if type_local and type_local != "Tous":
        query += f" AND t.TYPE_LOCAL = '{type_local}'"

    query += " ORDER BY f.DATE_MUTATION DESC LIMIT 5000"

    return run_query(_conn, query)

# Interface principale
def main():
    st.title("🏠 Analyse des Valeurs Foncières (DVF)")
    st.markdown("---")

    # Connexion à Snowflake
    conn = get_snowflake_connection()

    if conn is None:
        st.warning("⚠️ Impossible de se connecter à Snowflake. Veuillez vérifier votre configuration dans .streamlit/secrets.toml")
        st.info("""
        Pour configurer la connexion, créez un fichier `.streamlit/secrets.toml` avec :
        ```toml
        [snowflake]
        user = "votre_user"
        password = "votre_password"
        account = "votre_account"
        warehouse = "votre_warehouse"
        database = "VALFONC_ANALYTICS"
        schema = "GOLD"
        ```
        """)
        return

    # Barre latérale avec filtres
    st.sidebar.header("🔍 Filtres de recherche")

    # Chargement des communes
    communes_df = get_communes(conn)

    if communes_df.empty:
        st.error("Aucune donnée disponible")
        return

    communes_list = ["Toutes"] + communes_df["COMMUNE"].tolist()
    selected_commune = st.sidebar.selectbox("Sélectionner une commune", communes_list)

    # Filtre rue (si commune sélectionnée)
    selected_rue = None
    if selected_commune != "Toutes":
        rues_df = get_rues(conn, selected_commune)
        if not rues_df.empty:
            rues_list = ["Toutes"] + rues_df["VOIE"].tolist()
            selected_rue = st.sidebar.selectbox("Sélectionner une rue", rues_list)
            if selected_rue == "Toutes":
                selected_rue = None

    # Filtre type de local
    types_locaux = ["Tous", "MAISON", "APPARTEMENT", "LOCAL INDUSTRIEL. COMMERCIAL OU ASSIMILÉ"]
    selected_type = st.sidebar.selectbox("Type de bien", types_locaux)

    # Bouton de recherche
    if st.sidebar.button("🔎 Rechercher", type="primary"):
        commune_filter = None if selected_commune == "Toutes" else selected_commune

        # Chargement des données
        with st.spinner("Chargement des données..."):
            df = get_mutations(conn, commune_filter, selected_rue, selected_type)

        if df.empty:
            st.warning("Aucune transaction trouvée avec ces critères")
            return

        # Nettoyage des données
        df["VALEUR_FONCIERE"] = pd.to_numeric(df["VALEUR_FONCIERE"], errors="coerce")
        df["SURFACE_REELLE_BATI"] = pd.to_numeric(df["SURFACE_REELLE_BATI"], errors="coerce")
        df["DATE_MUTATION"] = pd.to_datetime(df["DATE_MUTATION"])

        # Affichage des métriques
        st.header("📊 Statistiques générales")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Nombre de transactions", f"{len(df):,}")

        with col2:
            prix_moyen = df["VALEUR_FONCIERE"].mean()
            st.metric("Prix moyen", f"{prix_moyen:,.0f} €")

        with col3:
            prix_median = df["VALEUR_FONCIERE"].median()
            st.metric("Prix médian", f"{prix_median:,.0f} €")

        with col4:
            surface_moyenne = df["SURFACE_REELLE_BATI"].mean()
            st.metric("Surface moyenne", f"{surface_moyenne:.0f} m²")

        st.markdown("---")

        # Graphiques
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 Évolution des prix dans le temps")
            df_time = df.groupby(df["DATE_MUTATION"].dt.to_period("M")).agg({
                "VALEUR_FONCIERE": "mean"
            }).reset_index()
            df_time["DATE_MUTATION"] = df_time["DATE_MUTATION"].astype(str)

            fig_time = px.line(
                df_time,
                x="DATE_MUTATION",
                y="VALEUR_FONCIERE",
                labels={"DATE_MUTATION": "Date", "VALEUR_FONCIERE": "Prix moyen (€)"}
            )
            st.plotly_chart(fig_time, use_container_width=True)

        with col2:
            st.subheader("📊 Distribution des prix")
            fig_dist = px.histogram(
                df,
                x="VALEUR_FONCIERE",
                nbins=30,
                labels={"VALEUR_FONCIERE": "Prix (€)", "count": "Nombre"}
            )
            st.plotly_chart(fig_dist, use_container_width=True)

        # Prix par type de local
        if "TYPE_LOCAL" in df.columns:
            st.subheader("🏘️ Prix moyen par type de bien")
            df_type = df.groupby("TYPE_LOCAL").agg({
                "VALEUR_FONCIERE": "mean",
                "TYPE_LOCAL": "count"
            }).reset_index(names="TYPE_LOCAL")
            df_type.columns = ["TYPE_LOCAL", "PRIX_MOYEN", "NOMBRE"]

            fig_type = px.bar(
                df_type,
                x="TYPE_LOCAL",
                y="PRIX_MOYEN",
                text="NOMBRE",
                labels={"TYPE_LOCAL": "Type de bien", "PRIX_MOYEN": "Prix moyen (€)", "NOMBRE": "Nombre"}
            )
            st.plotly_chart(fig_type, use_container_width=True)

        st.markdown("---")

        # Tableau des transactions
        st.subheader("📋 Détail des transactions")

        # Colonnes à afficher
        display_columns = [
            "DATE_MUTATION", "COMMUNE", "VOIE", "NO_VOIE",
            "TYPE_LOCAL", "VALEUR_FONCIERE", "SURFACE_REELLE_BATI",
            "NOMBRE_PIECES_PRINCIPALES"
        ]

        df_display = df[display_columns].copy()
        df_display["VALEUR_FONCIERE"] = df_display["VALEUR_FONCIERE"].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "N/A")
        df_display["SURFACE_REELLE_BATI"] = df_display["SURFACE_REELLE_BATI"].apply(lambda x: f"{x:.0f} m²" if pd.notna(x) else "N/A")

        st.dataframe(df_display, use_container_width=True, height=400)

        # Export CSV
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Télécharger les données (CSV)",
            data=csv,
            file_name=f"dvf_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
