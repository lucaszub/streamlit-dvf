import streamlit as st
import snowflake.connector
import pandas as pd

# Configuration de la page
st.set_page_config(
    page_title="Prédiction Prix Immobilier",
    page_icon="🔮",
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

# Fonction pour obtenir les codes postaux disponibles
@st.cache_data(ttl=3600)
def get_postal_codes(_conn):
    """Récupère la liste des codes postaux disponibles"""
    query = """
    SELECT DISTINCT a.CODE_POSTAL
    FROM VALFONC_ANALYTICS.GOLD.DIM_ADDRESS a
    INNER JOIN VALFONC_ANALYTICS.GOLD.FACT_MUTATION f ON a.ADDRESS_ID = f.ADDRESS_ID
    WHERE a.CODE_POSTAL IS NOT NULL
    ORDER BY a.CODE_POSTAL
    """
    return run_query(_conn, query)

# Interface principale
def main():
    st.title("🔮 Prédiction Prix Immobilier")
    st.markdown("Estimez le prix d'un bien immobilier en fonction de ses caractéristiques")
    st.markdown("---")

    # Connexion à Snowflake
    conn = get_snowflake_connection()

    if conn is None:
        st.warning("⚠️ Impossible de se connecter à Snowflake. Veuillez vérifier votre configuration dans .streamlit/secrets.toml")
        return

    # Charger les codes postaux disponibles
    postal_codes_df = get_postal_codes(conn)

    if postal_codes_df.empty:
        st.error("Aucune donnée disponible pour les codes postaux")
        return

    postal_codes_list = sorted(postal_codes_df["CODE_POSTAL"].tolist())

    # Interface de saisie
    st.header("📝 Caractéristiques du bien")

    col1, col2 = st.columns(2)

    with col1:
        surface = st.number_input("Surface habitable (m²)", min_value=10, max_value=500, value=70, step=5)
        pieces = st.selectbox("Nombre de pièces principales", options=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=2)

    with col2:
        postal = st.selectbox("Code postal", options=postal_codes_list, index=0)
        type_bien = st.selectbox("Type de bien", options=["MAISON", "APPARTEMENT"], index=0)

    st.markdown("---")

    # Bouton de prédiction
    if st.button("🔮 Estimer le prix", type="primary"):
        with st.spinner("Calcul de l'estimation..."):
            try:
                # Utilisation de la fonction PREDICT_PROPERTY_PRICE de Snowflake
                prediction_query = f"""
                SELECT PREDICT_PROPERTY_PRICE({surface}, {pieces}, '{postal}', '{type_bien}') as PRIX_ESTIME
                """

                result_df = run_query(conn, prediction_query)

                if not result_df.empty and result_df["PRIX_ESTIME"].iloc[0] is not None:
                    prix_estime = float(result_df["PRIX_ESTIME"].iloc[0])

                    # Affichage du résultat principal
                    st.success(f"### 💰 Prix estimé : {prix_estime:,.0f} €")

                    # Afficher des biens similaires avec la fonction FIND_SIMILAR_PROPERTIES
                    st.markdown("---")
                    st.subheader("🏘️ Biens similaires récents")

                    similar_query = f"""
                    SELECT *
                    FROM TABLE(FIND_SIMILAR_PROPERTIES({surface}, {pieces}, '{postal}', '{type_bien}'))
                    """

                    similar_df = run_query(conn, similar_query)

                    if not similar_df.empty:
                        # Formater les colonnes pour l'affichage
                        if "DATE_MUTATION" in similar_df.columns:
                            similar_df["DATE_MUTATION"] = pd.to_datetime(similar_df["DATE_MUTATION"]).dt.strftime('%Y-%m-%d')
                        if "PRIX" in similar_df.columns:
                            similar_df["PRIX"] = similar_df["PRIX"].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "N/A")
                        if "VALEUR_FONCIERE" in similar_df.columns:
                            similar_df["VALEUR_FONCIERE"] = similar_df["VALEUR_FONCIERE"].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "N/A")
                        if "SURFACE_M2" in similar_df.columns:
                            similar_df["SURFACE_M2"] = similar_df["SURFACE_M2"].apply(lambda x: f"{x:.0f} m²" if pd.notna(x) else "N/A")
                        if "SURFACE_REELLE_BATI" in similar_df.columns:
                            similar_df["SURFACE_REELLE_BATI"] = similar_df["SURFACE_REELLE_BATI"].apply(lambda x: f"{x:.0f} m²" if pd.notna(x) else "N/A")

                        st.dataframe(similar_df, use_container_width=True, hide_index=True)

                        st.info(f"📈 Cette estimation est basée sur **{len(similar_df)}** transactions similaires")
                    else:
                        st.info("Aucun bien similaire récent trouvé")

                else:
                    st.warning("⚠️ Pas assez de données comparables pour cette estimation")
                    st.info("""
                    **Suggestions :**
                    - Essayez avec un code postal différent
                    - Modifiez le nombre de pièces
                    - Ajustez la surface du bien
                    """)

                    # Proposer des alternatives
                    st.markdown("---")
                    st.subheader("📍 Codes postaux avec le plus de données")

                    alternative_query = f"""
                    SELECT
                        a.CODE_POSTAL,
                        COUNT(*) as NB_TRANSACTIONS,
                        AVG(f.VALEUR_FONCIERE) as PRIX_MOYEN
                    FROM VALFONC_ANALYTICS.GOLD.FACT_MUTATION f
                    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_ADDRESS a ON f.ADDRESS_ID = a.ADDRESS_ID
                    LEFT JOIN VALFONC_ANALYTICS.GOLD.DIM_TYPE_LOCAL t ON f.TYPE_LOCAL_ID = t.TYPE_LOCAL_ID
                    WHERE t.TYPE_LOCAL = '{type_bien}'
                        AND f.NOMBRE_PIECES_PRINCIPALES = {pieces}
                        AND f.VALEUR_FONCIERE > 0
                    GROUP BY a.CODE_POSTAL
                    ORDER BY NB_TRANSACTIONS DESC
                    LIMIT 10
                    """

                    alternative_df = run_query(conn, alternative_query)

                    if not alternative_df.empty:
                        alternative_df["PRIX_MOYEN"] = alternative_df["PRIX_MOYEN"].apply(lambda x: f"{x:,.0f} €")
                        alternative_df.columns = ["Code Postal", "Nombre de transactions", "Prix moyen"]
                        st.dataframe(alternative_df, use_container_width=True, hide_index=True)

            except Exception as e:
                st.error(f"❌ Erreur lors de la prédiction : {e}")
                st.info("Vérifiez que les fonctions PREDICT_PROPERTY_PRICE et FIND_SIMILAR_PROPERTIES existent dans votre base Snowflake")

if __name__ == "__main__":
    main()
