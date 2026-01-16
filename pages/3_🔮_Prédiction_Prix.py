import streamlit as st
import snowflake.connector
import pandas as pd

# Configuration de la page
st.set_page_config(
    page_title="Prédiction Prix Immobilier",
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

# Fonction pour obtenir les statistiques par zones
@st.cache_data(ttl=3600)
def get_zones_stats(_conn):
    """Récupère les statistiques par zones basées sur les prix moyens"""
    query = """
    SELECT 
        CASE 
            WHEN AVG_PRICE_BY_POSTAL < 200000 THEN 'Zone Économique'
            WHEN AVG_PRICE_BY_POSTAL < 300000 THEN 'Zone Modérée'
            WHEN AVG_PRICE_BY_POSTAL < 400000 THEN 'Zone Premium'
            ELSE 'Zone Luxe'
        END as ZONE_TYPE,
        AVG(AVG_PRICE_BY_POSTAL) as AVG_PRICE_BY_POSTAL,
        AVG(AVG_PRICE_PER_SQM_BY_POSTAL) as AVG_PRICE_PER_SQM_BY_POSTAL,
        AVG(DISTANCE_TO_CENTER_KM) as AVG_DISTANCE,
        COUNT(*) as NB_TRANSACTIONS
    FROM PREDICTION_PRIX 
    WHERE VALEUR_FONCIERE > 0 AND SURFACE_REELLE_BATI > 0
    GROUP BY ZONE_TYPE
    HAVING COUNT(*) >= 50
    ORDER BY AVG_PRICE_BY_POSTAL
    """
    return run_query(_conn, query)

# Interface principale
def main():
    st.title("🏠 Prédiction Prix Immobilier")
    st.markdown("Estimez le prix d'un appartement en fonction de ses caractéristiques")
    st.markdown("---")

    # Connexion à Snowflake
    conn = get_snowflake_connection()
    if conn is None:
        st.warning("⚠️ Impossible de se connecter à Snowflake. Veuillez vérifier votre configuration.")
        return

    # Charger les statistiques des zones
    zones_stats = get_zones_stats(conn)
    if zones_stats.empty:
        st.error("Aucune donnée disponible")
        return

    # Interface de saisie
    st.header("📝 Caractéristiques de l'appartement")
    
    col1, col2 = st.columns(2)
    
    with col1:
        surface = st.number_input("Surface habitable (m²)", min_value=20, max_value=200, value=70, step=5)
        pieces = st.selectbox("Nombre de pièces principales", options=[1, 2, 3, 4, 5, 6], index=2)
    
    with col2:
        # Sélection de la zone basée sur les données disponibles
        zones_disponibles = zones_stats["ZONE_TYPE"].tolist()
        zone = st.selectbox("Type de zone", options=zones_disponibles, 
                           help="Zones basées sur les gammes de prix")
        
        distance_center = st.slider("Distance du centre-ville (km)", min_value=0.0, max_value=20.0, value=5.0, step=0.5)

    # Afficher les infos de la zone sélectionnée
    zone_info = zones_stats[zones_stats["ZONE_TYPE"] == zone].iloc[0]
    
    with st.expander("📊 Informations de la zone sélectionnée"):
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.metric("Prix moyen zone", f"{zone_info['AVG_PRICE_BY_POSTAL']:,.0f} €")
        with col_info2:
            st.metric("Prix/m² moyen", f"{zone_info['AVG_PRICE_PER_SQM_BY_POSTAL']:,.0f} €/m²")
        with col_info3:
            st.metric("Transactions", f"{zone_info['NB_TRANSACTIONS']:,.0f}")

    st.markdown("---")

    # Bouton de prédiction
    if st.button("🔮 Estimer le prix", type="primary"):
        with st.spinner("Calcul de l'estimation..."):
            try:
                # Récupérer les paramètres de la zone
                avg_price_zone = float(zone_info['AVG_PRICE_BY_POSTAL'])
                avg_price_sqm = float(zone_info['AVG_PRICE_PER_SQM_BY_POSTAL'])
                
                # Calcul d'estimation simple (en attendant la fonction UDF)
                # Formule basique : surface × prix/m² zone × facteurs correcteurs
                distance_factor = max(0.7, 1 - (distance_center * 0.05))  # -5% par km
                size_factor = 1.0 if surface <= 70 else 0.98  # Légère décote pour grandes surfaces
                room_factor = 1.0 + (pieces - 3) * 0.02  # +/-2% par pièce vs 3 pièces
                
                prix_estime = surface * avg_price_sqm * distance_factor * size_factor * room_factor
                prix_par_m2 = prix_estime / surface
                
                # Affichage du résultat principal
                col_result1, col_result2, col_result3 = st.columns(3)
                with col_result1:
                    st.metric("💰 Prix estimé", f"{prix_estime:,.0f} €")
                with col_result2:
                    st.metric("💰 Prix/m²", f"{prix_par_m2:,.0f} €/m²")
                with col_result3:
                    ecart_zone = ((prix_par_m2 - avg_price_sqm) / avg_price_sqm) * 100
                    st.metric("📈 Vs moyenne zone", f"{ecart_zone:+.1f}%")

                # Recherche d'appartements similaires
                st.markdown("---")
                st.subheader("🏘️ Appartements similaires vendus")
                
                # Tolérance pour la recherche
                surface_min = surface * 0.8
                surface_max = surface * 1.2
                prix_min = prix_estime * 0.85
                prix_max = prix_estime * 1.15
                
                similar_query = f"""
                SELECT 
                    SURFACE_REELLE_BATI as "Surface (m²)",
                    NOMBRE_PIECES_PRINCIPALES as "Pièces",
                    VALEUR_FONCIERE as "Prix vendu (€)",
                    ROUND(VALEUR_FONCIERE / SURFACE_REELLE_BATI, 0) as "Prix/m² (€)",
                    ABS(VALEUR_FONCIERE - {prix_estime}) as "Écart prix (€)",
                    DISTANCE_TO_CENTER_KM as "Distance centre (km)"
                FROM PREDICTION_PRIX
                WHERE SURFACE_REELLE_BATI BETWEEN {surface_min} AND {surface_max}
                  AND NOMBRE_PIECES_PRINCIPALES = {pieces}
                  AND VALEUR_FONCIERE BETWEEN {prix_min} AND {prix_max}
                  AND VALEUR_FONCIERE IS NOT NULL
                ORDER BY ABS(VALEUR_FONCIERE - {prix_estime})
                LIMIT 10
                """
                
                similar_df = run_query(conn, similar_query)
                
                if not similar_df.empty:
                    # Formater l'affichage
                    similar_df_display = similar_df.copy()
                    for col in ["Prix vendu (€)", "Prix/m² (€)", "Écart prix (€)"]:
                        if col in similar_df_display.columns:
                            similar_df_display[col] = similar_df_display[col].apply(lambda x: f"{x:,.0f}")
                    
                    st.dataframe(similar_df_display, use_container_width=True, hide_index=True)
                    
                    # Statistiques des appartements similaires
                    prix_moyen_similaires = similar_df["Prix vendu (€)"].mean()
                    ecart_prediction = abs(prix_moyen_similaires - prix_estime)
                    precision = (1 - ecart_prediction / prix_moyen_similaires) * 100
                    
                    st.success(f"📈 Analyse basée sur **{len(similar_df)}** transactions similaires")
                    st.info(f"🎯 Précision de l'estimation: **{precision:.1f}%** (écart moyen: {ecart_prediction:,.0f} €)")
                    
                else:
                    st.warning("⚠️ Aucun appartement similaire trouvé avec ces critères")
                    
                    # Suggestions alternatives
                    st.markdown("### 💡 Suggestions")
                    suggestion_query = f"""
                    SELECT 
                        NOMBRE_PIECES_PRINCIPALES as "Pièces",
                        COUNT(*) as "Nb transactions",
                        AVG(VALEUR_FONCIERE) as "Prix moyen",
                        AVG(SURFACE_REELLE_BATI) as "Surface moyenne"
                    FROM PREDICTION_PRIX
                    WHERE SURFACE_REELLE_BATI BETWEEN {surface-20} AND {surface+20}
                      AND VALEUR_FONCIERE > 0
                    GROUP BY NOMBRE_PIECES_PRINCIPALES
                    ORDER BY COUNT(*) DESC
                    LIMIT 5
                    """
                    
                    suggestion_df = run_query(conn, suggestion_query)
                    if not suggestion_df.empty:
                        suggestion_df["Prix moyen"] = suggestion_df["Prix moyen"].apply(lambda x: f"{x:,.0f} €")
                        suggestion_df["Surface moyenne"] = suggestion_df["Surface moyenne"].apply(lambda x: f"{x:.0f} m²")
                        st.dataframe(suggestion_df, use_container_width=True, hide_index=True)
                        
            except Exception as e:
                st.error(f"❌ Erreur lors de la prédiction : {e}")

    # Footer
    st.markdown("---")
    st.markdown("*Application basée sur les données DVF (Demandes de Valeurs Foncières)*")

if __name__ == "__main__":
    main()