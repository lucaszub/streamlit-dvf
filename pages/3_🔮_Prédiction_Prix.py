import streamlit as st
from snowflake.snowpark import Session

# Configuration de la page
st.set_page_config(
    page_title="Prédiction Prix Immobilier",
    page_icon="🔮",
    layout="wide"
)

st.title("🏠 Prédiction Prix Immobilier - Rennes")

# Connexion Snowflake
@st.cache_resource
def init_snowflake():
    return Session.builder.configs({
        "account": st.secrets["snowflake"]["account"],
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "warehouse": st.secrets["snowflake"]["warehouse"],
        "database": st.secrets["snowflake"]["database"],
        "schema": st.secrets["snowflake"]["schema"]
    }).create()

session = init_snowflake()

# Interface
col1, col2 = st.columns(2)

with col1:
    surface = st.number_input("Surface (m²)", 10, 300, 70)
    pieces = st.selectbox("Pièces", [1,2,3,4,5,6,7,8], index=2)

with col2:
    distance = st.slider("Distance centre (km)", 0, 15, 5)
    postal = st.selectbox("Code postal", ["35000", "35200", "35700"])

if st.button("🔮 Prédire le prix"):
    # Prédiction
    result = session.sql(f"""
        SELECT PREDICT_PROPERTY_PRICE({surface}, {pieces}, {distance}, '{postal}') as prix
    """).collect()[0]['PRIX']

    st.success(f"**Prix estimé : {result:,.0f} €**")

    # Biens similaires
    st.subheader("🏘️ Biens similaires")
    similar = session.sql(f"""
        SELECT * FROM TABLE(FIND_SIMILAR_PROPERTIES({surface}, {pieces}, '{postal}'))
    """).to_pandas()

    st.dataframe(similar)
