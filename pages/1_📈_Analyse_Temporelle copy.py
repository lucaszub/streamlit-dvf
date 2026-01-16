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


st.title("📈 Analyse Temporelle des Valeurs Foncières")



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
def run_query(_conn, query, params=None):
    """Exécute une requête paramétrée et retourne un DataFrame

    params: tuple or list of parameters to bind to the query. If None, the query
    is executed as-is.
    """
    try:
        cursor = _conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        df = cursor.fetch_pandas_all()
        cursor.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors de l'exécution de la requête: {e}")
        return pd.DataFrame()
    
def get_disctinct_code_postal(conn, departement=None):
    # Retourne les codes postaux, optionnellement filtrés par département si
    # la valeur est fournie via la clause WHERE (paramétrée)
    base = """
        SELECT DISTINCT code_postal
        FROM dim_code_postal
    """
    if departement:
        query = base + "\n        WHERE code_departement = %s\n        ORDER BY code_postal"
        return run_query(conn, query, (departement,))
    else:
        query = base + "\n        ORDER BY code_postal"
        return run_query(conn, query)


def get_commune(conn, departement=None):
    """Retourne les communes distinctes, optionnellement filtrées par département"""
    base = """
        SELECT DISTINCT dcm.commune AS commune
        FROM dim_commune AS dcm
    """
    if departement:
        # Joindre avec dim_code_postal pour filtrer par département
        base += """
        INNER JOIN fact_mutation AS fm ON dcm.commune_id = fm.commune_id
        INNER JOIN dim_code_postal AS dcp ON fm.code_postal_id = dcp.code_postal_id
        WHERE dcp.code_departement = %s
        """
        query = base + "\n        ORDER BY dcm.commune"
        return run_query(conn, query, (departement,))
    else:
        query = base + "\n        ORDER BY dcm.commune"
        return run_query(conn, query)
def get_disctinct_departement(conn):
    query = """
        SELECT DISTINCT code_departement AS departement
        FROM dim_code_postal
        ORDER BY departement
    """
    return run_query(conn, query)


    
def get_disctinct_type_local(conn):
    query = """
        SELECT DISTINCT type_local
        FROM dim_type_local
        ORDER BY type_local
    """
    return run_query(conn, query)

def get_annee_options(conn):
    query = """
        SELECT DISTINCT EXTRACT(YEAR FROM date_mutation) AS annee
        FROM fact_mutation
        ORDER BY annee DESC
    """
    return run_query(conn, query)


def get_disctinct_voie(conn, departement=None, code_postal=None):
    """Retourne les voies (rue/voie) distinctes, optionnellement filtrées
    par département et/ou code_postal.
    """
    base = """
        SELECT DISTINCT voie
        FROM dim_code_postal
    """
    clauses = []
    params = []
    if departement:
        clauses.append("code_departement = %s")
        params.append(departement)
    if code_postal:
        clauses.append("code_postal = %s")
        params.append(code_postal)

    if clauses:
        base += "\n        WHERE " + " AND ".join(clauses)

    base += "\n        ORDER BY voie"
    return run_query(conn, base, tuple(params) if params else None)
    

def get_df(conn, code_postal=None, type_local=None, nombre_pieces_principales=None, annee=None, departement=None, voie=None, commune=None):
    """Construit dynamiquement une requête en fonction des filtres fournis.

    Tous les arguments sont optionnels. Si aucun filtre n'est fourni, la
    requête retourne (limité) l'ensemble des enregistrements.
    """

    base_query = """
        SELECT
            fm.date_mutation,
            fm.valeur_fonciere,
            fm.surface_reelle_bati,
            fm.nombre_pieces_principales,
            dcp.voie,
            
            dcp.code_postal,
            dtl.type_local
        FROM fact_mutation AS fm
        INNER JOIN dim_code_postal AS dcp
            ON fm.code_postal_id = dcp.code_postal_id
        INNER JOIN dim_type_local as dtl
            ON fm.type_local_id = dtl.type_local_id
        INNER JOIN dim_commune AS dcm
            ON fm.commune_id = dcm.commune_id
    """

    where_clauses = ["fm.valeur_fonciere IS NOT NULL"]
    params = []

    # Ajouter les filtres uniquement s'ils sont renseignés
    if code_postal:
        where_clauses.append("dcp.code_postal = %s")
        params.append(code_postal)
    if type_local:
        where_clauses.append("dtl.type_local = %s")
        params.append(type_local)
    if nombre_pieces_principales:
        where_clauses.append("fm.nombre_pieces_principales = %s")
        params.append(int(nombre_pieces_principales))
    if annee:
        where_clauses.append("EXTRACT(YEAR FROM fm.date_mutation) = %s")
        params.append(int(annee))
    if departement:
        where_clauses.append("dcp.code_departement = %s")
        params.append(departement)
    if voie:
        where_clauses.append("dcp.voie = %s")
        params.append(voie)
    if commune:
        where_clauses.append("dcm.commune = %s")
        params.append(commune)

    if where_clauses:
        base_query += "\n        WHERE " + " AND ".join(where_clauses)

    base_query += "\n        ORDER BY fm.valeur_fonciere DESC\n        LIMIT 1000\n    "

    # run_query acceptera None ou tuple(params)
    return run_query(conn, base_query, tuple(params) if params else None)



conn = get_snowflake_connection()

# === FILTRE 1: DÉPARTEMENT ===
departement_df = get_disctinct_departement(conn) if conn is not None else pd.DataFrame()
departement_options = ["Tout"] + (departement_df.iloc[:, 0].tolist() if not departement_df.empty else [])
departement = st.sidebar.selectbox("Département", departement_options, index=0)
departement_filter = None if departement == "Tout" else departement

# === FILTRE 2: CODE POSTAL (filtré par département) ===
if departement_filter:
    postal_df = get_disctinct_code_postal(conn, departement_filter) if conn is not None else pd.DataFrame()
else:
    postal_df = get_disctinct_code_postal(conn) if conn is not None else pd.DataFrame()
postal_options = ["Tout"] + (postal_df.iloc[:, 0].tolist() if not postal_df.empty else [])
code_postal = st.sidebar.selectbox("Code postal", postal_options, index=0)
code_postal_filter = None if code_postal == "Tout" else code_postal

# === FILTRE 3: COMMUNE (filtrée par département) ===
if departement_filter:
    commune_df = get_commune(conn, departement_filter) if conn is not None else pd.DataFrame()
else:
    commune_df = get_commune(conn) if conn is not None else pd.DataFrame()
commune_options = ["Tout"] + (commune_df.iloc[:, 0].tolist() if not commune_df.empty else [])
commune = st.sidebar.selectbox("Commune", commune_options, index=0)
commune_filter = None if commune == "Tout" else commune

# === FILTRE 4: TYPE DE LOCAL ===
type_df = get_disctinct_type_local(conn) if conn is not None else pd.DataFrame()
type_options = ["Tout"] + (type_df.iloc[:, 0].tolist() if not type_df.empty else [])
type_local = st.sidebar.selectbox("Type de local", type_options, index=0)
type_local_filter = None if type_local == "Tout" else type_local

# === FILTRE 5: VOIE (filtrée par département et code postal) ===
voie_df = get_disctinct_voie(conn, departement=departement_filter, code_postal=code_postal_filter) if conn is not None else pd.DataFrame()
voie_options = ["Tout"] + (voie_df.iloc[:, 0].tolist() if not voie_df.empty else [])
voie = st.sidebar.selectbox("Voie", voie_options, index=0)
voie_filter = None if voie == "Tout" else voie

# === FILTRE 6: NOMBRE DE PIÈCES ===
nombre_pieces_principales = st.sidebar.slider("Nombre de pièces principales", min_value=1, max_value=10, value=1)



# === FILTRE 7: ANNÉE ===
annee_df = get_annee_options(conn) if conn is not None else pd.DataFrame({"ANNEE": [2023]})
annee_options = ["Tout"] + (annee_df["ANNEE"].tolist() if not annee_df.empty else [])
annee = st.sidebar.selectbox("Année", options=annee_options, index=0)
annee_filter = None if annee == "Tout" else annee

st.sidebar.header("Filtres sélectionnés")

# Appel de get_df avec les filtres (None si "Tout" est sélectionné)
df = get_df(conn, code_postal_filter, type_local_filter, nombre_pieces_principales, annee_filter, departement_filter, voie_filter, commune_filter)

mean_price = df['VALEUR_FONCIERE'].mean() if not df.empty else 0


col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Prix moyen des biens", f"{mean_price:,.2f} €")
with col2:
    st.metric("Nombre de transactions", f"{len(df)}")
with col3:
    st.metric("Nombre de pièces principales", f"{nombre_pieces_principales}")
    
st.dataframe(df)


