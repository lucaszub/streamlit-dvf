import streamlit as st
import snowflake.connector
import pandas as pd
import json
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Assistant SQL DVF",
    page_icon="💬",
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

# Fonction pour appeler l'agent Snowflake
def call_agent(conn, message, conversation_history=None):
    """
    Appelle l'agent Snowflake ASSISTANTSQLDVF avec un message
    Retourne un dict avec 'response', 'sql_query', et 'metadata'
    """
    try:
        cursor = conn.cursor()

        # Méthode 1: Essayer d'appeler l'agent directement via CALL
        try:
            # Construire l'historique de conversation si disponible
            history_str = ""
            if conversation_history:
                messages = []
                for msg in conversation_history[-5:]:  # Garder les 5 derniers messages pour le contexte
                    role = msg["role"]
                    content = msg["content"].replace("'", "''")
                    messages.append(f"{{'role': '{role}', 'content': '{content}'}}")
                history_str = ", ".join(messages)

            # Ajouter le message actuel
            escaped_message = message.replace("'", "''")
            current_msg = f"{{'role': 'user', 'content': '{escaped_message}'}}"
            if history_str:
                full_messages = f"[{history_str}, {current_msg}]"
            else:
                full_messages = f"[{current_msg}]"

            # Appel à l'agent
            agent_call = f"CALL ASSISTANTSQLDVF!CHAT({full_messages})"

            cursor.execute(agent_call)
            result = cursor.fetchall()

            if result:
                # Parser la réponse de l'agent
                # Le format de réponse dépend de la configuration de l'agent
                response_text = str(result[0][0]) if result[0] else "Pas de réponse."

                return {
                    "response": response_text,
                    "sql_query": None,
                    "metadata": None
                }

        except Exception as e1:
            # Méthode 2: Si la méthode 1 échoue, essayer avec une approche alternative
            # Utiliser SNOWFLAKE.CORTEX.COMPLETE comme fallback
            st.warning(f"Méthode d'appel direct échouée: {e1}")

            # Essayer avec un prompt structuré pour générer du SQL
            prompt = f"""En tant qu'expert SQL, analysez cette question sur les données DVF (Demandes de Valeurs Foncières) et générez une requête SQL appropriée.

Question: {message}

Base de données: VALFONC_ANALYTICS.GOLD
Tables disponibles:
- FACT_MUTATION: contient les transactions immobilières (DATE_MUTATION, VALEUR_FONCIERE, SURFACE_REELLE_BATI, SURFACE_TERRAIN, NOMBRE_PIECES_PRINCIPALES)
- DIM_COMMUNE: communes (COMMUNE, CODE_DEPARTEMENT, COMMUNE_ID)
- DIM_ADDRESS: adresses (VOIE, TYPE_DE_VOIE, NO_VOIE, CODE_POSTAL, ADDRESS_ID)
- DIM_TYPE_LOCAL: types de locaux (TYPE_LOCAL, TYPE_LOCAL_ID)

Générez une requête SQL pour répondre à cette question. Répondez au format:
RÉPONSE: [explication en français]
SQL: [requête SQL]
"""

            escaped_prompt = prompt.replace("'", "''")
            llm_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large',
                '{escaped_prompt}'
            ) as response
            """

            cursor.execute(llm_query)
            llm_result = cursor.fetchone()

            if llm_result and llm_result[0]:
                response_text = str(llm_result[0])

                # Essayer d'extraire la requête SQL de la réponse
                sql_query = None
                if "SQL:" in response_text:
                    parts = response_text.split("SQL:")
                    if len(parts) > 1:
                        sql_query = parts[1].strip()
                        # Nettoyer la requête SQL
                        if "```sql" in sql_query:
                            sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
                        elif "```" in sql_query:
                            sql_query = sql_query.split("```")[1].split("```")[0].strip()

                return {
                    "response": response_text,
                    "sql_query": sql_query,
                    "metadata": {"method": "cortex_complete"}
                }
            else:
                raise Exception("Aucune réponse obtenue de Cortex")

    except Exception as e:
        st.error(f"Erreur lors de l'appel à l'agent: {e}")
        return {
            "response": f"Désolé, je n'ai pas pu traiter votre demande. Erreur: {str(e)}",
            "sql_query": None,
            "metadata": {"error": str(e)}
        }
    finally:
        if cursor:
            cursor.close()

# Fonction pour exécuter une requête SQL (si l'agent retourne une requête)
def execute_sql_query(conn, query):
    """
    Exécute une requête SQL et retourne un DataFrame
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        cursor.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors de l'exécution de la requête: {e}")
        return None

# Interface principale
def main():
    st.title("💬 Assistant SQL DVF")
    st.markdown("Posez vos questions sur les données immobilières DVF en langage naturel")
    st.markdown("---")

    # Connexion à Snowflake
    conn = get_snowflake_connection()

    if conn is None:
        st.warning("⚠️ Impossible de se connecter à Snowflake. Veuillez vérifier votre configuration dans .streamlit/secrets.toml")
        return

    # Initialiser l'historique de chat dans session_state
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Afficher l'historique des messages
    for i, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

            # Afficher les détails SQL si disponibles
            if "sql_query" in msg and msg["sql_query"]:
                with st.expander("🔍 Voir la requête SQL"):
                    st.code(msg["sql_query"], language="sql")

            if "sql_results" in msg and msg["sql_results"] is not None:
                with st.expander(f"📊 Voir les résultats ({len(msg['sql_results'])} lignes)", expanded=False):
                    st.dataframe(msg["sql_results"], use_container_width=True)

    # Zone de saisie du message
    if prompt := st.chat_input("Posez votre question sur les données DVF..."):
        # Ajouter le message de l'utilisateur
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Afficher le message de l'utilisateur
        with st.chat_message("user"):
            st.markdown(prompt)

        # Obtenir la réponse de l'agent
        with st.chat_message("assistant"):
            with st.spinner("L'assistant réfléchit..."):
                # Appeler l'agent avec l'historique de conversation
                agent_response = call_agent(conn, prompt, st.session_state.messages)

                response_text = agent_response["response"]
                sql_query = agent_response["sql_query"]

                # Afficher la réponse
                st.markdown(response_text)

                # Préparer le message à stocker
                assistant_message = {
                    "role": "assistant",
                    "content": response_text,
                    "sql_query": sql_query,
                    "sql_results": None
                }

                # Si une requête SQL a été générée, l'afficher et l'exécuter
                if sql_query:
                    with st.expander("🔍 Voir la requête SQL"):
                        st.code(sql_query, language="sql")

                    try:
                        df_results = execute_sql_query(conn, sql_query)
                        if df_results is not None and not df_results.empty:
                            with st.expander(f"📊 Voir les résultats ({len(df_results)} lignes)", expanded=True):
                                st.dataframe(df_results, use_container_width=True)

                                # Ajouter des statistiques sur les résultats
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Nombre de lignes", len(df_results))
                                with col2:
                                    st.metric("Nombre de colonnes", len(df_results.columns))
                                with col3:
                                    # Export CSV
                                    csv = df_results.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        label="📥 CSV",
                                        data=csv,
                                        file_name=f"resultats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                        mime="text/csv"
                                    )

                            assistant_message["sql_results"] = df_results
                        elif df_results is not None:
                            st.info("La requête n'a retourné aucun résultat.")
                    except Exception as e:
                        st.warning(f"La requête n'a pas pu être exécutée: {e}")

                st.session_state.messages.append(assistant_message)

    # Barre latérale avec informations et actions
    with st.sidebar:
        st.header("💡 Aide")

        st.markdown("""
        ### Questions suggérées:

        - "Quel est le prix médian des maisons à Rennes?"
        - "Combien de transactions ont eu lieu en 2023?"
        - "Quelles sont les communes avec le plus de ventes?"
        - "Montre-moi l'évolution des prix des appartements"
        - "Quels sont les 10 biens les plus chers vendus?"

        ### À propos de l'assistant:

        L'assistant SQL DVF utilise l'agent Snowflake Cortex pour:
        - Comprendre vos questions en langage naturel
        - Générer automatiquement des requêtes SQL
        - Exécuter ces requêtes sur la base de données DVF
        - Vous présenter les résultats de manière claire
        """)

        st.markdown("---")

        if st.button("🗑️ Effacer l'historique", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

        st.markdown("---")

        st.markdown(f"""
        **Statistiques de session:**
        - Messages: {len(st.session_state.messages)}
        - Dernière activité: {datetime.now().strftime('%H:%M:%S')}
        """)

if __name__ == "__main__":
    main()
