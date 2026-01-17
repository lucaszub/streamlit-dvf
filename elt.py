import snowflake.connector
import pandas as pd
import requests
import gzip
import io
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas

# Configuration Snowflake
SNOWFLAKE_CONFIG = {
    'user': 'LUCASZUB',
    'password': 'Bonjour04!Medard44?',
    'account': 'TLMANNA-BC08454',
    'warehouse': 'WH_DBT_VALFONC',
    'database': 'VALFONC_RAW',
    'schema': 'BRONZE'
}

# Liste de TOUS les départements français
DEPARTEMENTS = [
    '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
    '11', '12', '13', '14', '15', '16', '17', '18', '19', '21',
    '22', '23', '24', '25', '26', '27', '28', '29', '2A', '2B',
    '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
    '40', '41', '42', '43', '44', '45', '46', '47', '48', '49',
    '50', '51', '52', '53', '54', '55', '56', '57', '58', '59',
    '60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
    '70', '71', '72', '73', '74', '75', '76', '77', '78', '79',
    '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
    '90', '91', '92', '93', '94', '95',
    '971', '972', '973', '974', '976'  # DOM-TOM
]

def get_snowflake_connection():
    """Crée et retourne une connexion Snowflake"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("✅ Connexion Snowflake établie")
        return conn
    except Exception as e:
        print(f"❌ Erreur de connexion à Snowflake: {e}")
        return None

def telecharger_et_charger_departement(dept, conn):
    """Télécharge et charge les données d'un département dans Snowflake"""
    url = f"https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-{dept}.csv.gz"
    
    try:
        # Télécharger le fichier
        print(f"📥 Téléchargement département {dept}...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Dézipper à la volée
        print(f"📦 Décompression département {dept}...")
        decompressed = gzip.decompress(response.content)
        
        # Lire avec pandas
        print(f"📊 Lecture CSV département {dept}...")
        df = pd.read_csv(
            io.BytesIO(decompressed),
            sep=';',
            dtype=str,  # Tout en string pour éviter les problèmes de types
            low_memory=False
        )
        
        # Ajouter une colonne département pour traçabilité
        df['departement'] = dept
        
        # Charger dans Snowflake
        print(f"⬆️  Chargement dans Snowflake département {dept} ({len(df):,} lignes)...")
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='BAN_ADRESSES',
            database='VALFONC_RAW',
            schema='PUBLIC',
            auto_create_table=True,
            overwrite=False
        )
        
        if success:
            print(f"✅ Département {dept} chargé : {nrows:,} lignes\n")
            return True, nrows
        else:
            print(f"❌ Erreur lors du chargement du département {dept}\n")
            return False, 0
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur téléchargement département {dept}: {e}\n")
        return False, 0
    except Exception as e:
        print(f"❌ Erreur département {dept}: {e}\n")
        return False, 0

if __name__ == "__main__":
    print("🗺️  CHARGEMENT BAN DANS SNOWFLAKE")
    print("=" * 50)
    
    conn = get_snowflake_connection()
    
    if conn:
        start_time = datetime.now()
        total_lignes = 0
        succes = 0
        echecs = 0
        
        for i, dept in enumerate(DEPARTEMENTS):
            print(f"\n[{i+1}/{len(DEPARTEMENTS)}] Traitement département {dept}")
            print("-" * 50)
            
            success, nrows = telecharger_et_charger_departement(dept, conn)
            
            if success:
                succes += 1
                total_lignes += nrows
            else:
                echecs += 1
        
        conn.close()
        
        # Résumé
        duration = datetime.now() - start_time
        print("\n" + "=" * 50)
        print("✅ CHARGEMENT TERMINÉ !")
        print(f"⏱️  Durée : {duration}")
        print(f"📊 Départements réussis : {succes}/{len(DEPARTEMENTS)}")
        print(f"❌ Départements échoués : {echecs}")
        print(f"📈 Total lignes chargées : {total_lignes:,}")
        print("=" * 50)