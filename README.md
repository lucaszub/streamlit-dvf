# 🏠 Application DVF - Analyse des Valeurs Foncières

Application Streamlit pour analyser les données de valeurs foncières stockées dans Snowflake.

## 📋 Fonctionnalités

### 🔍 Page de Recherche
- Recherche par commune et rue
- Filtrage par type de bien (Maison, Appartement, Local commercial)
- Statistiques détaillées (nombre de transactions, prix moyen, prix médian, surface moyenne)
- Graphiques interactifs :
  - Évolution des prix dans le temps
  - Distribution des prix
  - Prix moyen par type de bien
- Tableau détaillé des transactions
- Export des données en CSV

### 📈 Page d'Analyse Temporelle
- Analyse par période (année, trimestre, mois)
- Prix médian et prix moyen par période
- Filtrage par département, commune et type de bien
- Filtrage par plage de dates
- Graphiques d'évolution temporelle :
  - Évolution du prix médian et moyen
  - Nombre de transactions par période
  - Variation du prix médian (%)
  - Comparaison par type de bien (Maison vs Appartement)
- Statistiques détaillées par type de bien
- Export des données temporelles en CSV

## 🚀 Installation

1. Cloner le repository

```bash
git clone <url-du-repo>
cd streamlit-dvf
```

2. Créer un environnement virtuel

```bash
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate
```

3. Installer les dépendances

```bash
pip install -r requirements.txt
```

4. Configurer la connexion Snowflake

Copier le fichier d'exemple et remplir avec vos identifiants :

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

Puis éditer `.streamlit/secrets.toml` avec vos informations Snowflake :

```toml
[snowflake]
user = "votre_username"
password = "votre_password"
account = "votre_account"
warehouse = "votre_warehouse"
database = "VALFONC_ANALYTICS"
schema = "GOLD"
```

## 🎯 Utilisation

Lancer l'application :

```bash
streamlit run app.py
```

L'application sera accessible à l'adresse : http://localhost:8501

## 📊 Structure des données

L'application utilise le semantic layer `VALFONC_ANALYTICS.GOLD.DVF` qui contient :

- **DIM_ADDRESS** : Adresses avec rue, code postal, commune
- **DIM_CODE_POSTAL** : Codes postaux et informations géographiques associées
- **DIM_COMMUNE** : Communes et départements
- **DIM_PARCELLE** : Parcelles cadastrales avec sections et numéros de plan
- **DIM_TYPE_LOCAL** : Types de locaux (maison, appartement, etc.)
- **FACT_MUTATION** : Transactions immobilières avec valeur foncière, date, surfaces

Pour une documentation complète du semantic layer, consultez le fichier `.claude` à la racine du projet.

## 🛠️ Technologies utilisées

- **Streamlit** : Framework web pour l'interface
- **Snowflake** : Base de données cloud
- **Pandas** : Manipulation de données
- **Plotly** : Visualisations interactives

## 📝 Notes

- Les données sont mises en cache pour améliorer les performances
- La limite de résultats est fixée à 5000 transactions par requête
- L'export CSV contient toutes les colonnes de données brutes
