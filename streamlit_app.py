# Import required libraries
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px

# Get the current active Snowflake session
session = get_active_session()

# Simple page setup
st.title("Analyse des Offres d'Emploi")
st.write("Cette application analyse les données des offres d'emploi provenant de Snowflake.")

# Analyse 1 : Top 10 des titres de postes les plus publiés par industrie
st.header("Top 10 des Titres de Postes les Plus Publiés par Industrie")
query_top_titles = """
SELECT
  ji.industry_id AS INDUSTRY,
  jp.title AS TITLE,
  COUNT(*) AS JOB_COUNT
FROM jobs_posting jp
JOIN job_industries ji ON jp.job_id = ji.job_id
GROUP BY ji.industry_id, jp.title
QUALIFY ROW_NUMBER() OVER (PARTITION BY ji.industry_id ORDER BY COUNT(*) DESC) <= 10
ORDER BY INDUSTRY, JOB_COUNT DESC;
"""
df_top_titles = session.sql(query_top_titles).to_pandas()

# Renommer les colonnes si nécessaire
df_top_titles = df_top_titles.rename(columns={
    "INDUSTRY": "industry",
    "TITLE": "title",
    "JOB_COUNT": "job_count"
})

# Créer le graphique
fig_top_titles = px.bar(df_top_titles, x="industry", y="job_count", color="title", title="Top 10 des Titres par Industrie")
st.plotly_chart(fig_top_titles)

# Analyse 2 : Top 10 des postes les mieux rémunérés par industrie
st.header("Top 10 des Postes les Mieux Rétribués par Industrie")
query_top_salaries = """
SELECT
  ji.industry_id AS INDUSTRY,
  jp.title AS TITLE,
  jp.med_salary AS MED_SALARY
FROM jobs_posting jp
JOIN job_industries ji ON jp.job_id = ji.job_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY ji.industry_id ORDER BY jp.med_salary DESC NULLS LAST) <= 10
ORDER BY INDUSTRY, MED_SALARY DESC;
"""
df_top_salaries = session.sql(query_top_salaries).to_pandas()

# Renommer les colonnes si nécessaire
df_top_salaries = df_top_salaries.rename(columns={
    "INDUSTRY": "industry",
    "TITLE": "title",
    "MED_SALARY": "med_salary"
})

# Créer le graphique
fig_top_salaries = px.bar(df_top_salaries, x="industry", y="med_salary", color="title", title="Top 10 des Postes les Mieux Rétribués")
st.plotly_chart(fig_top_salaries)

# Analyse 3 : Répartition des offres d'emploi par taille d'entreprise
st.header("Répartition des Offres d'Emploi par Taille d'Entreprise")
query_company_size = """
SELECT
  c.company_size AS COMPANY_SIZE,
  COUNT(jp.job_id) AS JOB_COUNT
FROM jobs_posting jp
JOIN companies c ON LOWER(TRIM(jp.company_name)) = LOWER(TRIM(c.name))
GROUP BY c.company_size
ORDER BY JOB_COUNT DESC;
"""
df_company_size = session.sql(query_company_size).to_pandas()

# Renommer les colonnes si nécessaire
df_company_size = df_company_size.rename(columns={
    "COMPANY_SIZE": "company_size",
    "JOB_COUNT": "job_count"
})

# Vérification du contenu du DataFrame
if df_company_size.empty:
    st.warning("Aucune donnée disponible pour cette analyse.")
else:
    # Créer le graphique
    fig_company_size = px.pie(df_company_size, names="company_size", values="job_count", title="Répartition par Taille d'Entreprise")
    st.plotly_chart(fig_company_size)

# Analyse 4 : Répartition des offres d'emploi par secteur d'activité
st.header("Répartition des Offres d'Emploi par Secteur d'Activité")
query_industries = """
SELECT
  ji.industry_id AS INDUSTRY,
  COUNT(*) AS JOB_COUNT
FROM jobs_posting jp
JOIN job_industries ji ON jp.job_id = ji.job_id
GROUP BY ji.industry_id
ORDER BY JOB_COUNT DESC;
"""
df_industries = session.sql(query_industries).to_pandas()

# Renommer les colonnes si nécessaire
df_industries = df_industries.rename(columns={
    "INDUSTRY": "industry",
    "JOB_COUNT": "job_count"
})

# Créer le graphique
fig_industries = px.bar(df_industries, x="industry", y="job_count", title="Répartition par Secteur d'Activité")
st.plotly_chart(fig_industries)

# Analyse 5 : Répartition des offres d'emploi par type d'emploi
st.header("Répartition des Offres d'Emploi par Type d'Emploi")
query_work_type = """
SELECT
  formatted_work_type AS WORK_TYPE,
  COUNT(*) AS JOB_COUNT
FROM jobs_posting
GROUP BY formatted_work_type
ORDER BY JOB_COUNT DESC;
"""
df_work_type = session.sql(query_work_type).to_pandas()

# Renommer les colonnes si nécessaire
df_work_type = df_work_type.rename(columns={
    "WORK_TYPE": "work_type",
    "JOB_COUNT": "job_count"
})

# Créer le graphique
fig_work_type = px.pie(df_work_type, names="work_type", values="job_count", title="Répartition par Type d'Emploi")
st.plotly_chart(fig_work_type)