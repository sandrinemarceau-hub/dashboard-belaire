import streamlit as st
import pandas as pd
import numpy as np
import re
import io
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="Usine Belaire - Dashboard & Sync", layout="wide")
st.title("🍾 Usine Belaire : Dashboard & Portail Client")

# --- FONCTION DE SYNCHRONISATION GOOGLE SHEETS ---
def mettre_a_jour_google_sheets(df):
    try:
        # Connexion sécurisée via les secrets Streamlit
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        
        # Ouverture du fichier Sheets
        sheet = client.open("Belaire_DB_Commandes").sheet1
        
        # Préparation des données (on ne garde que le résumé pour les clients)
        # On suppose que 'EXPE_NOM_CLIENT' est la colonne client
        cols_interet = ['NUM_CDE', 'EXPE_NOM_CLIENT', 'DATE_DISPO_ESTIMEE']
        df_sync = df[cols_interet].copy()
        df_sync['DERNIERE_MAJ'] = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        # On vide la feuille et on renvoie tout le nouveau contenu
        sheet.clear()
        sheet.update([df_sync.columns.values.tolist()] + df_sync.values.tolist())
        return True
    except Exception as e:
        st.error(f"Erreur de synchro Cloud : {e}")
        return False

# --- TOUTES TES FONCTIONS DE NETTOYAGE V19 (Rappelées ici pour le bon fonctionnement) ---
def nettoyer_code(val):
    if pd.isna(val) or str(val).lower() in ['nan', 'none', '']: return ""
    s = str(val).split('.')[0].upper()
    return re.sub(r'\D', '', s).lstrip('0')

def extraire_codes_multiples(val):
    if pd.isna(val) or str(val).lower() in ['nan', 'none', '']: return []
    return [c.lstrip('0') for c in re.findall(r'\d+', str(val))]

def extraire_code_prod(val):
    if pd.isna(val) or str(val).lower() in ['nan', 'none', '']: return ""
    s = str(val).upper()
    match = re.search(r'(?:VA)?0*(\d{4,6})', s)
    if match: return match.group(1).lstrip('0')
    return nettoyer_code(s)

def nettoyer_nombre(serie):
    return pd.to_numeric(serie.astype(str).str.replace(r'\s+', '', regex=True).str.replace(',', '.'), errors='coerce').fillna(0)

@st.cache_data
def lire_csv_streamlit(uploaded_file):
    if uploaded_file is None: return pd.DataFrame()
    raw_bytes = uploaded_file.read()
    encodages = ['utf-8', 'iso-8859-1', 'cp1252', 'mac_roman']
    separateurs = [';', ',', '\t']
    for enc in encodages:
        try:
            text = raw_bytes.decode(enc)
            for sep in separateurs:
                try:
                    df_raw = pd.read_csv(io.StringIO(text), sep=sep, dtype=str, on_bad_lines='skip', header=None)
                    if len(df_raw.columns) > 1:
                        header_row = 0
                        for i in range(min(20, len(df_raw))):
                            ligne = " ".join(df_raw.iloc[i].fillna("").astype(str).str.upper())
                            if ("CODE" in ligne or "ART" in ligne or "REF" in ligne) and ("QTE" in ligne or "QUANT" in ligne or "STOCK" in ligne or "PHYS" in ligne or "DATE" in ligne or "PLANIF" in ligne):
                                header_row = i
                                break
                        df = df_raw.iloc[header_row+1:].copy()
                        df.columns = [re.sub(r'[^\w\s]', '', str(c)).strip().upper() for c in df_raw.iloc[header_row].values]
                        df = df.loc[:, df.columns != '']
                        return df
                except: continue
        except: continue
    return pd.DataFrame()

# --- INTERFACE ---
col1, col2 = st.columns(2)
with col1:
    st.subheader("📦 Fichiers Principaux")
    f_bdd = st.file_uploader("1. Nomenclature (BDD)", type=['xlsx'])
    f_stock = st.file_uploader("2. Stock (.csv)", type=['csv'])
    f_cmd = st.file_uploader("3. Commandes (.csv)", type=['csv'])
with col2:
    st.subheader("🏭 OF Production")
    f_std = st.file_uploader("OF STD", type=['csv'])
    f_mgc = st.file_uploader("OF MGC", type=['csv'])
    f_roya = st.file_uploader("OF ROYA", type=['csv'])

if st.button("🚀 GÉNÉRER & SYNCHRONISER"):
    if not f_bdd or not f_stock or not f_cmd:
        st.error("Veuillez charger les fichiers obligatoires.")
    else:
        with st.spinner("Calculs et Mise à jour du Portail Client..."):
            # --- LOGIQUE DE CALCUL (Même que V19) ---
            df_mapping = pd.read_excel(f_bdd, dtype=str)
            df_commandes = lire_csv_streamlit(f_cmd)
            df_stocks = lire_csv_streamlit(f_stock)
            
            # [Ici se passe toute la logique de nettoyage et de calcul de DATE_DISPO_ESTIMEE...]
            # (Je raccourcis pour l'exemple mais garde bien TOUTE ta logique de calcul ici)
            # ... (Logique V19 complète) ...
            
            # --- SYNCHRO CLOUD ---
            success = mettre_a_jour_google_sheets(df_commandes)
            
            if success:
                st.success("🌐 Portail Client mis à jour en temps réel !")
            
            # --- TÉLÉCHARGEMENT EXCEL ---
            # (Code pour générer l'Excel habituel)
            st.download_button("📥 Télécharger l'Excel de l'Usine", data=output.getvalue(), file_name="DASHBOARD_BELAIRE.xlsx")
