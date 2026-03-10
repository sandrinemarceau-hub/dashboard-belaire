import streamlit as st
import pandas as pd
import numpy as np
import re
import io
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(page_title="Usine Belaire - Dashboard & Sync", layout="wide")
st.title("🍾 Usine Belaire : Dashboard & Portail Client")

# --- FONCTIONS DE NETTOYAGE ---
def nettoyer_code(val):
    if isinstance(val, (pd.Series, np.ndarray, list)): val = val[0]
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
    for enc in ['utf-8', 'iso-8859-1', 'cp1252', 'mac_roman']:
        try:
            text = raw_bytes.decode(enc)
            for sep in [';', ',', '\t']:
                try:
                    df_raw = pd.read_csv(io.StringIO(text), sep=sep, dtype=str, on_bad_lines='skip', header=None)
                    if len(df_raw.columns) > 1:
                        header_row = 0
                        for i in range(min(15, len(df_raw))):
                            ligne = " ".join(df_raw.iloc[i].fillna("").astype(str).str.upper())
                            if ("CODE" in ligne or "ART" in ligne or "REF" in ligne) and ("QTE" in ligne or "STOCK" in ligne):
                                header_row = i
                                break
                        df = df_raw.iloc[header_row+1:].copy()
                        df.columns = [str(c).strip().upper() for c in df_raw.iloc[header_row].values]
                        return df
                except: continue
        except: continue
    return pd.DataFrame()

def calculer_date_max_robuste(serie_dates):
    liste = [str(d).strip() for d in serie_dates.tolist() if pd.notna(d)]
    if any("Pas de prod prévue" in s for s in liste): return "No production planned"
    dates_trouvees = []
    for s in liste:
        match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', s)
        if match:
            try:
                d_obj = datetime.strptime(match.group(1), "%d/%m/%Y")
                dates_trouvees.append(d_obj)
            except: continue
    if dates_trouvees: return max(dates_trouvees).strftime("%d/%m/%Y")
    return "In Stock"

# --- SYNCHRO CLOUD AVEC RÉFÉRENCE CLIENT ---
def mettre_a_jour_google_sheets(df_global):
    try:
        if "json_key" not in st.secrets: return False
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_info = json.loads(st.secrets["json_key"])
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open("Belaire_DB_Commandes").sheet1
        
        # Identification des colonnes
        col_cde = next((c for c in df_global.columns if 'NUM' in c and 'CDE' in c), df_global.columns[0])
        col_cli = next((c for c in df_global.columns if 'CLI' in c), df_global.columns[1])
        
        # On récupère la colonne C (index 2) pour la référence client
        col_ref_client = df_global.columns[2] 
        
        df_temp = df_global.copy()
        df_temp[col_cde] = df_temp[col_cde].astype(str).str.strip()
        
        # Synthèse par commande (on garde la ref client de la colonne C)
        df_client = df_temp.groupby(col_cde).agg({
            col_ref_client: 'first',
            col_cli: 'first',
            'DATE_DISPO_ESTIMEE': lambda x: calculer_date_max_robuste(x)
        }).reset_index()
        
        df_client['DERNIERE_MAJ'] = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        # Nouveaux noms de colonnes pour le Sheets
        df_client.columns = ['INTERNAL_ID', 'CUSTOMER_REF', 'CLIENT_NAME', 'DISPO_DATE', 'LAST_UPDATE']

        sheet.clear()
        sheet.update([df_client.columns.values.tolist()] + df_client.values.tolist())
        return True
    except Exception as e:
        st.error(f"Sync error : {e}")
        return False

# --- INTERFACE ---
col1, col2 = st.columns(2)
with col1:
    st.subheader("📦 Main Files")
    f_bdd = st.file_uploader("1. Nomenclature (xlsx)", type=['xlsx'])
    f_stock = st.file_uploader("2. Stock (csv)", type=['csv'])
    f_cmd = st.file_uploader("3. Orders (csv)", type=['csv'])
with col2:
    st.subheader("🏭 Production (OF)")
    f_std = st.file_uploader("OF STD", type=['csv'])
    f_mgc = st.file_uploader("OF MGC", type=['csv'])
    f_roya = st.file_uploader("OF ROYA", type=['csv'])

if st.button("🚀 GENERATE & SYNC"):
    if not f_bdd or not f_stock or not f_cmd:
        st.error("Missing files.")
    else:
        with st.spinner("Processing..."):
            df_mapping = pd.read_excel(f_bdd, dtype=str)
            df_commandes = lire_csv_streamlit(f_cmd)
            df_stocks = lire_csv_streamlit(f_stock)
            
            # Logiciel de calcul (identique à précédemment)
            # ... [Le reste du code de calcul reste le même] ...
            # [Note: J'ai raccourci ici pour la lecture, mais garde bien TOUTE ta logique de calcul entre les deux]
            
            # --- CALCULS ---
            df_mapping['CLE_MAP'] = df_mapping['CODE ARTICLE'].apply(nettoyer_code)
            map_parent = {row['CLE_MAP']: nettoyer_code(row.get('CODE SF/PROD', row['CLE_MAP'])) for _, row in df_mapping.iterrows() if row['CLE_MAP']}
            
            # Stocks
            c_art_s = next((c for c in df_stocks.columns if 'CODE' in c or 'ARTICLE' in c), df_stocks.columns[0])
            c_qte_s = next((c for c in df_stocks.columns if 'STOCK' in c or 'PHYS' in c or 'QTE' in c), df_stocks.columns[-1])
            df_stocks['CLE_STK'] = df_stocks[c_art_s].apply(nettoyer_code)
            df_stocks['QTE_PROPRE'] = nettoyer_nombre(df_stocks[c_qte_s])
            dict_stock = df_stocks.groupby('CLE_STK')['QTE_PROPRE'].sum().to_dict()

            # Production fusionnée
            list_prod = []
            for site, f_p in {'STD': f_std, 'MGC': f_mgc, 'ROYA': f_roya}.items():
                if f_p:
                    df_p = lire_csv_streamlit(f_p)
                    if not df_p.empty:
                        c_dt = next((c for c in df_p.columns if 'DATE' in c or 'PLANIF' in c), None)
                        c_qt = next((c for c in df_p.columns if 'QTE' in c or 'QUANT' in c), None)
                        cols_art = [c for c in df_p.columns if any(k in c for k in ['CODE', 'ART', 'REF'])]
                        if c_dt and cols_art:
                            for c_art in cols_art:
                                tmp = pd.DataFrame()
                                tmp['CLE_PROD'] = df_p[c_art].apply(extraire_code_prod)
                                tmp['DATE_PROD'] = pd.to_datetime(df_p[c_dt], dayfirst=True, errors='coerce')
                                tmp['QTE_PROD'] = nettoyer_nombre(df_p[c_qt])
                                tmp['SITE'] = site
                                list_prod.append(tmp.dropna(subset=['DATE_PROD']))
            df_prod_totale = pd.concat(list_prod).sort_values('DATE_PROD') if list_prod else pd.DataFrame()

            # Dispo
            c_art_cde = next((c for c in df_commandes.columns if 'CODE' in c or 'ARTICLE' in c), df_commandes.columns[0])
            df_commandes['CLE_CDE'] = df_commandes[c_art_cde].apply(nettoyer_code)
            c_qte_cde = next((c for c in df_commandes.columns if 'TOTAL' in c or 'QTE' in c), df_commandes.columns[-1])
            df_commandes['QTE_CDE'] = nettoyer_nombre(df_commandes[c_qte_cde])
            df_commandes = df_commandes.sort_values(by=['CLE_CDE'])
            df_commandes['CUMUL'] = df_commandes.groupby('CLE_CDE')['QTE_CDE'].cumsum()

            def verifier_dispo(row):
                c = row['CLE_CDE'] 
                stk = float(dict_stock.get(c, 0))
                if stk >= row['CUMUL']: return "In Stock"
                if not df_prod_totale.empty:
                    p = map_parent.get(c, c)
                    match = df_prod_totale[(df_prod_totale['CLE_PROD'] == c) | (df_prod_totale['CLE_PROD'] == p)].copy()
                    if not match.empty:
                        match['SOMME'] = match['QTE_PROD'].cumsum()
                        res = match[match['SOMME'] >= (row['CUMUL'] - stk)]
                        if not res.empty: return f"{res.iloc[0]['DATE_PROD'].strftime('%d/%m/%Y')} ({res.iloc[0]['SITE']})"
                return "No production planned"

            df_commandes['DATE_DISPO_ESTIMEE'] = df_commandes.apply(verifier_dispo, axis=1)

            # Export Excel
            output = io.BytesIO()
            df_commandes.to_excel(output, index=False, engine='xlsxwriter')
            st.success("✅ Dashboard Ready")
            st.download_button("📥 Download Excel", data=output.getvalue(), file_name="DASHBOARD_BELAIRE.xlsx")
            
            # SYNCHRO
            if mettre_a_jour_google_sheets(df_commandes):
                st.info("🌐 Client Portal updated with Customer References!")
