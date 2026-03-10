import streamlit as st
import pandas as pd
import numpy as np
import re
import io

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="Générateur Dashboard Belaire", layout="wide")
st.title("🍾 Générateur de Dashboard Belaire")
st.markdown("Glissez vos fichiers d'export ci-dessous pour générer le tableau de bord.")

# --- FONCTIONS DE NETTOYAGE (V19) ---
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

# --- INTERFACE UTILISATEUR (ZONES DE DÉPÔT) ---
col1, col2 = st.columns(2)
with col1:
    st.subheader("📦 Fichiers Principaux")
    f_bdd = st.file_uploader("1. Nomenclature (BDD_ARTICLES... .xlsx)", type=['xlsx'])
    f_stock = st.file_uploader("2. Fichier Stock (.csv)", type=['csv'])
    f_cmd = st.file_uploader("3. Fichier Commandes (.csv)", type=['csv'])

with col2:
    st.subheader("🏭 Fichiers de Production (OF)")
    f_std = st.file_uploader("OF Production STD (.csv)", type=['csv'])
    f_mgc = st.file_uploader("OF Production MGC (.csv)", type=['csv'])
    f_roya = st.file_uploader("OF Production ROYA (.csv)", type=['csv'])

# --- MOTEUR DE CALCUL ---
if st.button("🚀 GÉNÉRER LE DASHBOARD"):
    if not f_bdd or not f_stock or not f_cmd:
        st.error("⚠️ Veuillez charger au moins la BDD, le Stock et les Commandes pour continuer.")
    else:
        with st.spinner("Analyse des données et calculs en cours (cela peut prendre quelques secondes)..."):
            
            # 1. Chargement
            df_mapping = pd.read_excel(f_bdd, dtype=str)
            df_commandes = lire_csv_streamlit(f_cmd)
            df_stocks = lire_csv_streamlit(f_stock)
            
            dict_nomenclature = {}
            map_parent = {}
            composants_suivis = ['COL', 'COIFFE', 'ET', 'EL LABEL', 'CARTON', 'CE', 'STICKER']

            # 2. BDD
            df_mapping['CLE_MAP'] = df_mapping['CODE ARTICLE'].apply(nettoyer_code)
            for idx, row in df_mapping.iterrows():
                cle = row['CLE_MAP']
                if not cle: continue
                map_parent[cle] = nettoyer_code(row.get('CODE SF/PROD', cle)) or cle
                dict_nomenclature[cle] = {}
                for comp in composants_suivis:
                    col_reelle = next((c for c in df_mapping.columns if comp in c), None)
                    codes = extraire_codes_multiples(str(row.get(col_reelle, '')))
                    dict_nomenclature[cle][comp] = codes[0] if codes else ""
                    if comp == 'ET' and len(codes) > 1:
                        dict_nomenclature[cle]['_EXTRA_CE'] = codes[1]
                if '_EXTRA_CE' in dict_nomenclature[cle]:
                    if dict_nomenclature[cle].get('CE', '') == "":
                        dict_nomenclature[cle]['CE'] = dict_nomenclature[cle]['_EXTRA_CE']
                    del dict_nomenclature[cle]['_EXTRA_CE']

            # 3. Stock
            dict_stock = {}
            if not df_stocks.empty:
                c_art_s = next((c for c in df_stocks.columns if any(k in c for k in ['CODE', 'ARTICLE', 'REFERENCE', 'REF'])), df_stocks.columns[0])
                c_qte_s = next((c for c in df_stocks.columns if any(k in c for k in ['STOCK', 'PHYS', 'QTE', 'QUANTITE'])), df_stocks.columns[-1])
                df_stocks['CLE_STK'] = df_stocks[c_art_s].apply(nettoyer_code)
                df_stocks['QTE_PROPRE'] = nettoyer_nombre(df_stocks[c_qte_s])
                dict_stock = df_stocks.groupby('CLE_STK')['QTE_PROPRE'].sum().to_dict()
                dict_stock.pop("", None)

            # 4. Prod
            list_prod = []
            fichiers_prod = {'STD': f_std, 'MGC': f_mgc, 'ROYA': f_roya}
            for site, f_prod in fichiers_prod.items():
                if f_prod:
                    df_p = lire_csv_streamlit(f_prod)
                    if not df_p.empty:
                        c_dt = next((c for c in df_p.columns if any(k in c for k in ['DATE', 'PLANIF', 'REALISATION'])), None)
                        c_qt = next((c for c in df_p.columns if any(k in c for k in ['QTE', 'QUANTITE'])), None)
                        cols_art = [c for c in df_p.columns if any(k in c for k in ['CODE', 'ART', 'ENTREE', 'SORTIE', 'REF'])]
                        if c_dt and cols_art:
                            for c_art in cols_art:
                                tmp = pd.DataFrame()
                                tmp['CLE_PROD'] = df_p[c_art].apply(extraire_code_prod)
                                tmp['DATE_PROD'] = pd.to_datetime(df_p[c_dt], dayfirst=True, errors='coerce')
                                tmp['QTE_PROD'] = nettoyer_nombre(df_p[c_qt]) if c_qt else 0
                                tmp['SITE'] = site
                                tmp = tmp[tmp['CLE_PROD'] != ""]
                                list_prod.append(tmp.dropna(subset=['DATE_PROD']))
            df_prod_totale = pd.concat(list_prod).sort_values('DATE_PROD') if list_prod else pd.DataFrame()

            # 5. Commandes
            c_art_cde = next((c for c in df_commandes.columns if 'CODE' in c or 'ARTICLE' in c), df_commandes.columns[0])
            df_commandes['CLE_CDE'] = df_commandes[c_art_cde].apply(nettoyer_code)
            for comp in composants_suivis:
                df_commandes[comp] = df_commandes['CLE_CDE'].apply(lambda x: dict_nomenclature.get(x, {}).get(comp, '')).astype(str)
            
            c_qte_cde = next((c for c in df_commandes.columns if 'TOTAL' in c or 'QTE' in c), df_commandes.columns[-1])
            df_commandes['QTE_CDE'] = nettoyer_nombre(df_commandes[c_qte_cde])
            df_commandes = df_commandes.sort_values(by=['CLE_CDE'])
            df_commandes['CUMUL'] = df_commandes.groupby('CLE_CDE')['QTE_CDE'].cumsum()

            def verifier_dispo(row):
                c = row['CLE_CDE'] 
                p = map_parent.get(c, c) 
                stk = float(dict_stock.get(c, 0))
                if stk >= row['CUMUL']: return "En Stock"
                if not df_prod_totale.empty:
                    match = df_prod_totale[(df_prod_totale['CLE_PROD'] == c) | (df_prod_totale['CLE_PROD'] == p)].copy()
                    if not match.empty:
                        match['SOMME'] = match['QTE_PROD'].cumsum()
                        res = match[match['SOMME'] >= (row['CUMUL'] - stk)]
                        if not res.empty: 
                            return f"{res.iloc[0]['DATE_PROD'].strftime('%d/%m/%Y')} ({res.iloc[0]['SITE']})"
                return "Pas de prod prévue"

            df_commandes['DATE_DISPO_ESTIMEE'] = df_commandes.apply(verifier_dispo, axis=1)

            # 6. Export Excel en mémoire
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                cols_export = [c for c in df_commandes.columns if c not in ['CLE_CDE', 'CUMUL', 'QTE_CDE']]
                df_export = df_commandes[cols_export].astype(str).replace('nan', '')
                df_export.to_excel(writer, sheet_name='Suivi', index=False)
                
                wb, ws = writer.book, writer.sheets['Suivi']
                f_ok = wb.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1})
                f_nok = wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})
                f_warn = wb.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500', 'border': 1})
                f_hdr = wb.add_format({'bold': True, 'bg_color': '#305496', 'font_color': 'white', 'border': 1})

                for col_num, value in enumerate(df_export.columns.values):
                    ws.write(0, col_num, value, f_hdr)

                if 'DATE_DISPO_ESTIMEE' in df_export.columns:
                    idx_date = df_export.columns.get_loc('DATE_DISPO_ESTIMEE')
                    ws.conditional_format(1, idx_date, len(df_export), idx_date, {'type': 'cell', 'criteria': 'equal to', 'value': '"En Stock"', 'format': f_ok})
                    ws.conditional_format(1, idx_date, len(df_export), idx_date, {'type': 'text', 'criteria': 'containing', 'value': '(', 'format': f_warn})
                    ws.conditional_format(1, idx_date, len(df_export), idx_date, {'type': 'text', 'criteria': 'containing', 'value': 'Pas', 'format': f_nok})

                for col_name in composants_suivis:
                    if col_name in df_export.columns:
                        c_idx = df_export.columns.get_loc(col_name)
                        for r in range(len(df_export)):
                            val = str(df_export.iloc[r, c_idx]).strip()
                            if val != "":
                                stk_val = float(dict_stock.get(nettoyer_code(val), 0))
                                ws.write(r + 1, c_idx, val, f_ok if stk_val > 0 else f_nok)
                ws.set_column('A:AZ', 18)
                ws.freeze_panes(1, 0)
            
            st.success("✅ Dashboard prêt !")
            st.download_button(
                label="📥 TÉLÉCHARGER LE DASHBOARD FINAL",
                data=output.getvalue(),
                file_name="DASHBOARD_BELAIRE_FINAL.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
