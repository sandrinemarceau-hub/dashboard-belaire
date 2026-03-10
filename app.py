# --- FONCTION DE CALCUL DE LA DATE MAX PAR COMMANDE ---
def calculer_date_max_commande(serie_dates):
    liste_dates = serie_dates.astype(str).tolist()
    
    # 1. Si un article est critique, toute la commande l'est
    if any("Pas de prod prévue" in d for d in liste_dates):
        return "Pas de prod prévue"
    
    # 2. Extraire les dates réelles pour trouver la plus lointaine
    dates_obj = []
    for d in liste_dates:
        # On cherche le format JJ/MM/AAAA dans la chaîne
        match = re.search(r'(\d{2}/\d{2}/\d{4})', d)
        if match:
            dates_obj.append(datetime.strptime(match.group(1), "%d/%m/%Y"))
    
    if dates_obj:
        # On renvoie la date la plus lointaine au format texte
        return max(dates_obj).strftime("%d/%m/%Y")
    
    # 3. Si aucune date trouvée, c'est que tout est en stock
    return "En Stock"

# --- NOUVELLE FONCTION DE SYNCHRO ---
def mettre_a_jour_google_sheets(df_global):
    try:
        if "json_key" not in st.secrets:
            st.error("Secret manquant.")
            return False
            
        # Connexion
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_info = json.loads(st.secrets["json_key"])
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open("Belaire_DB_Commandes").sheet1

        # --- LOGIQUE DE SYNTHÈSE PAR COMMANDE ---
        # On regroupe par NUM_CDE pour n'avoir qu'une ligne par commande
        df_client = df_global.groupby('NUM_CDE').agg({
            'EXPE_NOM_CLIENT': 'first',
            'DATE_DISPO_ESTIMEE': lambda x: calculer_date_max_commande(x)
        }).reset_index()
        
        df_client['DERNIERE_MAJ'] = datetime.now().strftime("%d/%m/%Y %H:%M")

        # Mise à jour du Sheets
        sheet.clear()
        sheet.update([df_client.columns.values.tolist()] + df_client.values.tolist())
        return True
    except Exception as e:
        st.error(f"Erreur Synchro : {e}")
        return False

# --- (Dans le bloc IF du bouton principal, à la fin) ---
# Assurez-vous que l'appel ressemble à ça :
# success = mettre_a_jour_google_sheets(df_commandes)
