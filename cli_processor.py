# cli_processor.py
import argparse
import csv
import os
import sys
import json
import re
import time
from datetime import datetime
from dotenv import load_dotenv  

load_dotenv()

# Importiere deine existierenden Module
from config import load_config
from trello_client import TrelloClient
from hubspot_client import HubSpotClient
from hubspot_write import HubSpotWriteClient
from openai_assistant_client import OpenAIAssistantClient

# --- Hilfsfunktionen ---

def clean_html(raw_html):
    """Entfernt HTML-Tags für Step 2 (reiner Text)."""
    if not raw_html:
        return ""
    cleanr = re.compile('<.*?>')
    text = re.sub(cleanr, '', raw_html)
    return text.strip()

def normalize_email(email):
    return email.strip().lower() if email else ""

def get_timestamp_iso(ts_ms):
    """Konvertiert HubSpot Timestamp (ms) in lesbares ISO Datum."""
    if not ts_ms: return ""
    try:
        return datetime.fromtimestamp(int(ts_ms)/1000).isoformat()
    except:
        return str(ts_ms)

# --- Hauptlogik ---

def run_processing(csv1_path, csv2_path, output_csv_path, auto_mode=False):
    # 1. Konfiguration laden
    try:
        app_cfg, trello_cfg, hs_cfg, oa_cfg = load_config()
    except Exception as e:
        print(f"Fehler beim Laden der Config (.env prüfen!): {e}")
        sys.exit(1)

    # Clients initialisieren
    trello_client = TrelloClient(trello_cfg)
    hs_read_client = HubSpotClient(hs_cfg)
    n_c_id = getattr(hs_cfg, "note_to_contact_type_id", 0)
    n_d_id = getattr(hs_cfg, "note_to_deal_type_id", 0)
    
    if n_c_id == 0:
        print("WARNUNG: HS_ASSOC_NOTE_TO_CONTACT_TYPE_ID ist 0 oder nicht gesetzt in .env!")

    hs_write_client = HubSpotWriteClient(
        hs_cfg, 
        note_to_contact_type_id=n_c_id,
        note_to_deal_type_id=n_d_id
    )
    ai_client = OpenAIAssistantClient(oa_cfg)

    # 2. CSVs einlesen
    print(f"Lese CSV 1 (HubSpot): {csv1_path}")
    with open(csv1_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        hubspot_rows = list(reader)
        fieldnames = reader.fieldnames

    # Prüfen ob notwendige Spalten für Status-Update existieren, sonst hinzufügen
    if "STATUS" not in fieldnames: fieldnames.append("STATUS")
    if "NOTE_ID" not in fieldnames: fieldnames.append("NOTE_ID")

    print(f"Lese CSV 2 (Trello Mapping): {csv2_path}")
    email_to_trello = {}
    with open(csv2_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Passe hier ggf. die Spaltennamen an deine CSV an!
            # Ich gehe von 'email' und 'trello_id' aus, wie im Code oft genutzt.
            email = normalize_email(row.get('email', '') or row.get('Email', ''))
            tid = row.get('trello_id', '') or row.get('Trello ID', '')
            if email and tid:
                email_to_trello[email] = tid.strip()

    # Ausgabedatei vorbereiten (wir kopieren existierende Einträge, wenn Output schon existiert)
    # Um Datenverlust zu vermeiden, arbeiten wir auf der Liste im Speicher und schreiben nach jedem Row.
    # Wenn output_csv_path schon existiert, laden wir deren Status.
    processed_status = {}
    if os.path.exists(output_csv_path):
        print(f"Lade bestehenden Fortschritt aus {output_csv_path}...")
        with open(output_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                hs_id = row.get('hubspot_contact_id') or row.get('Contact ID') # Anpassung an deine Spaltennamen
                if hs_id and row.get('STATUS') == 'DONE':
                    processed_status[hs_id] = row.get('NOTE_ID')

    # Datei zum Schreiben öffnen (append mode simulieren wir durch komplettes Neuschreiben oder wir schreiben Zeile für Zeile)
    # Sicherer: Wir schreiben alles neu in die Output Datei.
    
    # 3. Iteration
    total = len(hubspot_rows)
    print(f"\nStarte Verarbeitung von {total} Kontakten...")
    print(f"Modus: {'AUTOMATISCH' if auto_mode else 'INTERAKTIV (Bestätigung erforderlich)'}\n")

    processed_rows = []

    for index, row in enumerate(hubspot_rows, start=1):
        # Spaltennamen anpassen (CSV 1)
        hs_id = row.get('hubspot_contact_id') or row.get('Contact ID')
        email = normalize_email(row.get('email') or row.get('Email'))
        
        # Check ob schon fertig
        if hs_id in processed_status:
            row['STATUS'] = 'DONE'
            row['NOTE_ID'] = processed_status[hs_id]
            processed_rows.append(row)
            # print(f"[{index}/{total}] Skipped {email} (bereits DONE)")
            continue

        print(f"--------------------------------------------------")
        print(f"[{index}/{total}] Bearbeite: {email} (ID: {hs_id})")

        # Matching Trello
        trello_id = email_to_trello.get(email)
        if not trello_id:
            print(" -> KEIN Trello Match gefunden. Überspringe.")
            row['STATUS'] = 'SKIPPED_NO_TRELLO'
            processed_rows.append(row)
            write_csv(output_csv_path, fieldnames, processed_rows)
            continue

        try:
            # --- STEP 1: Trello Fetch ---
            print(f" -> Step 1: Hole Trello Daten (ID: {trello_id})...")
            # Wir nutzen deine TrelloClient Logik, aber holen die Details manuell um Text zu bauen
            # Da trello_client.fetch_card_bundle nicht direkt im Client ist (sondern im runner),
            # nutzen wir hier simple requests über den Client wrapper falls möglich, 
            # oder wir simulieren die Logik aus step1_trello_fetch.
            
            # Um es einfach zu halten, nehmen wir an, der User hat step1_trello_fetch.py nicht als Library verfügbar.
            # Wir rufen direkt die Trello API über deinen Client auf.
            card_json = trello_client._get(f"/cards/{trello_id}", params={"fields": "name,desc,url"})
            actions_json = trello_client._get(f"/cards/{trello_id}/actions", params={"filter": "commentCard", "limit": 100})
            checklists_json = trello_client._get(f"/cards/{trello_id}/checklists")

            # Text bauen
            trello_text_parts = [f"TRELLO CARD: {card_json.get('name')}"]
            if card_json.get('desc'):
                trello_text_parts.append(f"DESC: {card_json.get('desc')}")
            
            if checklists_json:
                trello_text_parts.append("\nCHECKLISTS:")
                for cl in checklists_json:
                    trello_text_parts.append(f"- {cl.get('name')}:")
                    for item in cl.get('checkItems', []):
                        state = "[x]" if item['state'] == 'complete' else "[ ]"
                        trello_text_parts.append(f"  {state} {item['name']}")
            
            if actions_json:
                trello_text_parts.append("\nCOMMENTS:")
                for act in actions_json:
                    txt = act.get('data', {}).get('text', '')
                    date = act.get('date', '')
                    if txt:
                        trello_text_parts.append(f"[{date}] {txt}")
            
            full_trello_text = "\n".join(trello_text_parts)

            # --- STEP 2: HubSpot Fetch ---
            print(f" -> Step 2: Hole HubSpot Notizen & Anrufe...")
            # IDs holen
            note_ids = hs_read_client.list_associated_object_ids(hs_id, "notes")
            call_ids = hs_read_client.list_associated_object_ids(hs_id, "calls")
            deal_ids = hs_read_client.list_associated_object_ids(hs_id, "deals")
            
            hs_text_parts = []
            
            # Batch Read Notes
            if note_ids:
                notes_data = hs_read_client.batch_read_objects("notes", note_ids, properties=["hs_note_body", "hs_timestamp"])
                for n in notes_data:
                    props = n.get('properties', {})
                    ts = get_timestamp_iso(props.get('hs_timestamp'))
                    body = clean_html(props.get('hs_note_body', ''))
                    if body:
                        hs_text_parts.append(f"NOTE [{ts}]: {body}")

            # Batch Read Calls
            if call_ids:
                calls_data = hs_read_client.batch_read_objects("calls", call_ids, properties=["hs_call_body", "hs_call_outcome", "hs_timestamp"])
                for c in calls_data:
                    props = c.get('properties', {})
                    ts = get_timestamp_iso(props.get('hs_timestamp'))
                    outcome = props.get('hs_call_outcome', '')
                    body = clean_html(props.get('hs_call_body', ''))
                    hs_text_parts.append(f"CALL [{ts}] (Outcome: {outcome}): {body}")

            full_hs_text = "\n".join(hs_text_parts)

            # --- STEP 3: AI Assistant ---
            print(f" -> Step 3: Sende an AI Assistant...")
            merged_context = f"=== TRELLO DATEN ===\n{full_trello_text}\n\n=== HUBSPOT DATEN ===\n{full_hs_text}"
            
            # Hier nutzen wir den Prompt aus step3_openai_assistant oder main
            # Wir wollen aber direkt die Zusammenfassung für den Note-Body.
            # Da dein Setup Schritt 3 (JSON Analyse) und Schritt 4 (HTML Render) trennt,
            # emulieren wir das hier verkürzt: Wir bitten die AI um die finale HTML Notiz.
            
            prompt = (
                "Du bist ein CRM Assistent. Analysiere die folgenden Daten aus Trello und HubSpot. "
                "Erstelle eine Zusammenfassung als HTML-Notiz (nutze <b>, <ul>, <li>, <br>). "
                "Fasse Erfolge, Herausforderungen und den aktuellen Status zusammen. "
                "Sei präzise und professionell."
                "\n\nDATEN:\n" + merged_context
            )
            
            # Wir nutzen summarize_with_assistant, aber geben den Prompt so, dass wir Text zurückbekommen
            # Achtung: Deine Assistant Funktion erwartet JSON-Output vom Assistant im step3-file?
            # Im `openai_assistant_client.py` gibst du nur den Text zurück. Das ist gut.
            ai_output = ai_client.summarize_with_assistant(merged_context, extra_user_prompt=prompt)
            
            # --- STEP 4: Review & Write ---
            print("\n--- VORSCHAU DER GENERIERTEN NOTIZ ---")
            # Kurze Vorschau (stripped) für die Konsole
            print(clean_html(ai_output)[:500] + "...") 
            print("--------------------------------------")

            should_write = False
            if auto_mode:
                should_write = True
            else:
                user_in = input(">> Notiz in HubSpot schreiben? (j/n/s=skip): ").strip().lower()
                if user_in == 'j' or user_in == 'y':
                    should_write = True
                elif user_in == 's':
                    print("Übersprungen.")
                    row['STATUS'] = 'SKIPPED_USER'
                    processed_rows.append(row)
                    write_csv(output_csv_path, fieldnames, processed_rows)
                    continue
                else:
                    print("Abgebrochen für diesen Kontakt.")
                    row['STATUS'] = 'REJECTED'
                    processed_rows.append(row)
                    write_csv(output_csv_path, fieldnames, processed_rows)
                    continue

            if should_write:
                print(f" -> Schreibe Notiz in HubSpot (Kontakt {hs_id} + {len(deal_ids)} Deals)...")
                
                # NEUER AUFRUF:
                note_id = hs_write_client.create_note_html_with_associations(
                    html_body=ai_output,
                    contact_id=hs_id,
                    deal_ids=deal_ids  # Hier übergeben wir die Deals!
                )
                
                print(f" -> ERFOLG! Note ID: {note_id}")
                
                row['STATUS'] = 'DONE'
                row['NOTE_ID'] = note_id
                processed_rows.append(row)
                write_csv(output_csv_path, fieldnames, processed_rows)
                time.sleep(1)

        except Exception as e:
            print(f"!!! FEHLER bei {email}: {e}")
            row['STATUS'] = f"ERROR: {str(e)}"
            processed_rows.append(row)
            write_csv(output_csv_path, fieldnames, processed_rows)

    print(f"\nFertig. Ergebnisse gespeichert in: {output_csv_path}")

def write_csv(path, fieldnames, rows):
    """Schreibt CSV komplett neu (atomares Update simulieren)."""
    # Temporär schreiben, dann umbenennen wäre sicherer, aber hier reicht direct write
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

# --- CLI Entry Point ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verarbeitet Kontakte aus CSV mit Trello & HubSpot Daten.")
    parser.add_argument("csv1", help="Pfad zur CSV 1 (HubSpot IDs & Emails)")
    parser.add_argument("csv2", help="Pfad zur CSV 2 (Emails & Trello IDs)")
    parser.add_argument("--out", default="processed_contacts.csv", help="Ausgabe CSV Pfad")
    parser.add_argument("--auto", action="store_true", help="Deaktiviert Benutzerbestätigung (Vorsicht!)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv1):
        print(f"Datei nicht gefunden: {args.csv1}")
        sys.exit(1)
    if not os.path.exists(args.csv2):
        print(f"Datei nicht gefunden: {args.csv2}")
        sys.exit(1)

    run_processing(args.csv1, args.csv2, args.out, args.auto)