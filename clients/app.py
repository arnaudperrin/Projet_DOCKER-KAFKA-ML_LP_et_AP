import os
import json
import time
import threading
from collections import defaultdict
from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_COMMANDES = "commandes-creees"
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clients-group")

# Données TP1 conservées
clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

# Nouveauté TP2 : compteur du nombre total de commandes par client_id
commandes_par_client = defaultdict(int)

# --- Endpoints REST TP1 conservés ---
@app.route('/clients', methods=['GET'])
def get_clients():
    return jsonify(clients), 200

@app.route('/clients/<int:client_id>', methods=['GET'])
def get_client(client_id):
    client = next((c for c in clients if c["id"] == client_id), None)
    if client is None:
        return jsonify({"erreur": "Client introuvable"}), 404
    # On ajoute au passage le total de commandes consommées
    client_response = dict(client)
    client_response["total_commandes"] = commandes_par_client[client_id]
    return jsonify(client_response), 200

# --- Nouveauté TP2 : Consommateur Kafka exécuté en arrière-plan ---
def demarrer_consommateur_kafka():
    consumer = None
    for _ in range(15):
        try:
            consumer = KafkaConsumer(
                TOPIC_COMMANDES,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',  # Ne perd pas les messages envoyés quand le service était éteint
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[Clients] Consommateur Kafka connecté au groupe '{GROUP_ID}'.")
            break
        except Exception as e:
            print(f"[Clients] En attente de Kafka ({e})...")
            time.sleep(2)

    if not consumer:
        print("[Clients] Impossible de démarrer le consommateur Kafka.")
        return

    for message in consumer:
        event = message.value
        client_id = event.get("client_id")
        produit = event.get("produit")
        montant = event.get("montant")

        # Trouver le client correspondant
        client = next((c for c in clients if c["id"] == client_id), None)
        nom_client = client["nom"] if client else f"Inconnu ({client_id})"

        # Incrémenter le total de commandes pour ce client
        commandes_par_client[client_id] += 1

        print("=" * 50)
        print(f"[Kafka EVENT REÇU] Client: {nom_client} | Produit: {produit} | Montant: {montant}€")
        print(f"[Total commandes] Le client {nom_client} (id: {client_id}) a passé {commandes_par_client[client_id]} commande(s) au total.")
        print("=" * 50, flush=True)

if __name__ == '__main__':
    # Démarrage du thread consommateur
    thread = threading.Thread(target=demarrer_consommateur_kafka, daemon=True)
    thread.start()
    app.run(host='0.0.0.0', port=8080)