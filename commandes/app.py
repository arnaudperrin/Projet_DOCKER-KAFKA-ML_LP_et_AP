import os
import json
import time
import threading
from datetime import datetime
import requests
from flask import Flask, jsonify, request
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)

CLIENTS_SERVICE_URL = os.getenv("CLIENTS_SERVICE_URL", "http://clients:8080")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_COMMANDES = "commandes-creees"
TOPIC_DEMANDES_PRED = "demandes-prediction"
TOPIC_RESULTATS_PRED = "resultats-prediction"

# Initialisation du producteur Kafka
producer = None
def get_producer():
    global producer
    if producer is None:
        for _ in range(15):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("[Commandes] Connecté au broker Kafka.")
                break
            except Exception as e:
                print(f"[Commandes] En attente de Kafka ({e})...")
                time.sleep(2)
    return producer

# Données existantes TP1
commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

# Endpoints TP1 & TP2 conservés 
@app.route('/commandes', methods=['GET'])
def get_commandes():
    return jsonify(commandes), 200

@app.route('/commandes/<int:commande_id>', methods=['GET'])
def get_commande(commande_id):
    commande = next((c for c in commandes if c["id"] == commande_id), None)
    if commande is None:
        return jsonify({"erreur": "Commande introuvable"}), 404

    try:
        url = f"{CLIENTS_SERVICE_URL}/clients/{commande['client_id']}"
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            client_data = response.json()
        elif response.status_code == 404:
            client_data = {"erreur": "Client associé inexistant"}
        else:
            client_data = {"erreur": f"Erreur service client (Code {response.status_code})"}
    except requests.exceptions.RequestException:
        return jsonify({
            "erreur": "Le microservice clients est actuellement indisponible",
            "commande": commande
        }), 503

    return jsonify({
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": client_data
    }), 200

@app.route('/commandes', methods=['POST'])
def create_commande():
    data = request.get_json()
    if not data or not all(k in data for k in ("client_id", "produit", "montant")):
        return jsonify({"erreur": "Champs requis : client_id, produit, montant"}), 400

    new_id = len(commandes) + 1
    mois_actuel = data.get("mois", datetime.utcnow().strftime("%Y-%m"))

    nouvelle_commande = {
        "id": new_id,
        "client_id": int(data["client_id"]),
        "produit": data["produit"],
        "montant": float(data["montant"])
    }
    commandes.append(nouvelle_commande)

    evenement = {
        "commande_id": new_id,
        "client_id": nouvelle_commande["client_id"],
        "produit": nouvelle_commande["produit"],
        "montant": nouvelle_commande["montant"],
        "mois": mois_actuel
    }

    prod = get_producer()
    if prod:
        prod.send(TOPIC_COMMANDES, value=evenement)
        prod.flush()
        print(f"[Commandes] Événement publié sur '{TOPIC_COMMANDES}': {evenement}")

    return jsonify(nouvelle_commande), 201

#  TP3 : Prédiction
@app.route('/predictions/demande', methods=['POST'])
def demander_prediction():
    data = request.get_json()
    champs_requis = [
        "client_id", "age", "anciennete", "nombre_commandes",
        "montant_total", "montant_moyen", "jours_depuis_derniere_commande"
    ]
    if not data or not all(k in data for k in champs_requis):
        return jsonify({"erreur": f"Champs requis: {champs_requis}"}), 400

    prod = get_producer()
    if prod:
        prod.send(TOPIC_DEMANDES_PRED, value=data)
        prod.flush()
        print(f"[Commandes] Demande de prédiction envoyée sur '{TOPIC_DEMANDES_PRED}' pour client {data['client_id']}")
        return jsonify({"message": "Demande de prédiction envoyée à Kafka", "data": data}), 202
    return jsonify({"erreur": "Kafka indisponible"}), 503

def ecouter_resultats_predictions():
    consumer = None
    for _ in range(15):
        try:
            consumer = KafkaConsumer(
                TOPIC_RESULTATS_PRED,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="commandes-prediction-results-group",
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[Commandes] En écoute des résultats sur '{TOPIC_RESULTATS_PRED}'...")
            break
        except Exception as e:
            print(f"[Commandes] En attente de Kafka ({e})...")
            time.sleep(2)

    if not consumer:
        return

    for message in consumer:
        res = message.value
        print("**************************************************")
        print(f"[Commandes - RETOUR PRÉDICTION REÇU]")
        print(f"Client ID: {res.get('client_id')}")
        print(f"Prédiction de commande: {'OUI' if res.get('prediction') == 1 else 'NON'}")
        print(f"Probabilité estimée: {res.get('probabilite_commande') * 100:.1f}%")
        print("**************************************************", flush=True)

if __name__ == '__main__':
    thread_pred = threading.Thread(target=ecouter_resultats_predictions, daemon=True)
    thread_pred.start()
    app.run(host='0.0.0.0', port=8080)