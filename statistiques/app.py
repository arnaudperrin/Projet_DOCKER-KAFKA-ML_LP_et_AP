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
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "statistiques-group")

# Compteurs statistiques
commandes_par_produit = defaultdict(int)
chiffre_affaires_par_mois = defaultdict(float)

@app.route('/stats/produits', methods=['GET'])
def get_stats_produits():
    return jsonify(commandes_par_produit), 200

@app.route('/stats/ca', methods=['GET'])
def get_stats_ca():
    return jsonify(chiffre_affaires_par_mois), 200

def demarrer_consommateur_kafka():
    consumer = None
    for _ in range(15):
        try:
            consumer = KafkaConsumer(
                TOPIC_COMMANDES,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[Stats] Consommateur Kafka connecté au groupe '{GROUP_ID}'.")
            break
        except Exception as e:
            print(f"[Stats] En attente de Kafka ({e})...")
            time.sleep(2)

    if not consumer:
        return

    for message in consumer:
        event = message.value
        produit = event.get("produit")
        montant = float(event.get("montant", 0))
        mois = event.get("mois", "Inconnu")

        commandes_par_produit[produit] += 1
        chiffre_affaires_par_mois[mois] += montant

        print(f"[Stats EVENT] Produit: {produit} (Total: {commandes_par_produit[produit]}) | CA {mois}: {chiffre_affaires_par_mois[mois]}€", flush=True)

if __name__ == '__main__':
    thread = threading.Thread(target=demarrer_consommateur_kafka, daemon=True)
    thread.start()
    app.run(host='0.0.0.0', port=8080)