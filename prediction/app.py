import os
import json
import time
import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_DEMANDES = os.getenv("TOPIC_DEMANDES", "demandes-prediction")
TOPIC_RESULTATS = os.getenv("TOPIC_RESULTATS", "resultats-prediction")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "prediction-group")

# 1. Chargement du modèle de Machine Learning
print("[Prediction] Chargement du modèle...")
model = joblib.load("modele_commande.pkl")
print("[Prediction] Modèle chargé avec succès.")

# 2. Initialisation du Producer Kafka pour publier les résultats
producer = None
for _ in range(15):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[Prediction] Connecté au broker Kafka (Producer).")
        break
    except Exception as e:
        print(f"[Prediction] En attente de Kafka (Producer) ({e})...")
        time.sleep(2)

# 3. Initialisation du Consumer Kafka pour écouter les demandes
consumer = None
for _ in range(15):
    try:
        consumer = KafkaConsumer(
            TOPIC_DEMANDES,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"[Prediction] Consommateur Kafka connecté au topic '{TOPIC_DEMANDES}' (Groupe: {GROUP_ID}).")
        break
    except Exception as e:
        print(f"[Prediction] En attente de Kafka (Consumer) ({e})...")
        time.sleep(2)

if not consumer or not producer:
    print("[Prediction] Erreur critique lors de la connexion à Kafka.")
    exit(1)

# 4. Boucle de prédiction événementielle
for message in consumer:
    data = message.value
    print(f"\n[Prediction EVENT REÇU] Données brutes : {data}", flush=True)

    # Construction du DataFrame d'entrée
    X = pd.DataFrame([{
        "age": data["age"],
        "anciennete": data["anciennete"],
        "nombre_commandes": data["nombre_commandes"],
        "montant_total": data["montant_total"],
        "montant_moyen": data["montant_moyen"],
        "jours_depuis_derniere_commande": data["jours_depuis_derniere_commande"]
    }])

    # Calcul de la prédiction et de la probabilité
    prediction = int(model.predict(X)[0])
    probabilite = float(model.predict_proba(X)[0][1])

    print(f"Client: {data['client_id']}")
    print(f"Prediction: {prediction}")
    print(f"Probabilite: {probabilite:.2f}")

    # Construction de l'événement résultat
    resultat = {
        "client_id": data["client_id"],
        "prediction": prediction,
        "probabilite_commande": round(probabilite, 4)
    }

    # Publication du résultat pour le microservice commandes
    producer.send(TOPIC_RESULTATS, value=resultat)
    producer.flush()
    print(f"[Prediction] Résultat publié sur '{TOPIC_RESULTATS}' : {resultat}", flush=True)