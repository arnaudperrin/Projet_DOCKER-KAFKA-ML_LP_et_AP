# Architecture de Microservices E-Commerce (Python, Flask & Apache Kafka)

Une architecture de microservices pilotée par les événements, construite avec **Python (Flask)**, **Apache Kafka (mode KRaft)** et **Docker**. Ce projet simule un écosystème e-commerce où les services communiquent de manière asynchrone via des flux d'événements, avec en prime des analyses prédictives propulsées par le Machine Learning.

## 🏗️ Architecture & Services

Le système se compose de **4 microservices principaux** et d'un courtier de messages :

1. **`kafka` (Apache Kafka en mode KRaft)** : Le bus d'événements central qui sert de système nerveux. Il gère la publication et la consommation de messages asynchrones sans nécessiter ZooKeeper.

2. **`clients` (Port `8080`)**: Gère les profils clients et suit leur état. Il consomme les événements Kafka pour mettre à jour les compteurs de commandes en temps réel.

3. **`commandes` (Port `8081`)**: Gère la création des commandes. Lorsqu'une commande est passée via une requête HTTP POST, elle enregistre les données et publie un événement `commandes-creees` dans Kafka.

4. **`statistiques` (Port `8082`)**: Un consommateur de flux en temps réel. Il écoute les événements de commande, agrège les métriques (fréquence des produits, chiffre d'affaires mensuel) et expose des points de terminaison analytiques.

5. **`prediction`**: Un service d'inférence de Machine Learning utilisant un modèle de **Random Forest** entraîné (`joblib`) pour prédire le comportement ou les métriques d'achat des clients à partir de données tabulaires.

## 🛡️ Résilience et Tolérance aux Pannes

L'utilisation d'une architecture orientée événements via **Apache Kafka** confère au système des propriétés clés en matière de résilience :

* **Découplage temporel et spatial** : Les microservices producteurs (comme `commandes`) et consommateurs (comme `statistiques` ou `clients`) ne se connaissent pas directement. Si un service consommateur est momentanément indisponible, le service producteur n'est pas bloqué et continue de fonctionner.
* **Persistance des messages (Tampon de stockage)** : Kafka conserve les messages sur le disque selon sa configuration de rétention. Si le service `statistiques` subit un arrêt ou un redémarrage, aucun événement de commande n'est perdu : dès qu'il se relance, il reprend la lecture là où il s'était arrêté grâce à la gestion des *offsets* et aux groupes de consommateurs (`group_id`).
* **Isolation des pannes (Pas de cascade d'erreurs)** : Contrairement à un appel synchrone en cascade (HTTP direct) où la panne d'un service en aval entraîne l'échec global de la chaîne, l'asynchronisme de Kafka isole chaque composant.
* **Auto-cicatrisation (Self-healing)** : Grâce aux politiques de redémarrage de Docker Compose, si un conteneur crash subitement (par exemple suite à une exception temporaire de connexion), Docker se charge de le redémarrer automatiquement.

## 🛠️ Prérequis

Assurez-vous d'avoir les éléments suivants installés sur votre machine :

* [Docker](https://www.docker.com/) & Docker Compose
* Git

## 🚀 Guide de Démarrage Rapide

### 1. Cloner le dépôt & se placer à la racine du projet
```bash
cd tp-microservices
```

### 2. Construire et lancer tous les services avec Docker Compose
Exécutez la commande suivante pour fabriquer les images personnalisées des microservices et lancer l'ensemble du cluster en arrière-plan :
```bash
docker compose up --build -d
```

### 3. Vérifier que les conteneurs fonctionnent
```bash
docker compose ps
```

## 🧪 Tester l'API & le Flux d'Événements

### 1. Consulter le profil d'un client
```bash
curl http://localhost:8080/clients/1
```

### 2. Créer une commande (Déclenche un événement Kafka)
```bash
curl -X POST http://localhost:8081/commandes \
  -H "Content-Type: application/json" \
  -d '{"client_id": 1, "produit": "Clavier", "montant": 45.0}'
```

### 3. Consulter les statistiques en temps réel via le consommateur Kafka
* **Statistiques par produit :**
  ```bash
  curl http://localhost:8082/stats/produits
  ```
* **Statistiques du chiffre d'affaires mensuel :**
  ```bash
  curl http://localhost:8082/stats/ca
  ```

### 4. Lancer une prédiction de Machine Learning
```bash
curl -X POST http://localhost:8081/predictions/demande \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": 1,
    "age": 30,
    "anciennete": 2,
    "nombre_commandes": 5,
    "montant_total": 500.0,
    "montant_moyen": 100.0,
    "jours_depuis_derniere_commande": 10
  }'
```

## ⚙️ Configuration & Variables d'Environnement

Kafka est configuré en **mode KRaft** avec le mappage d'écouteurs suivant :
* **Réseau interne Docker** : `kafka:9092` (utilisé par les microservices pour publier et consommer les messages).
* **Quorum du contrôleur** : Port `29093`.

Chaque microservice récupère la variable d'environnement `KAFKA_BOOTSTRAP_SERVERS` définie dans le fichier `docker-compose.yml` pour se connecter automatiquement au courtier de messages.

## 🧰 Dépannage

* **Port déjà utilisé (`Address already in use`)** :
  Si les ports `8080`, `8081`, `8082` ou `9092` sont bloqués par des processus résiduels, libérez-les avec :
  ```bash
  sudo fuser -k 8080/tcp 8081/tcp 8082/tcp 9092/tcp
  ```

* **Réinitialisation complète de l'environnement** :
  Pour supprimer définitivement les conteneurs et nettoyer les volumes (y compris les journaux de métadonnées Kafka KRaft), exécutez :
  ```bash
  docker compose down -v
  ```