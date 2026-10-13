import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier

# Données d'exemple pour entraîner le modèle initial
# Features: [age, anciennete, nombre_commandes, montant_total, montant_moyen, jours_depuis_derniere_commande]
X_train = np.array([
    [22, 1, 2, 100.0, 50.0, 45],
    [32, 3, 8, 1250.0, 156.25, 12],
    [45, 5, 20, 4500.0, 225.0, 5],
    [19, 0, 1, 30.0, 30.0, 90],
    [28, 2, 6, 750.0, 125.0, 15],
    [60, 4, 12, 1800.0, 150.0, 60]
])
# Target: 1 (va commander), 0 (ne va pas commander)
y_train = np.array([0, 1, 1, 0, 1, 0])

model = RandomForestClassifier(n_estimators=10, random_state=42)
model.fit(X_train, y_train)

# Sauvegarde du modèle
joblib.dump(model, "modele_commande.pkl")
print("Modèle entraîné et sauvegardé dans 'modele_commande.pkl'.")