# Projet IIoT — Supervision de Production Standardisée (MQTT + Sparkplug B)

Mini-Projet 2 (Avancé) — émulation d'un atelier Usine 4.0 :
un **Nœud EoN** (Edge of Network) publie l'état de plusieurs lignes de
production en **Sparkplug B** (encodé Protobuf) via MQTT ; une application
**Host** décode le flux, gère l'état de connexion et calcule le **TRS** ;
une **IHM web** affiche le statut en direct et permet un arrêt d'urgence.

Projet jumelé au chapitre théorique **« La 5G pour l'IIoT »**.

---

## Arborescence

```
projet-iiot-5g/
├── emulateur/     # Node EoN Python (Phase 2)
├── backend/       # Application Host FastAPI (Phase 3)
├── frontend/      # IHM React (Phase 4)
├── docs/          # Rapport 10 p. + slides + chronogramme (Phase 6)
├── requirements.txt
└── test_broker.py
```

## Feuille de route

| Phase | Objet | Dossier |
|-------|-------|---------|
| 0 | Environnement + broker MQTT | — |
| 1 | Comprendre Sparkplug B | — |
| 2 | Node EoN (émulateur) | `emulateur/` |
| 3 | Backend Host (décodage + TRS) | `backend/` |
| 4 | Frontend IHM (React) | `frontend/` |
| 5 | Scénario de test NDEATH | — |
| 6 | Chapitre 5G IIoT + slides | `docs/` |

## Installation (Phase 0)

```bash
python -m venv venv
# Windows :
venv\Scripts\activate
# Linux/Mac :
source venv/bin/activate

pip install -r requirements.txt
```

## Lancer le broker et tester

Démarrer Mosquitto (service, ou `mosquitto -v` en console), puis :

```bash
# Option 1 — outils Mosquitto
mosquitto_sub -h localhost -t test/hello      # terminal 1
mosquitto_pub -h localhost -t test/hello -m "ca marche"   # terminal 2

# Option 2 — script Python de secours
python test_broker.py
```

Si le message s'affiche → **Phase 0 validée.**

## Namespace Sparkplug B utilisé

```
spBv1.0/Groupe_Usine/[MessageType]/[Node_ID]/[Device_ID]
```

- **Node_ID** : `Atelier_1`
- **Device_ID** : `Ligne_Assemblage`, `Ligne_Conditionnement`
- **Variables par ligne** : Statut machine, Compteur pièces totales,
  Compteur rebuts, ID OF en cours
