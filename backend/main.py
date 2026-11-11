"""
main.py  -  Application Host Sparkplug B (Primary Application) - Phase 3
=======================================================================
Role :
  - s'abonne a tout le groupe d'usine sur le broker MQTT,
  - DECODE les payloads binaires Protobuf (Sparkplug B),
  - reconstitue l'etat complet de chaque ligne (le DDATA est en RBE :
    on fusionne les variations sur le dernier etat connu),
  - gere l'etat de connexion (NBIRTH/NDEATH/DBIRTH/DDEATH),
  - calcule les indicateurs : Taux de Qualite, Cadence, Disponibilite, TRS,
  - expose le tout en temps reel via WebSocket (+ une route REST),
  - envoie une commande d'ARRET D'URGENCE (DCMD) a une ligne.

Lancer (depuis le dossier backend/) :
    uvicorn main:app --port 8000
"""

import time
import json
import threading
import asyncio
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from tahutils.tahu import sparkplug_b as sparkplug
from tahutils.tahu.sparkplug_b import MetricDataType as DT

# ========================= CONFIGURATION =========================
BROKER = "localhost"
PORT = 1883
GROUP = "Groupe_Usine"
NODE = "Atelier_1"

# ===================== ETAT PARTAGE (thread MQTT <-> FastAPI) =====================
verrou = threading.Lock()
noeud = {"en_ligne": False}
lignes = {}            # nom_ligne -> dict d'etat
mqttc = None           # client MQTT (initialise au demarrage)


# Quantite nominale d'un Ordre de Fabrication (sert a calculer l'avancement).
# Alignee sur le rythme de l'emulateur (changement d'OF ~ toutes les 40 pieces).
OBJECTIF_OF = 40


def etat_vide():
    return {"online": False, "statut": "?", "pieces": 0, "rebuts": 0,
            "of": "-", "t_birth": None, "t_maj": None, "t_run": 0.0,
            "pieces_debut_of": 0, "pieces_birth": 0}


# ===================== DECODAGE D'UNE METRIQUE PROTOBUF =====================
def lire_valeur(m):
    dt = m.datatype
    if dt in (DT.Int64, DT.UInt64):
        return m.long_value
    if dt in (DT.Int8, DT.Int16, DT.Int32, DT.UInt8, DT.UInt16, DT.UInt32):
        return m.int_value
    if dt == DT.Boolean:
        return m.boolean_value
    if dt in (DT.String, DT.Text, DT.UUID):
        return m.string_value
    if dt == DT.Float:
        return m.float_value
    if dt == DT.Double:
        return m.double_value
    return None


def accumuler_dispo(e):
    """Avant de changer de statut, on compte le temps ecoule sous l'ancien statut."""
    now = time.time()
    if e["t_maj"] is not None and e["statut"] == "RUNNING":
        e["t_run"] += now - e["t_maj"]
    e["t_maj"] = now


def appliquer_metriques(e, payload):
    maj = {m.name: lire_valeur(m) for m in payload.metrics}
    if "Statut machine" in maj:
        e["statut"] = maj["Statut machine"]
    if "Pieces totales" in maj:
        e["pieces"] = maj["Pieces totales"]
    if "Rebuts" in maj:
        e["rebuts"] = maj["Rebuts"]
    if "OF en cours" in maj:
        # Nouvel OF : on memorise le compteur de pieces a cet instant
        if maj["OF en cours"] != e["of"]:
            e["pieces_debut_of"] = e["pieces"]
        e["of"] = maj["OF en cours"]


# ===================== CALLBACKS MQTT =====================
def on_connect(client, userdata, flags, rc):
    sujet = f"spBv1.0/{GROUP}/#"
    client.subscribe(sujet)
    # Demande de RENAISSANCE : le Host reclame a l'EoN de renvoyer ses
    # NBIRTH/DBIRTH. Ainsi, meme si l'emulateur a demarre AVANT le backend,
    # on recupere l'etat complet (les naissances ne sont pas retenues).
    reb = sparkplug.Payload()
    reb.timestamp = int(time.time() * 1000)
    sparkplug.addMetric(reb, "Node Control/Rebirth", None, DT.Boolean, True)
    client.publish(f"spBv1.0/{GROUP}/NCMD/{NODE}", reb.SerializeToString(),
                   qos=0, retain=False)
    print(f"Host connecte. Abonne a : {sujet} (+ demande de rebirth)")


def on_message(client, userdata, msg):
    parts = msg.topic.split("/")     # spBv1.0 / GROUP / TYPE / NODE / [DEVICE]
    if len(parts) < 4:
        return
    mtype = parts[2]
    device = parts[4] if len(parts) > 4 else None

    payload = sparkplug.Payload()
    payload.ParseFromString(msg.payload)

    with verrou:
        if mtype == "NBIRTH":
            noeud["en_ligne"] = True
            print(">>> NBIRTH : noeud en ligne")

        elif mtype == "NDEATH":
            noeud["en_ligne"] = False
            for e in lignes.values():
                e["online"] = False
            print(">>> NDEATH : noeud HORS LIGNE (testament recu)")

        elif mtype == "DBIRTH":
            e = lignes.setdefault(device, etat_vide())
            now = time.time()
            e.update({"online": True, "t_birth": now, "t_maj": now, "t_run": 0.0})
            appliquer_metriques(e, payload)
            # On memorise le compteur a l'instant ou on commence a observer :
            # la cadence se mesure sur les pieces produites DEPUIS cet instant.
            e["pieces_birth"] = e["pieces"]
            print(f">>> DBIRTH : {device} en ligne")

        elif mtype == "DDATA":
            e = lignes.setdefault(device, etat_vide())
            accumuler_dispo(e)              # comptabilise le temps avant le changement
            appliquer_metriques(e, payload)  # fusionne les variations (RBE)

        elif mtype == "DDEATH":
            if device in lignes:
                lignes[device]["online"] = False


# ===================== CALCUL DES INDICATEURS (TRS) =====================
def vue_ligne(nom, e):
    now = time.time()
    pieces, rebuts = e["pieces"], e["rebuts"]

    # Taux de Qualite = pieces bonnes / pieces totales
    qualite = round((pieces - rebuts) / pieces * 100, 1) if pieces > 0 else 100.0

    # Cadence moyenne = pieces produites DEPUIS l'observation / temps ecoule
    duree_min = (now - e["t_birth"]) / 60 if e["t_birth"] else 0
    pieces_observees = max(0, pieces - e["pieces_birth"])
    cadence = round(pieces_observees / duree_min, 1) if duree_min > 0 else 0.0

    # Disponibilite = temps RUNNING / temps total (inclut la periode en cours)
    t_run = e["t_run"]
    if e["t_maj"] and e["statut"] == "RUNNING":
        t_run += now - e["t_maj"]
    duree_tot = (now - e["t_birth"]) if e["t_birth"] else 0
    dispo = round(t_run / duree_tot * 100, 1) if duree_tot > 0 else 100.0

    # TRS simplifie = Disponibilite x Qualite
    trs = round(dispo / 100 * qualite, 1)

    # Avancement de l'OF en cours
    pieces_of = max(0, pieces - e["pieces_debut_of"])
    avancement = min(100, round(pieces_of / OBJECTIF_OF * 100)) if OBJECTIF_OF else 0

    # Feu pour l'IHM
    if not e["online"]:
        feu = "gris"
    elif e["statut"] == "RUNNING":
        feu = "vert"
    elif e["statut"] == "FAULT":
        feu = "rouge"
    else:                      # STOPPED / inconnu
        feu = "orange"

    return {"ligne": nom, "online": e["online"], "statut": e["statut"],
            "pieces": pieces, "rebuts": rebuts, "of": e["of"],
            "qualite": qualite, "cadence": cadence, "dispo": dispo,
            "trs": trs, "feu": feu,
            "of_pieces": pieces_of, "of_objectif": OBJECTIF_OF,
            "avancement": avancement}


def snapshot():
    with verrou:
        return {"noeud": noeud["en_ligne"],
                "lignes": [vue_ligne(n, e) for n, e in lignes.items()]}


# ===================== APPLICATION FASTAPI =====================
clients = set()  # WebSockets connectes


async def diffuseur():
    """Pousse l'etat a tous les clients WebSocket toutes les 0.5 s (temps reel)."""
    while True:
        await asyncio.sleep(0.5)
        data = json.dumps(snapshot())
        mortes = set()
        for ws in clients:
            try:
                await ws.send_text(data)
            except Exception:
                mortes.add(ws)
        clients.difference_update(mortes)


@asynccontextmanager
async def lifespan(app):
    global mqttc
    mqttc = mqtt.Client(client_id="Host-Primary")
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.connect(BROKER, PORT, keepalive=30)
    mqttc.loop_start()
    tache = asyncio.create_task(diffuseur())
    yield
    tache.cancel()
    mqttc.loop_stop()
    mqttc.disconnect()


app = FastAPI(title="Host Sparkplug B", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


@app.get("/api/etat")
def api_etat():
    """Etat courant (REST) - pratique pour un chargement initial ou un test."""
    return snapshot()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_text(json.dumps(snapshot()))
        while True:
            await ws.receive_text()   # maintient la connexion ouverte
    except WebSocketDisconnect:
        clients.discard(ws)


@app.post("/api/arret/{ligne}")
def arret_urgence(ligne: str):
    """Envoie une commande DCMD d'arret d'urgence a une ligne."""
    payload = sparkplug.Payload()
    payload.timestamp = int(time.time() * 1000)
    sparkplug.addMetric(payload, "Arret Urgence", None, DT.Boolean, True)
    topic = f"spBv1.0/{GROUP}/DCMD/{NODE}/{ligne}"
    mqttc.publish(topic, payload.SerializeToString(), qos=0, retain=False)
    print(f"<<< DCMD Arret d'urgence envoye a {ligne}")
    return {"ok": True, "ligne": ligne, "action": "arret_urgence"}
