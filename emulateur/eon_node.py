"""
eon_node.py  -  Noeud EoN (Edge of Network) - Mini-Projet 2
============================================================
Emule un atelier Usine 4.0 : plusieurs lignes de production dont on
publie l'etat en Sparkplug B (encode Protobuf) par-dessus MQTT.

Cycle de vie respecte :
  NBIRTH / DBIRTH  a l'initialisation  (certificats de naissance)
  DDATA            en cours de vie, en Report-by-Exception (RBE)
  NDEATH           via le TESTAMENT MQTT (Will) si le noeud tombe

Namespace impose :
  spBv1.0/Groupe_Usine/[MessageType]/[Node_ID]/[Device_ID]

Lancer :  python eon_node.py
Arreter :  Ctrl+C  -> declenche le testament NDEATH (scenario de test)
"""

import time
import random
import sys

import paho.mqtt.client as mqtt
from tahutils.tahu import sparkplug_b as sparkplug
from tahutils.tahu.sparkplug_b import MetricDataType as DT

# ========================= CONFIGURATION =========================
BROKER = "localhost"
PORT = 1883

GROUP = "Groupe_Usine"          # Groupe d'usine
NODE = "Atelier_1"              # Node_ID : notre noeud Edge
LIGNES = ["Ligne_Assemblage", "Ligne_Conditionnement"]  # Device_ID
PERIODE = 1.0                  # secondes entre deux cycles de simulation


# ===================== OUTIL : construire un topic =====================
def topic(message_type, device=None):
    """spBv1.0/Groupe_Usine/[TYPE]/[NODE]/[DEVICE]"""
    t = f"spBv1.0/{GROUP}/{message_type}/{NODE}"
    if device:
        t += f"/{device}"
    return t


# ===================== ETAT D'UNE LIGNE DE PRODUCTION =====================
class Ligne:
    """Simule une ligne : produit des pieces, des rebuts, change d'OF, peut tomber en panne."""

    def __init__(self, nom):
        self.nom = nom
        self.statut = "RUNNING"            # RUNNING / FAULT / STOPPED
        self.pieces = 0                    # compteur pieces totales
        self.rebuts = 0                    # compteur rebuts
        self.of = self._nouvel_of()        # ID de l'ordre de fabrication
        self._fault_restant = 0            # nb de cycles restants en panne
        self._dernier = {}                 # memoire pour le Report-by-Exception

    def _nouvel_of(self):
        return f"OF-{random.randint(10000, 99999)}"

    def cycle(self):
        """Fait avancer la simulation d'un pas et renvoie les 4 variables."""
        if self.statut == "STOPPED":
            pass  # arret d'urgence : la ligne est figee

        elif self.statut == "FAULT":
            self._fault_restant -= 1
            if self._fault_restant <= 0:
                self.statut = "RUNNING"    # la panne se resorbe

        else:  # RUNNING
            prod = random.randint(1, 3)
            self.pieces += prod
            if random.random() < 0.08:     # ~8% : une piece part en rebut
                self.rebuts += 1
            if self.pieces % 40 < prod:    # ~tous les 40 pieces : nouvel OF
                self.of = self._nouvel_of()
            if random.random() < 0.03:     # ~3% : panne machine
                self.statut = "FAULT"
                self._fault_restant = random.randint(3, 6)

        return {
            "Statut machine": self.statut,
            "Pieces totales": self.pieces,
            "Rebuts": self.rebuts,
            "OF en cours": self.of,
        }

    def changements(self, valeurs):
        """Report-by-Exception : ne renvoie que les variables dont la valeur a change."""
        diff = {k: v for k, v in valeurs.items() if self._dernier.get(k) != v}
        self._dernier.update(valeurs)
        return diff


# Instancie les lignes
lignes = {nom: Ligne(nom) for nom in LIGNES}


# ===================== AJOUT D'UNE METRIQUE (bon type) =====================
def ajouter(payload, nom, valeur):
    type_sp = DT.String if isinstance(valeur, str) else DT.Int64
    sparkplug.addMetric(payload, nom, None, type_sp, valeur)


# ===================== PUBLIER LES NAISSANCES (NBIRTH + DBIRTH) =====================
def publier_naissances(client):
    # --- NBIRTH : le noeud se presente ---
    nbirth = sparkplug.getNodeBirthPayload()
    # Metrique standard : permet au Host de demander une "renaissance"
    sparkplug.addMetric(nbirth, "Node Control/Rebirth", None, DT.Boolean, False)
    client.publish(topic("NBIRTH"), nbirth.SerializeToString(), qos=0, retain=False)

    # --- DBIRTH : chaque ligne declare ses 4 variables ---
    for nom, ligne in lignes.items():
        dbirth = sparkplug.getDeviceBirthPayload()
        valeurs = {
            "Statut machine": ligne.statut,
            "Pieces totales": ligne.pieces,
            "Rebuts": ligne.rebuts,
            "OF en cours": ligne.of,
        }
        for k, v in valeurs.items():
            ajouter(dbirth, k, v)
        ligne._dernier = dict(valeurs)  # initialise la reference RBE
        client.publish(topic("DBIRTH", nom), dbirth.SerializeToString(), qos=0, retain=False)

    print(">>> Naissances publiees : NBIRTH + DBIRTH x", len(lignes))


# ===================== CALLBACKS MQTT =====================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connecte au broker. Noeud EoN '{NODE}' en ligne.")
        # Ecoute des commandes entrantes (Host -> Noeud)
        client.subscribe(topic("NCMD"))         # ex: demande de rebirth
        client.subscribe(topic("DCMD") + "/+")  # ex: arret d'urgence d'une ligne
        publier_naissances(client)
    else:
        print("Echec de connexion, rc =", rc)


def on_message(client, userdata, msg):
    payload = sparkplug.Payload()
    payload.ParseFromString(msg.payload)
    parts = msg.topic.split("/")      # spBv1.0 / GROUP / TYPE / NODE / [DEVICE]
    message_type = parts[2]

    if message_type == "NCMD":
        for m in payload.metrics:
            if m.name == "Node Control/Rebirth" and m.boolean_value:
                print(">>> Rebirth demande par le Host.")
                publier_naissances(client)

    elif message_type == "DCMD":
        device = parts[4]
        for m in payload.metrics:
            if m.name == "Arret Urgence" and m.boolean_value:
                if device in lignes:
                    lignes[device].statut = "STOPPED"
                    print(f"!!! ARRET D'URGENCE recu pour {device}")


# ===================== BOUCLE DE SIMULATION =====================
def boucle(client):
    try:
        while True:
            time.sleep(PERIODE)
            for nom, ligne in lignes.items():
                valeurs = ligne.cycle()
                diff = ligne.changements(valeurs)   # Report-by-Exception
                if diff:
                    ddata = sparkplug.getDdataPayload()
                    for k, v in diff.items():
                        ajouter(ddata, k, v)
                    client.publish(topic("DDATA", nom), ddata.SerializeToString(),
                                   qos=0, retain=False)
                    print(f"DDATA {nom:22s} -> {diff}")
    except KeyboardInterrupt:
        # ARRET BRUTAL (Ctrl+C) : on NE fait PAS de client.disconnect().
        # Le broker va detecter la coupure et publier le TESTAMENT NDEATH.
        print("\nArret brutal (Ctrl+C) : le broker va publier NDEATH (testament).")


# ===================== PROGRAMME PRINCIPAL =====================
def main():
    # 1) Construire le testament NDEATH *avant* de se connecter.
    #    (il partage le meme bdSeq que le futur NBIRTH)
    death = sparkplug.getNodeDeathPayload()

    client = mqtt.Client(client_id=f"EoN-{NODE}")
    client.on_connect = on_connect
    client.on_message = on_message

    # 2) Enregistrer le testament : le broker l'emettra si on disparait.
    client.will_set(topic("NDEATH"), death.SerializeToString(), qos=1, retain=False)

    # 3) Se connecter et demarrer.
    client.connect(BROKER, PORT, keepalive=30)
    client.loop_start()
    boucle(client)


if __name__ == "__main__":
    main()
