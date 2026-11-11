"""
test_broker.py  -  Validation Phase 0
---------------------------------------
Verifie que le broker MQTT local (Mosquitto) fonctionne.
A utiliser si les commandes mosquitto_pub / mosquitto_sub
ne sont pas dans ton PATH.

Lancer :  python test_broker.py
Attendu :  "OK : broker operationnel" doit s'afficher.
"""

import time
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC = "test/hello"
MESSAGE = "ca marche"

recu = {"ok": False}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connecte au broker.")
        client.subscribe(TOPIC)
        client.publish(TOPIC, MESSAGE)
    else:
        print(f"Echec de connexion, code = {rc}")


def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"Message recu sur '{msg.topic}' : {payload}")
    if payload == MESSAGE:
        recu["ok"] = True


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(BROKER, PORT, 60)
except Exception as e:
    print(f"Impossible de joindre le broker ({BROKER}:{PORT}).")
    print(f"   -> Mosquitto est-il bien lance ?  Detail : {e}")
    raise SystemExit(1)

client.loop_start()
time.sleep(2)
client.loop_stop()
client.disconnect()

if recu["ok"]:
    print("\nOK : broker operationnel. Phase 0 validee.")
else:
    print("\nKO : pas de message recu. Verifie que Mosquitto tourne.")
