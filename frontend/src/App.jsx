import { useEffect, useState, useRef } from "react";

const WS_URL = "ws://localhost:8000/ws";
const API_URL = "http://localhost:8000";

const COULEURS = {
  vert: "#22c55e",
  orange: "#f59e0b",
  rouge: "#ef4444",
  gris: "#6b7280",
};

const LIBELLE_STATUT = {
  RUNNING: "En marche",
  FAULT: "Panne",
  STOPPED: "Arrêté (urgence)",
  "?": "Inconnu",
};

export default function App() {
  const [data, setData] = useState({ noeud: false, lignes: [] });
  const [connecte, setConnecte] = useState(false);
  const wsRef = useRef(null);

  // Connexion WebSocket temps reel (avec reconnexion automatique)
  useEffect(() => {
    let ferme = false;
    function connecter() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;
      ws.onopen = () => setConnecte(true);
      ws.onmessage = (e) => setData(JSON.parse(e.data));
      ws.onclose = () => {
        setConnecte(false);
        if (!ferme) setTimeout(connecter, 1500);
      };
      ws.onerror = () => ws.close();
    }
    connecter();
    return () => {
      ferme = true;
      wsRef.current && wsRef.current.close();
    };
  }, []);

  async function arretUrgence(ligne) {
    if (!window.confirm(`Confirmer l'ARRÊT D'URGENCE de ${ligne} ?`)) return;
    try {
      await fetch(`${API_URL}/api/arret/${ligne}`, { method: "POST" });
    } catch {
      alert("Backend injoignable (port 8000).");
    }
  }

  return (
    <div className="app">
      <header className="entete">
        <div>
          <h1>IHM Usine — Supervision Sparkplug B</h1>
          <p className="sous-titre">Atelier_1 · MQTT + Sparkplug B (Protobuf)</p>
        </div>
        <div className="etats">
          <Badge ok={data.noeud} ok_txt="Nœud en ligne" ko_txt="Nœud hors ligne" />
          <Badge ok={connecte} ok_txt="Temps réel" ko_txt="Déconnecté" />
        </div>
      </header>

      <main className="grille">
        {data.lignes.length === 0 && (
          <p className="vide">En attente des données de l'atelier…</p>
        )}
        {data.lignes.map((l) => (
          <CarteLigne key={l.ligne} l={l} onArret={() => arretUrgence(l.ligne)} />
        ))}
      </main>
    </div>
  );
}

function Badge({ ok, ok_txt, ko_txt }) {
  return (
    <span className={`badge ${ok ? "badge-ok" : "badge-ko"}`}>
      <span className="point" /> {ok ? ok_txt : ko_txt}
    </span>
  );
}

function CarteLigne({ l, onArret }) {
  const couleur = COULEURS[l.feu] || COULEURS.gris;
  const statut = LIBELLE_STATUT[l.statut] || l.statut;
  return (
    <section className="carte" style={{ borderTopColor: couleur }}>
      <div className="carte-haut">
        <span
          className="feu"
          style={{ background: couleur, boxShadow: `0 0 18px ${couleur}` }}
        />
        <div>
          <h2>{l.ligne.replace(/_/g, " ")}</h2>
          <span className="statut" style={{ color: couleur }}>
            {statut}
          </span>
        </div>
      </div>

      <div className="of">
        <div className="of-ligne">
          <span>
            OF : <b>{l.of}</b>
          </span>
          <span className="of-compte">
            {l.of_pieces}/{l.of_objectif}
          </span>
        </div>
        <div className="barre">
          <div
            className="barre-fill"
            style={{ width: `${l.avancement}%`, background: couleur }}
          />
        </div>
      </div>

      <div className="kpis">
        <Kpi label="Pièces" val={l.pieces} />
        <Kpi label="Rebuts" val={l.rebuts} />
        <Kpi label="Qualité" val={`${l.qualite} %`} />
        <Kpi label="Cadence" val={`${l.cadence}/min`} />
        <Kpi label="Dispo." val={`${l.dispo} %`} />
        <Kpi label="TRS" val={`${l.trs} %`} fort />
      </div>

      <button className="btn-arret" onClick={onArret} disabled={!l.online}>
        ⏻ ARRÊT D'URGENCE
      </button>
    </section>
  );
}

function Kpi({ label, val, fort }) {
  return (
    <div className={`kpi ${fort ? "kpi-fort" : ""}`}>
      <span className="kpi-val">{val}</span>
      <span className="kpi-label">{label}</span>
    </div>
  );
}
