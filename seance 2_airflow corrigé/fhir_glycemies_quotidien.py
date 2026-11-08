"""TP séance 2 · Cas d'usage : le flux FHIR quotidien des glycémies.

extraire (API FHIR, pagination) → aplatir (Parquet) → valider (Pandera) → charger (DuckDB, idempotent)
Le guide pas à pas est dans TD.md (partie 04).

Avant de commencer :
  - copier ce fichier dans dags/ ;
  - créer le pool api_fhir : Admin › Pools › + , nom api_fhir, 3 places.
Sans réseau : déclencher avec le paramètre source = fichier (formulaire « Trigger »).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

DONNEES = Path(__file__).resolve().parents[1] / "donnees"
BASE = os.environ.get("FHIR_BASE", "https://hapi.fhir.org/baseR4")   # panne volontaire (TD.md, 4.4) : mettre une mauvaise URL
UNITES = ["g/L", "mg/dL", "mmol/L"]
EXEMPLE = DONNEES / "exemple" / "glycemies_exemple.ndjson"     # repli sans réseau (données de la séance 1)
glycemies = Asset("glycemies")


def jour(data_interval_start) -> str:
    """Le jour traité ; une exécution manuelle n'a pas d'intervalle : on prend la veille."""
    return (data_interval_start or datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def alerter(context):
    """Appelée quand une tâche échoue définitivement (après ses retries)."""
    ti = context["ti"]
    message = f"ALERTE {datetime.now():%H:%M:%S} · {ti.dag_id}.{ti.task_id} en échec : {context.get('exception')}"
    print(message)
    with open(DONNEES / "alertes.log", "a") as f:
        f.write(message + "\n")


@dag(schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/Paris", interval=timedelta(days=1)),
     start_date=datetime(2026, 11, 1), catchup=False, tags=["fhir", "tp"],
     default_args={"on_failure_callback": alerter},
     params={"source": Param("api", enum=["api", "fichier"],
                             description="api : serveur FHIR public ; fichier : données d'exemple, sans réseau")})
def fhir_glycemies_quotidien():

    # En production : retries=5, retry_delay=2 min ; raccourci ici pour voir la panne pendant le TP
    @task(pool="api_fhir", retries=3, retry_delay=timedelta(seconds=20))
    def extraire(params=None, data_interval_start=None) -> str:
        """Étape 1 : glycémies (LOINC 2345-7) modifiées depuis le début de l'intervalle, toutes pages."""
        import requests

        chemin = DONNEES / "brut" / f"glycemies_{jour(data_interval_start)}.ndjson"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        if params["source"] == "fichier":
            chemin.write_text(EXEMPLE.read_text())
            print("Source fichier :", EXEMPLE)
            return str(chemin)

        url = f"{BASE}/Observation"
        requete = {"code": "http://loinc.org|2345-7", "_count": 100,
                   "_lastUpdated": f"ge{jour(data_interval_start)}"}
        ressources = []
        # À COMPLÉTER (étape 1) : boucle de pagination de la séance 1, qui remplit `ressources`
        #   - r = requests.get(url, params=requete, headers={"Accept": "application/fhir+json"}, timeout=30)
        #   - r.raise_for_status() : une erreur HTTP fait échouer la tâche, donc déclenche un retry
        #   - ajouter les ressources de bundle["entry"], suivre le lien « next », puis requete = None
        raise NotImplementedError("Étape 1 à compléter (ou déclencher avec source = fichier)")
        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)                            # XCom : un chemin, pas les données



    @task
    def aplatir(chemin: str) -> str:
        """Une ligne par Observation, enregistrée en Parquet."""
        import pandas as pd

        lignes = []

        with open(chemin, "r", encoding="utf-8") as f:
            for ligne in f:
                observation = json.loads(ligne)

                patient = observation.get("subject", {}).get("reference")
                quantite = observation.get("valueQuantity") or {}

                lignes.append({
                    "id": observation.get("id"),
                    "patient": patient,
                    "date": observation.get("effectiveDateTime"),
                    "statut": observation.get("status"),
                    "valeur": quantite.get("value"),
                    "unite": quantite.get("unit")
                })

        df = pd.DataFrame(
            lignes,
            columns=["id", "patient", "date", "statut", "valeur", "unite"]
        )

        sortie = Path(chemin).with_suffix(".parquet")
        df.to_parquet(sortie, index=False)

        print("Nombre de lignes :", len(df))
        print("Unités :", df["unite"].value_counts(dropna=False).to_dict())

        return str(sortie)



    @task
    def valider(chemin: str) -> str:
        """Valider les glycémies et conserver les lignes valides."""
        import pandas as pd
        import pandera.pandas as pa

        df = pd.read_parquet(chemin)

        schema = pa.DataFrameSchema({
            "unite": pa.Column(
                str,
                checks=pa.Check.isin(UNITES),
                nullable=True
            ),
            "valeur": pa.Column(
                float,
                checks=pa.Check.gt(0),
                nullable=True,
                coerce=True
            ),
            "patient": pa.Column(
                str,
                nullable=True
            )
        })

        # Vérifier le schéma avec Pandera
        schema.validate(df, lazy=True)

        # Séparer les lignes valides des lignes rejetées
        masque = (
            df["unite"].isin(UNITES)
            & df["valeur"].gt(0)
            & df["patient"].notna()
            & df["patient"].ne("")
        )

        valides = df.loc[masque].copy()
        rejetees = df.loc[~masque].copy()

        sortie = Path(chemin).with_name(
            Path(chemin).stem + "_valides.parquet"
        )

        valides.to_parquet(sortie, index=False)

        print("Lignes valides :", len(valides))
        print("Lignes rejetées :", len(rejetees))

        return str(sortie)

    @task(outlets=[glycemies])
    def charger(chemin: str, data_interval_start=None):
        """Charger les glycémies dans DuckDB sans doublons."""
        import duckdb

        base = DONNEES / "entrepot.duckdb"
        base.parent.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect(str(base))

        con.execute("""
            CREATE TABLE IF NOT EXISTS glycemies (
                id VARCHAR,
                patient VARCHAR,
                date TIMESTAMP,
                statut VARCHAR,
                valeur DOUBLE,
                unite VARCHAR
            )
        """)

        con.execute("BEGIN TRANSACTION")
        try:
            con.execute("""
                DELETE FROM glycemies
                WHERE CAST(date AS DATE) IN (
                    SELECT DISTINCT CAST(date AS DATE)
                    FROM read_parquet(?)
                )
            """, [chemin])

            con.execute("""
                INSERT INTO glycemies
                SELECT
                    id,
                    patient,
                    TRY_CAST(date AS TIMESTAMP),
                    statut,
                    CASE
                        WHEN unite = 'g/L' THEN valeur * 100
                        WHEN unite = 'mmol/L' THEN valeur * 18.0182
                        ELSE valeur
                    END,
                    'mg/dL'
                FROM read_parquet(?)
            """, [chemin])

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        total = con.execute(
            "SELECT COUNT(*) FROM glycemies"
        ).fetchone()[0]

        print("Glycémies chargées :",
              con.execute(
                  "SELECT COUNT(*) FROM read_parquet(?)",
                  [chemin]
              ).fetchone()[0])
        print("Total dans DuckDB :", total)

        con.close()

    charger(valider(aplatir(extraire())))


fhir_glycemies_quotidien()
