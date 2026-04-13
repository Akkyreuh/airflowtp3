"""
DAG : Pipeline DVF — Data Lake (HDFS) vers Data Warehouse (PostgreSQL)
Source : Demandes de Valeurs Foncières — data.gouv.fr
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
DVF_URL = "https://www.data.gouv.fr/api/1/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

# ── Configuration du DAG ──────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval="0 6 * * *",  # tous les jours à 6h
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        """
        Vérifie la disponibilité des sources de données et de l'infrastructure.
        Retourne un dictionnaire de la forme :
        {
            "dvf_api": True,
            "hdfs": True,
            "timestamp": "2024-01-15T06:00:00"
        }
        Lève une AirflowException si une source critique est indisponible.
        """
        statuts = {}

        # TODO 1 : Vérifier l'API data.gouv.fr
        try:
            response = requests.head(DVF_URL, timeout=10, allow_redirects=True)
            statuts["dvf_api"] = response.status_code < 500
        except requests.RequestException:
            statuts["dvf_api"] = False

        # TODO 2 : Vérifier la disponibilité de HDFS (WebHDFS)
        try:
            hdfs_response = requests.get(
                f"{WEBHDFS_BASE_URL}/?op=LISTSTATUS&user.name={WEBHDFS_USER}",
                timeout=10,
            )
            statuts["hdfs"] = hdfs_response.status_code == 200
        except requests.RequestException:
            statuts["hdfs"] = False

        # TODO 3 : Logger l'état de chaque service
        for service, ok in statuts.items():
            logger.info("Service [%s] : %s", service, "OK" if ok else "KO")

        # TODO 4 : Lever une exception si HDFS ou l'API DVF est inaccessible
        if not statuts.get("dvf_api"):
            raise AirflowException("L'API data.gouv.fr (DVF) est inaccessible.")
        if not statuts.get("hdfs"):
            raise AirflowException("Le cluster HDFS (WebHDFS) est inaccessible.")

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        """
        Télécharge le fichier CSV DVF depuis data.gouv.fr.
        Paramètre :
            statuts (dict) : résultat de verifier_sources()
        Retourne :
            str : chemin local du fichier CSV téléchargé (ex: /tmp/dvf_2023.csv)
        Le téléchargement doit être fait en streaming pour gérer les gros fichiers
        (le fichier DVF peut dépasser 500 Mo).
        """
        # TODO 1 : Construire le nom du fichier local
        annee = datetime.now().year
        local_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}.csv")

        # TODO 2 : Télécharger en streaming
        logger.info("Début du téléchargement DVF -> %s", local_path)
        response = requests.get(DVF_URL, stream=True, timeout=300)
        response.raise_for_status()

        downloaded = 0
        chunk_size = 8192
        log_interval = 50 * 1024 * 1024  # 50 Mo
        next_log = log_interval

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= next_log:
                        logger.info("Téléchargé : %.1f Mo", downloaded / 1024 / 1024)
                        next_log += log_interval

        # TODO 3 : Vérifier que le fichier téléchargé n'est pas vide
        size = os.path.getsize(local_path)
        if size < 1000:
            raise AirflowException(
                f"Fichier téléchargé trop petit ({size} octets), téléchargement échoué."
            )

        # TODO 4 : Logger la taille finale et retourner le chemin
        logger.info("Téléchargement terminé : %.2f Mo -> %s", size / 1024 / 1024, local_path)
        return local_path

    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        """
        Uploade le fichier CSV local vers HDFS (zone raw / data lake).
        Structure Hive-style partitionnée :
            /data/dvf/raw/annee=YYYY/dept=75/dvf_YYYY_75.csv
        Paramètre :
            local_path (str) : chemin du fichier CSV local (depuis telecharger_dvf)
        Retourne :
            str : chemin HDFS du fichier uploadé
        Rappel de l'upload WebHDFS en deux étapes :
            Etape 1 - Initier : PUT NameNode -> 307 Redirect
            Etape 2 - Uploader : PUT DataNode (URL de redirection) avec les données
        """
        annee = datetime.now().year
        dept = "75"
        # Bonus 1 : structure Hive-style partitionnée (partition pruning compatible Spark/Hive)
        hdfs_partition_path = f"{HDFS_RAW_PATH}/annee={annee}/dept={dept}"
        hdfs_filename = f"dvf_{annee}_{dept}.csv"
        hdfs_file_path = f"{hdfs_partition_path}/{hdfs_filename}"

        # TODO 1 : Créer le répertoire HDFS partitionné si nécessaire
        mkdirs_url = f"{WEBHDFS_BASE_URL}{hdfs_partition_path}/?op=MKDIRS&user.name={WEBHDFS_USER}"
        mkdirs_resp = requests.put(mkdirs_url)
        mkdirs_resp.raise_for_status()
        if not mkdirs_resp.json().get("boolean"):
            raise AirflowException(f"Impossible de créer le répertoire HDFS : {hdfs_partition_path}")
        logger.info("Répertoire HDFS créé (ou déjà existant) : %s", hdfs_partition_path)

        # TODO 2 : Initier l'upload (étape 1)
        create_url = (
            f"{WEBHDFS_BASE_URL}{hdfs_file_path}"
            f"?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true"
        )
        init_resp = requests.put(create_url, allow_redirects=False)
        if init_resp.status_code not in (307, 200):
            init_resp.raise_for_status()
        redirect_url = init_resp.headers["Location"]
        logger.info("Redirection DataNode : %s", redirect_url)

        # TODO 3 : Envoyer le fichier vers le DataNode (étape 2)
        with open(local_path, "rb") as f:
            upload_resp = requests.put(
                redirect_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
            )
        if upload_resp.status_code != 201:
            raise AirflowException(
                f"Upload HDFS échoué : HTTP {upload_resp.status_code} — {upload_resp.text}"
            )
        logger.info("Fichier uploadé vers HDFS : %s", hdfs_file_path)

        # TODO 4 : Nettoyer le fichier temporaire local après upload
        os.remove(local_path)
        logger.info("Fichier temporaire supprimé : %s", local_path)

        # TODO 5 : Logger le chemin HDFS et retourner hdfs_file_path
        return hdfs_file_path

    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        """
        Lit le CSV depuis HDFS, applique les filtres métier et calcule les agrégats.
        Filtres à appliquer :
            - Type de bien : "Appartement" uniquement
            - Commune : Paris (codes postaux 75001 à 75020)
            - Surface : entre 9 m² et 500 m²
            - Prix : supérieur à 10 000 EUR
            - Nature de la mutation : "Vente"
        Retourne :
            dict avec deux clés :
                "agregats"       : liste de dicts (une ligne par arrondissement)
                "stats_globales" : dict avec les statistiques globales Paris
        """
        import io

        # TODO 1 : Lire le CSV depuis HDFS
        open_url = f"{WEBHDFS_BASE_URL}{hdfs_path}?op=OPEN&user.name={WEBHDFS_USER}"
        response = requests.get(open_url, allow_redirects=True, timeout=300)
        response.raise_for_status()
        df = pd.read_csv(io.BytesIO(response.content), sep=",", low_memory=False, encoding="latin-1")
        logger.info("CSV chargé depuis HDFS : %d lignes, %d colonnes", len(df), len(df.columns))

        # TODO 2 : Renommer les colonnes (minuscules, espaces -> _)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # TODO 3 : Filtrer les données
        nb_avant = len(df)
        df = df[df["type_local"] == "Appartement"]
        df = df[df["nature_mutation"] == "Vente"]
        df["code_postal"] = df["code_postal"].astype(str).str.strip()
        codes_paris = [str(75000 + i) for i in range(1, 21)]
        df = df[df["code_postal"].isin(codes_paris)]
        df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
        df["valeur_fonciere"] = (
            pd.to_numeric(
                df["valeur_fonciere"].astype(str).str.replace(",", "."), errors="coerce"
            )
        )
        df = df[df["surface_reelle_bati"].between(9, 500)]
        df = df[df["valeur_fonciere"] > 10_000]
        logger.info("Filtrage : %d -> %d lignes", nb_avant, len(df))

        # TODO 4 : Calculer le prix au m²
        df = df[df["surface_reelle_bati"] > 0].copy()
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        # TODO 5 : Extraire l'arrondissement depuis le code postal
        def code_postal_to_arrdt(cp: str) -> int:
            suffix = int(cp) - 75000
            return 16 if suffix == 116 else suffix

        df["arrondissement"] = df["code_postal"].apply(code_postal_to_arrdt)

        # TODO 6 : Agréger par arrondissement
        now = datetime.now()
        df["annee"] = now.year
        df["mois"] = now.month

        agg = (
            df.groupby(["code_postal", "arrondissement", "annee", "mois"])
            .agg(
                prix_m2_moyen=("prix_m2", "mean"),
                prix_m2_median=("prix_m2", "median"),
                prix_m2_min=("prix_m2", "min"),
                prix_m2_max=("prix_m2", "max"),
                nb_transactions=("prix_m2", "count"),
                surface_moyenne=("surface_reelle_bati", "mean"),
            )
            .reset_index()
        )

        agregats = agg.to_dict(orient="records")

        # TODO 7 : Calculer les statistiques globales Paris
        stats_globales = {
            "annee": now.year,
            "mois": now.month,
            "nb_transactions_total": int(len(df)),
            "prix_m2_median_paris": float(df["prix_m2"].median()),
            "prix_m2_moyen_paris": float(df["prix_m2"].mean()),
            "arrdt_plus_cher": int(agg.loc[agg["prix_m2_median"].idxmax(), "arrondissement"]),
            "arrdt_moins_cher": int(agg.loc[agg["prix_m2_median"].idxmin(), "arrondissement"]),
            "surface_mediane": float(df["surface_reelle_bati"].median()),
        }
        logger.info("Stats globales Paris : %s", stats_globales)

        # TODO 8 : Retourner le dictionnaire résultat
        return {"agregats": agregats, "stats_globales": stats_globales}

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        """
        Insère les données agrégées dans PostgreSQL (zone curated).
        Utilise un UPSERT (INSERT ... ON CONFLICT DO UPDATE) pour l'idempotence.
        Paramètre :
            resultats (dict) : résultat de traiter_donnees()
        Retourne :
            int : nombre de lignes insérées ou mises à jour
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # TODO 1 : Récupérer les agrégats depuis le dictionnaire résultats
        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})

        # TODO 2 : Requête UPSERT pour prix_m2_arrondissement
        upsert_query = """
            INSERT INTO prix_m2_arrondissement
                (code_postal, arrondissement, annee, mois,
                 prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
                 nb_transactions, surface_moyenne, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
                prix_m2_moyen    = EXCLUDED.prix_m2_moyen,
                prix_m2_median   = EXCLUDED.prix_m2_median,
                prix_m2_min      = EXCLUDED.prix_m2_min,
                prix_m2_max      = EXCLUDED.prix_m2_max,
                nb_transactions  = EXCLUDED.nb_transactions,
                surface_moyenne  = EXCLUDED.surface_moyenne,
                updated_at       = NOW();
        """

        # TODO 3 : Exécuter l'UPSERT pour chaque arrondissement
        nb_inseres = 0
        for row in agregats:
            hook.run(
                upsert_query,
                parameters=(
                    str(row["code_postal"]),
                    int(row["arrondissement"]),
                    int(row["annee"]),
                    int(row["mois"]),
                    float(row["prix_m2_moyen"]),
                    float(row["prix_m2_median"]),
                    float(row["prix_m2_min"]),
                    float(row["prix_m2_max"]),
                    int(row["nb_transactions"]),
                    float(row["surface_moyenne"]),
                ),
            )
            nb_inseres += 1

        # TODO 4 : Insérer les statistiques globales dans stats_marche
        upsert_stats = """
            INSERT INTO stats_marche
                (annee, mois, nb_transactions_total, prix_m2_median_paris,
                 prix_m2_moyen_paris, arrdt_plus_cher, arrdt_moins_cher, surface_mediane)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (annee, mois) DO UPDATE SET
                nb_transactions_total  = EXCLUDED.nb_transactions_total,
                prix_m2_median_paris   = EXCLUDED.prix_m2_median_paris,
                prix_m2_moyen_paris    = EXCLUDED.prix_m2_moyen_paris,
                arrdt_plus_cher        = EXCLUDED.arrdt_plus_cher,
                arrdt_moins_cher       = EXCLUDED.arrdt_moins_cher,
                surface_mediane        = EXCLUDED.surface_mediane,
                date_calcul            = NOW();
        """
        hook.run(
            upsert_stats,
            parameters=(
                int(stats_globales.get("annee", datetime.now().year)),
                int(stats_globales.get("mois", datetime.now().month)),
                int(stats_globales.get("nb_transactions_total", 0)),
                float(stats_globales.get("prix_m2_median_paris", 0)),
                float(stats_globales.get("prix_m2_moyen_paris", 0)),
                int(stats_globales.get("arrdt_plus_cher", 0)),
                int(stats_globales.get("arrdt_moins_cher", 0)),
                float(stats_globales.get("surface_mediane", 0)),
            ),
        )

        # TODO 5 : Retourner le nombre de lignes traitées
        logger.info("UPSERT terminé : %d arrondissements insérés/mis à jour", nb_inseres)
        return nb_inseres

    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        """
        Génère un rapport SQL listant les arrondissements parisiens
        classés par prix médian au m² décroissant.
        Paramètre :
            nb_inseres (int) : nombre de lignes insérées (depuis inserer_postgresql)
        Retourne :
            str : rapport formaté (tableau texte)
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # TODO 1 : Requête SQL de ranking
        query = """
            SELECT
                arrondissement,
                prix_m2_median,
                prix_m2_moyen,
                nb_transactions,
                surface_moyenne
            FROM prix_m2_arrondissement
            WHERE annee = %s AND mois = %s
            ORDER BY prix_m2_median DESC
            LIMIT 20;
        """

        # TODO 2 : Exécuter la requête
        now = datetime.now()
        records = hook.get_records(query, parameters=(now.year, now.month))

        # TODO 3 : Formater les résultats en tableau lisible
        header = (
            f"{'Arrondissement':<16} | {'Médian (EUR/m²)':>15} | "
            f"{'Moyen (EUR/m²)':>14} | {'Transactions':>12} | {'Surface moy.':>12}"
        )
        separator = "-" * len(header)
        lines = [header, separator]

        for arrdt, median, moyen, nb_trans, surface in records:
            lines.append(
                f"{arrdt:<16} | {median:>15,.0f} | {moyen:>14,.0f} | "
                f"{nb_trans:>12} | {surface:>12.1f}"
            )

        lines.append(separator)
        lines.append(f"Lignes insérées/mises à jour : {nb_inseres}")
        rapport = "\n".join(lines)

        # TODO 4 : Logger le rapport
        logger.info("Rapport DVF :\n%s", rapport)

        # TODO 5 : Retourner le rapport
        return rapport

    @task(task_id="analyser_tendances")
    def analyser_tendances(rapport: str) -> str:
        """
        Bonus 2 : Calcule l'évolution du prix au m² entre le mois courant et le mois précédent
        par arrondissement, et met à jour stats_marche avec les résultats.
        Paramètre :
            rapport (str) : rapport depuis generer_rapport()
        Retourne :
            str : tableau des variations mois sur mois
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        now = datetime.now()

        query = """
            SELECT
                a.arrondissement,
                a.prix_m2_median                                          AS prix_courant,
                b.prix_m2_median                                          AS prix_precedent,
                ROUND(
                    ((a.prix_m2_median - b.prix_m2_median) / b.prix_m2_median) * 100,
                    2
                )                                                         AS variation_pct
            FROM prix_m2_arrondissement a
            JOIN prix_m2_arrondissement b
                ON  a.arrondissement = b.arrondissement
                AND (
                    (a.mois > 1 AND b.annee = a.annee AND b.mois = a.mois - 1)
                    OR
                    (a.mois = 1 AND b.annee = a.annee - 1 AND b.mois = 12)
                )
            WHERE a.annee = %s AND a.mois = %s
            ORDER BY variation_pct DESC;
        """
        records = hook.get_records(query, parameters=(now.year, now.month))

        header = f"{'Arrondissement':<16} | {'M (EUR/m²)':>12} | {'M-1 (EUR/m²)':>13} | {'Variation':>10}"
        separator = "-" * len(header)
        lines = ["\n=== Tendances mois sur mois ===", header, separator]

        for arrdt, prix_c, prix_p, variation in records:
            tendance = "▲" if variation > 0 else ("▼" if variation < 0 else "=")
            lines.append(
                f"{arrdt:<16} | {prix_c:>12,.0f} | {prix_p:>13,.0f} | {tendance} {variation:>+.2f}%"
            )

        lines.append(separator)
        tendances_rapport = "\n".join(lines)
        logger.info(tendances_rapport)
        return tendances_rapport

    # ── Orchestration des tâches ──────────────────────────────────────────────
    statuts = verifier_sources()
    local_path = telecharger_dvf(statuts)
    hdfs_path = stocker_hdfs_raw(local_path)
    resultats = traiter_donnees(hdfs_path)
    nb_inseres = inserer_postgresql(resultats)
    rapport = generer_rapport(nb_inseres)
    tendances = analyser_tendances(rapport)

    chain(statuts, local_path, hdfs_path, resultats, nb_inseres, rapport, tendances)


pipeline_dvf()
