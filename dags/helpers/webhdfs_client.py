"""
Client WebHDFS pour interagir avec le cluster HDFS via l'API REST.
Documentation : https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
"""
import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    """Client léger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        """Construit l'URL WebHDFS pour une opération donnée."""
        path = path.lstrip("/")
        query = f"op={op}&user.name={self.user}"
        for key, value in params.items():
            query += f"&{key}={value}"
        return f"{self.base_url}/{path}?{query}"

    def mkdirs(self, hdfs_path: str) -> bool:
        """
        Crée un répertoire (et ses parents) dans HDFS.
        Retourne True si succès, lève une exception sinon.
        """
        url = self._url(hdfs_path, "MKDIRS")
        response = requests.put(url)
        response.raise_for_status()
        result = response.json()
        logger.info("mkdirs %s -> %s", hdfs_path, result)
        return result["boolean"]

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        """
        Uploade un fichier local vers HDFS.
        Retourne le chemin HDFS du fichier uploadé.
        Rappel : WebHDFS upload = 2 étapes
        1. PUT sur le NameNode (allow_redirects=False) -> récupère l'URL de redirection
        2. PUT sur le DataNode avec le contenu binaire du fichier
        """
        url = self._url(hdfs_path, "CREATE")
        # Étape 1 : initier l'upload, récupérer la redirection 307
        init_response = requests.put(url, allow_redirects=False)
        if init_response.status_code not in (307, 200):
            init_response.raise_for_status()
        redirect_url = init_response.headers["Location"]

        # Étape 2 : envoyer le contenu binaire vers le DataNode
        with open(local_file_path, "rb") as f:
            upload_response = requests.put(
                redirect_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
            )
        upload_response.raise_for_status()
        logger.info("upload %s -> %s (HTTP %s)", local_file_path, hdfs_path, upload_response.status_code)
        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        """
        Lit le contenu d'un fichier HDFS.
        Retourne les données brutes (bytes).
        """
        url = self._url(hdfs_path, "OPEN")
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()
        logger.info("open %s -> %d bytes", hdfs_path, len(response.content))
        return response.content

    def exists(self, hdfs_path: str) -> bool:
        """Vérifie si un fichier ou répertoire existe dans HDFS."""
        url = self._url(hdfs_path, "GETFILESTATUS")
        response = requests.get(url)
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        response.raise_for_status()

    def list_status(self, hdfs_path: str) -> list:
        """Liste le contenu d'un répertoire HDFS."""
        url = self._url(hdfs_path, "LISTSTATUS")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.info("list_status %s -> %d entries", hdfs_path, len(data["FileStatuses"]["FileStatus"]))
        return data["FileStatuses"]["FileStatus"]
