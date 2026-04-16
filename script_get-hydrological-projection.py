#!/usr/bin/env python3
"""
Explore2 - Catalogue et téléchargement des fichiers NetCDF
"""

import csv
import requests
from pathlib import Path

BASE       = "https://entrepot.recherche.data.gouv.fr"
COLLECTION = "explore2-projections_hydrologiques"
COLUMNS    = ["region", "gcm", "scenario", "run", "rcm", "rcm_ver", "bias", "hm", "freq", "dates"]


def parse_filename(filename):
    """Découpe un nom de fichier Explore2 en ses composantes."""
    parts = filename.removesuffix(".nc").split("_")
    if len(parts) != 11 or parts[0] != "debit":
        return None
    return dict(zip(COLUMNS, parts[1:]))


def get_datasets():
    """Retourne la liste des datasets 'Projections hydrologiques' de la collection."""
    r = requests.get(f"{BASE}/api/search", params={
        "q": "*",
        "subtree": COLLECTION,
        "type": "dataset",
        "per_page": 100,
    }, timeout=15)
    r.raise_for_status()
    items = r.json()["data"]["items"]
    return [d for d in items if d.get("name", "").startswith("Projections hydrologiques")]


def get_files(dataset_doi):
    """Retourne la liste des fichiers .nc d'un dataset."""
    r = requests.get(
        f"{BASE}/api/datasets/:persistentId/versions/:latest/files",
        params={"persistentId": dataset_doi},
        timeout=20,
    )
    r.raise_for_status()
    return [f for f in r.json().get("data", []) if f["dataFile"]["filename"].endswith(".nc")]


def build_catalog(output_csv="explore2_files.csv"):
    """
    Construit le catalogue complet de tous les fichiers et le sauvegarde en CSV.
    Retourne la liste des entrées sous forme de dicts.
    """
    datasets = get_datasets()
    print(f"{len(datasets)} datasets trouvés")

    rows = []
    for i, ds in enumerate(datasets):
        doi  = ds["global_id"]
        name = ds["name"]
        print(f"  [{i+1}/{len(datasets)}] {name[:60]} ...", end=" ", flush=True)

        files = get_files(doi)
        print(f"{len(files)} fichiers")

        for f in files:
            filename = f["dataFile"]["filename"]
            parsed   = parse_filename(filename)
            if parsed is None:
                print(f"    [ignoré] {filename}")
                continue
            rows.append({
                "filename" : filename,
                "file_id"  : f["dataFile"]["id"],
                "url"      : f"{BASE}/api/access/datafile/{f['dataFile']['id']}",
                "size_mb"  : round(f["dataFile"].get("filesize", 0) / 1e6, 1),
                **parsed,
            })

    fieldnames = ["filename", "file_id", "url", "size_mb"] + COLUMNS
    with open(output_csv, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{len(rows)} fichiers -> {output_csv}")
    return rows


def filter_files(rows, **kwargs):
    """
    Filtre le catalogue par regexp sur les colonnes.

    Exemples :
        filter_files(rows, scenario="rcp85")
        filter_files(rows, hm="EROS", bias="ADAMONT")
    """
    import re
    result = rows
    for key, pattern in kwargs.items():
        result = [r for r in result if re.search(pattern, r.get(key, ""), re.IGNORECASE)]
    return result


def download_files(rows, output_dir="./explore2_data"):
    """Télécharge les fichiers d'une liste de lignes du catalogue."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    total_gb = sum(float(r["size_mb"]) for r in rows) / 1000
    print(f"{len(rows)} fichiers à télécharger ({total_gb:.2f} Go) -> {out}\n")

    for i, row in enumerate(rows):
        dest = out / row["filename"]
        if dest.exists():
            print(f"[{i+1}/{len(rows)}] SKIP {row['filename']}")
            continue

        print(f"[{i+1}/{len(rows)}] {row['filename']} ({row['size_mb']} Mo)")
        with requests.get(row["url"], stream=True, timeout=120) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done  = 0
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)
                    done += len(chunk)
                    if total:
                        print(f"\r  {done/1e6:.1f}/{total/1e6:.1f} Mo  ({done/total*100:.0f}%)",
                              end="", flush=True)
        print(f"\r  OK" + " " * 40)


def summary(rows):
    """Affiche les valeurs disponibles par colonne pour explorer le catalogue."""
    total_gb = sum(float(r["size_mb"]) for r in rows) / 1000
    print(f"\n{'='*55}")
    print(f"  {len(rows)} fichiers  /  {total_gb:.1f} Go au total")
    print(f"{'='*55}")
    for col in ["region", "scenario", "gcm", "rcm", "bias", "hm"]:
        vals = sorted(set(r[col] for r in rows))
        print(f"\n  {col}")
        for v in vals:
            count = sum(1 for r in rows if r[col] == v)
            print(f"    {v:<55} ({count} fichiers)")
    print()


# ── Exemple d'utilisation ─────────────────────────────────────────────────────
if __name__ == "__main__":
    # Construire le catalogue
    catalog = build_catalog("explore2_files.csv")
    
    # Explorer les options disponibles
    summary(catalog)

    # Filtrer (regexp, insensible à la casse)
    selection = filter_files(catalog,
                             scenario="rcp85",
                             gcm="CNRM",
                             bias="ADAMONT",
                             hm="EROS")
    print(f"{len(selection)} fichiers sélectionnés")
    summary(selection)

    # Télécharger
    download_files(selection, output_dir="./explore2_data")
