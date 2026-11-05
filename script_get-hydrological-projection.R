# Explore2 - Catalogue et téléchargement des fichiers NetCDF


## 0. REQUIREMENT ____________________________________________________
if(!require(httr2)) install.packages("httr2")
if(!require(dplyr)) install.packages("dplyr")

BASE = "https://entrepot.recherche.data.gouv.fr"
COLLECTION = "explore2-projections_hydrologiques"
COLUMNS = c("region", "gcm", "scenario", "run", "rcm", "rcm_ver",
            "bias", "hm", "freq", "dates")


## 1. FONCTIONS ______________________________________________________
# Prépare une requête abandonnée après `timeout` secondes sans recevoir
# de données. req_timeout() limiterait la durée totale de la requête
# et couperait les téléchargements longs.
new_request = function(url, timeout) {
    httr2::req_options(httr2::request(url),
                       connecttimeout=timeout,
                       low_speed_limit=1,
                       low_speed_time=timeout)
}

# Découpe un nom de fichier Explore2 en ses composantes.
parse_filename = function(filename) {
    parts = strsplit(sub("\\.nc$", "", filename), "_", fixed=TRUE)[[1]]
    if (length(parts) != 11 || parts[1] != "debit") {
        return (NULL)
    }
    return (setNames(as.list(parts[-1]), COLUMNS))
}

# Retourne la liste des datasets 'Projections hydrologiques' de la
# collection.
get_datasets = function() {
    req = httr2::req_url_query(new_request(paste0(BASE, "/api/search"),
                                           timeout=15),
                               q="*",
                               subtree=COLLECTION,
                               type="dataset",
                               per_page=100)
    items = httr2::resp_body_json(httr2::req_perform(req))$data$items
    Filter(function(d) {
        !is.null(d$name) && startsWith(d$name, "Projections hydrologiques")
    }, items)
}

# Retourne la liste des fichiers .nc d'un dataset.
get_files = function(dataset_doi) {
    url = paste0(BASE, "/api/datasets/:persistentId/versions/:latest/files")
    req = httr2::req_url_query(new_request(url, timeout=20),
                               persistentId=dataset_doi)
    files = httr2::resp_body_json(httr2::req_perform(req))$data
    Filter(function(f) endsWith(f$dataFile$filename, ".nc"), files)
}

# Construit le catalogue complet de tous les fichiers et le sauvegarde
# en CSV. Retourne le catalogue sous forme de tibble, une ligne par
# fichier.
build_catalog = function(output_csv="explore2_catalog.csv") {
    datasets = get_datasets()
    cat(sprintf("%d datasets trouvés\n", length(datasets)))

    Rows = list()
    for (i in seq_along(datasets)) {
        doi = datasets[[i]]$global_id
        name = datasets[[i]]$name
        cat(sprintf("  [%d/%d] %s ... ",
                    i, length(datasets), substr(name, 1, 60)))

        files = get_files(doi)
        cat(sprintf("%d fichiers\n", length(files)))

        for (f in files) {
            filename = f$dataFile$filename
            parsed = parse_filename(filename)
            if (is.null(parsed)) {
                cat(sprintf("    [ignoré] %s\n", filename))
                next
            }
            file_id = format(f$dataFile$id, scientific=FALSE, trim=TRUE)
            filesize = f$dataFile$filesize
            if (is.null(filesize)) {
                filesize = 0
            }
            Rows[[length(Rows) + 1]] =
                c(list(filename=filename,
                       file_id=file_id,
                       url=paste0(BASE, "/api/access/datafile/", file_id),
                       size_mb=round(filesize / 1e6, 1)),
                  parsed)
        }
    }

    rows = dplyr::bind_rows(Rows)
    write.csv(rows, output_csv, row.names=FALSE, quote=FALSE)

    cat(sprintf("\n%d fichiers -> %s\n", nrow(rows), output_csv))
    return (rows)
}

# Filtre le catalogue par regexp sur les colonnes, sans tenir compte de
# la casse.
#
# Exemples :
#     filter_files(rows, scenario="rcp85")
#     filter_files(rows, hm="EROS", bias="ADAMONT")
filter_files = function(rows, ...) {
    patterns = list(...)
    for (key in names(patterns)) {
        rows = rows[grepl(patterns[[key]], rows[[key]],
                          ignore.case=TRUE, perl=TRUE), ]
    }
    return (rows)
}

# Télécharge les fichiers d'une sélection du catalogue. Chaque fichier
# est écrit sous un nom temporaire et ne prend son nom définitif qu'une
# fois complet, pour qu'un téléchargement interrompu soit repris au
# lancement suivant.
download_files = function(rows, output_dir="./explore2_data") {
    dir.create(output_dir, showWarnings=FALSE, recursive=TRUE)

    total_gb = sum(rows$size_mb) / 1000
    cat(sprintf("%d fichiers à télécharger (%.2f Go) -> %s\n\n",
                nrow(rows), total_gb, output_dir))

    for (i in seq_len(nrow(rows))) {
        filename = rows$filename[i]
        dest = file.path(output_dir, filename)
        if (file.exists(dest)) {
            cat(sprintf("[%d/%d] SKIP %s\n", i, nrow(rows), filename))
            next
        }

        cat(sprintf("[%d/%d] %s (%s Mo)\n",
                    i, nrow(rows), filename, rows$size_mb[i]))
        dest_part = paste0(dest, ".part")
        req = httr2::req_progress(new_request(rows$url[i], timeout=120))
        httr2::req_perform(req, path=dest_part)
        file.rename(dest_part, dest)
        cat("  OK\n")
    }
    invisible(NULL)
}

# Affiche les valeurs disponibles par colonne pour explorer le
# catalogue.
summary_catalog = function(rows) {
    total_gb = sum(rows$size_mb) / 1000
    line = strrep("=", 55)
    cat(sprintf("\n%s\n  %d fichiers  /  %.1f Go au total\n%s\n",
                line, nrow(rows), total_gb, line))
    for (col in c("region", "scenario", "gcm", "rcm", "bias", "hm")) {
        cat(sprintf("\n  %s\n", col))
        for (v in sort(unique(rows[[col]]), method="radix")) {
            cat(sprintf("    %-55s (%d fichiers)\n",
                        v, sum(rows[[col]] == v)))
        }
    }
    cat("\n")
}


## 2. EXEMPLE D'UTILISATION __________________________________________
# Construire le catalogue
catalog = build_catalog("explore2_catalog.csv")

# Explorer les options disponibles
summary_catalog(catalog)

# Filtrer (regexp, insensible à la casse)
selection = filter_files(catalog,
                         scenario="rcp85",
                         gcm="CNRM",
                         bias="ADAMONT",
                         hm="J2000")
summary_catalog(selection)

# Télécharger
download_files(selection, output_dir="./explore2_data")
