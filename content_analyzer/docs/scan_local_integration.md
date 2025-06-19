# Intégration du fichier `scan_local_mini.csv`

Ce document décrit l'ajout du fichier de scan local et les adaptations
apportées au parseur CSV afin de l'intégrer dans le pipeline.

## Adaptations principales

- Normalisation des chemins UNC contenant des doubles échappements.
- Conversion des champs booléens "True"/"False" en valeurs Python.
- Parsing robuste des dates au format `DD/MM/YYYY HH:mm:ss` avec tolérance
  pour d'autres formats.
- Remplacement des extensions manquantes par `"unknown"`.
- Nettoyage du champ `Owner` pour supprimer les valeurs `<ERROR_5>`.

Les tests unitaires couvrent désormais l'import complet du fichier et vérifient
que les 63 enregistrements sont correctement insérés en base.
