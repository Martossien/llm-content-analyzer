# Doc-IA — guide de l'utilisateur

Doc-IA analyse le contenu des fichiers d'un partage réseau avec une IA locale (rien ne sort de
l'organisme) et vous dit, fichier par fichier : **niveau de sensibilité**, **données personnelles
(RGPD)**, **documents financiers et juridiques**, **durée de conservation à respecter**. Il calcule
aussi ce qui encombre le partage : **doublons**, **fichiers non ouverts depuis des années**,
**fichiers libérables**. Tout est restitué dans l'interface, un rapport HTML pour la direction,
un classeur Excel et un jeu de fichiers pour Power BI.

> Vocabulaire : une **campagne** = un partage (ou un service) audité à une date donnée. Tout tient
> dans un seul fichier `.sqlite` que vous choisissez au départ ; vous pouvez le rouvrir, le
> sauvegarder, le relancer plus tard.

## 1. Installation (poste Windows)

1. Copier dans un même dossier : `Docia.exe` et `SMBeagle.exe` (le scanner).
2. Double-cliquer sur `Docia.exe` : la fenêtre s'ouvre (une console reste derrière, c'est normal).
3. En bas à droite, **mode administrateur** → onglet *Serveur & performances* : l'administrateur
   renseigne l'adresse du serveur IA et clique **Tester la connexion**. Ce réglage est enregistré
   (`docia.toml`) — l'utilisateur n'a pas à y revenir.

L'OCR (lecture des PDF scannés, courriers et factures numérisés) est **intégré** à `Docia.exe` :
aucune installation. `Docia.exe doctor` (ligne de commande) vérifie que tout est en place.

![Accueil — parcours en quatre étapes](images/01_accueil.png)

## 2. Lancer un audit : quatre étapes

### Étape 1 · Source

Bandeau du haut → **Nouvelle…** : choisir où enregistrer la campagne (ex. `Z:\audits\finance-2026.sqlite`).

Puis, carte **1 · Source**, deux possibilités :

- **Scanner maintenant** : indiquer le **lecteur réseau mappé** (ex. `P:\`) ou le dossier à
  auditer (ex. `\\serveur\partage\Finance`), puis **Scanner, importer et préparer**. C'est le
  mode standard : le scan passe par Windows, avec le compte de la session — il doit **lire** le
  partage. La progression s'affiche (fichiers vus) ; on peut **Arrêter** — ce qui a été vu est
  conservé. (Le mode « nom du serveur SMB » existe mais n'est pas utilisé pour l'audit.)
- **Importer un CSV SMBeagle existant** si un scan a déjà été fait ailleurs.

À la fin, la ligne d'état indique combien de fichiers seront analysés et combien sont exclus
(fichiers système, images, archives… exclus par règle, jamais par oubli).

![Source — scanner un partage](images/02_source.png)

### Étape 2 · Serveur IA

Le serveur IA est une machine dédiée (GPU) sur le réseau de l'organisme — son adresse est réglée
une fois par l'administrateur. **Tester la connexion** : ✔ vert = on peut lancer. Sinon, prévenir
l'administrateur (serveur éteint, port filtré, mauvaise adresse).

### Étape 3 · Analyse

**▶ Lancer l'analyse**. La barre indique l'avancement, la vitesse (fichiers/heure) et le temps
restant estimé. Vous pouvez fermer l'application ou **■ Arrêter** à tout moment : **rien n'est
perdu**, la relance reprend exactement où elle s'était arrêtée.

Les très gros fichiers sont lus en entier (par morceaux complets puis réunis), les doublons exacts
ne sont analysés qu'une fois, les PDF scannés passent par l'OCR — automatiquement.

![Analyse en cours](images/03_analyse.png)

### Étape 4 · Consulter

Les trois boutons ouvrent les onglets **Résultats**, **Statistiques**, **Rapports**.

## 3. Lire les résultats

Onglet **Résultats** : un tableau, une ligne par fichier, les plus sensibles en tête. Les couleurs
sont les mêmes partout (interface, rapport) :

| Sécurité | Signification |
|---|---|
| **C3 secret** (rouge) | identifiants, mots de passe, clés, données de santé, informations pouvant compromettre une personne ou la sécurité |
| **C2 confidentiel** (orange) | données personnelles, contrats, informations financières internes |
| **C1 interne** (jaune) | usage interne sans donnée sensible |
| **C0 public** (vert) | rien de sensible |

La colonne **rgpd** va de *aucune* à *critique* ; **conservation** indique la durée et le fondement
(preuve, légal, fiscal, RH, contractuel). Les filtres et la recherche réduisent la liste.

Cliquer une ligne ouvre la **fiche** : résumé en français, justification, données personnelles
repérées, montants, parties, conservation.

![Résultats et fiche d'un fichier](images/04_resultats.png)

### Vérifier (rôle du relecteur)

Sous la fiche : **✔ Valider** si l'IA a raison, **✎ Corriger…** pour choisir la bonne classe (et un
commentaire), ou marquer *à vérifier*. Le compteur « vérifiés par un humain » de l'Accueil et
l'onglet *Statistiques → Vérification* suivent l'avancement de la relecture ; le rapport pour la
direction distingue ce qui a été vérifié.

## 4. Statistiques

Quatre sous-onglets :

- **Hygiène** : espace récupérable (doublons), fichiers non accédés depuis N ans, candidats au
  nettoyage, répartitions par extension / propriétaire / partage / répertoire / taille.
- **Risque** : C3 / C2, RGPD élevé, fichiers sensibles, classification croisée par partage,
  propriétaire ou répertoire.
- **Conservation** : le plan de conservation (fin, durée, fondement, échus).
- **Vérification** : validés / corrigés / à vérifier, écarts entre l'IA et le relecteur.

Le seuil d'ancienneté (années) se règle en haut. La date retenue est la **première** date d'accès
observée : l'audit lui-même (lecture des fichiers, OCR) ne « rajeunit » pas les fichiers, même sur
un lecteur en lecture seule où le scanner ne peut pas restaurer les dates.

![Statistiques — hygiène](images/05_statistiques.png)

## 5. Rapports et exports

Onglet **Rapports** : **Rapport HTML** (un seul fichier, pour la direction), **Markdown**, **Classeur
Excel** (un onglet par vue), **Dossier Power BI** (CSV au schéma stable pour Power BI Report
Server), **CSV** et **JSON**. « Ouvrir le dernier document » l'affiche aussitôt.

À droite, **Sauvegarde de la base** : sauvegarder maintenant, restaurer une sauvegarde. Une
sauvegarde est faite automatiquement avant toute réanalyse complète.

![Rapports et sauvegarde](images/06_rapports.png)

## 6. Relancer plus tard

Accueil, carte **Relancer une analyse** :

- **Seulement ce qui manque** (recommandé) : après un nouveau scan, seuls les fichiers nouveaux ou
  modifiés sont analysés ; les vérifications humaines sont conservées.
- **Aussi les fichiers en erreur**.
- **Tout réanalyser** : quand l'administrateur a changé le prompt ou le modèle (sauvegarde automatique avant).

Le bandeau **Récentes** rouvre une campagne précédente.

## 7. Analyse rapide

Carte **Analyse rapide** : un fichier ou un dossier local, résultat immédiat dans *Résultats* —
pratique pour vérifier un document avant de le déposer.

## 8. Pour l'administrateur (mode administrateur)

- **Prompt** : le texte des consignes données à l'IA est modifiable, enregistrable par profil,
  testable sur un fichier ; tout changement est tracé (les fichiers analysés avec un autre prompt
  seront réanalysés à la demande).
- **Serveur & performances** : adresse et modèle, raisonnement, contexte, chemin du scanner,
  dates d'accès préservées pendant le scan, **Mesurer la vitesse de la LLM** (fichiers/heure,
  JSON valides). Les réglages sont de deux natures :
  - **Envoyés à chaque requête** (ils pilotent, quel que soit le lancement du serveur) :
    *Tokens par bloc* = combien de contenu docia regroupe par requête (32 000 : moins de requêtes,
    échecs isolés) ; *Budget de raisonnement* = plafond de « réflexion » **imposé** au modèle par
    requête (6 000 : coupé net au-delà, la réponse JSON garde toujours sa place) et l'*Effort*
    (medium mesuré = même qualité que xhigh, 2–3× plus rapide) ; *Température* ; *Fichiers par lot
    DocFuse* = rythme d'extraction sur le poste (local, le serveur n'y est pour rien).
    *Requêtes en vol* = nombre de blocs envoyés **en même temps** au serveur (8 : le GPU est
    nourri en continu — c'est normal et voulu, le serveur sait traiter plusieurs requêtes à la
    fois ; monter au-delà n'accélère pas, ça allonge la file) ; *Timeout* = patience par requête.
  - **Descriptifs du serveur** (doivent correspondre au lancement, sinon erreurs) : *URL de base* —
    l'adresse de la machine qui héberge la LLM, en général **une autre machine** que le poste :
    `http://nom-ou-IP-du-serveur:8000/v1` (vLLM) ou `http://serveur:8080/api` (open-webui) ;
    `127.0.0.1` ne vaut que si la LLM tourne sur le poste lui-même. C'est le **seul flux réseau**
    de Doc-IA, et il ne transporte que du texte. Ensuite : nom du modèle, clé, et *Contexte du
    modèle*. Ce dernier est **auto-vérifié** : au début de chaque
    analyse, docia lit la valeur réellement servie et s'y borne en avertissant si la config diverge
    (`docia doctor` l'affiche aussi).
- Ligne de commande : `Docia.exe doctor`, `scan`, `run`, `report`, `export`, `backup`,
  `restore`, `reanalyze`, `campaigns` (voir README).

![Serveur & performances](images/07_serveur.png)

## 9. Questions fréquentes

- *Un PDF scanné sort « non évalué »* → `Docia.exe doctor` : l'OCR embarqué doit apparaître
  (`ocr_engines ['tesseract']`). Sinon l'exe est incomplet : reprendre le build de la CI.
- *« scanner SMBeagle introuvable »* → placer `SMBeagle.exe` à côté de `Docia.exe` ou renseigner
  le chemin dans *Serveur & performances*.
- *L'analyse est lente* → c'est le serveur IA qui fixe le débit ; le raisonnement `medium` est
  le meilleur compromis mesuré ; `xhigh` double le temps sans gain.
- *Un fichier est « en erreur »* → sa raison est dans la fiche ; **Relancer → Aussi les fichiers en
  erreur** après avoir corrigé la cause (fichier verrouillé, chemin disparu…).
