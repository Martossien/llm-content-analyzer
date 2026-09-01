# Doc-IA analyzer v3 — conception

*30/08/2026 — remplace le POC `legacy/` (18 000 lignes, 59 % de GUI Tkinter). Contexte et décisions amont : `~/Doc-IA/docs/ANALYSE_2026-08-30.md` (D1–D7).*

## 1. Rôle

Sur un **poste Windows** ayant accès aux partages : lire le CSV SMBeagle, tenir une base
SQLite des fichiers, extraire le texte **localement** avec DocFuse (bibliothèque), envoyer des
**blocs texte** de 16–64K tokens à une LLM (vLLM direct ou open-webui natif), recevoir un
**JSON multi-fichiers** garanti par schéma, persister une analyse par fichier, **reprendre** après
interruption sans rien refaire d'inutile. Tout en CLI d'abord ; GUI et exe en 4b.

## 2. Non-négociables

- Un fichier = un `## SOURCE:` entier dans un bloc (DocFuse ne coupe jamais) ; la LLM rend une
  entrée par `file_ref` = valeur exacte de la ligne `## SOURCE:`.
- Aucune « réparation » de JSON : le schéma est imposé au serveur (`response_format`
  `json_schema`), le client **valide** et rejette proprement.
- Jamais de perte silencieuse : chaque fichier a un statut et une raison (exclu, introuvable,
  extraction en erreur, absent de la réponse, hors plafond…).
- Reprise : une analyse est liée à `(file_id, content_version, prompt_hash, model)`. Relancer
  `run` ne renvoie que ce qui manque. Un bloc `sent` sans résultat au redémarrage est renvoyé.
- Rescan : un fichier déjà connu dont `fast_hash`, `size` ou `last_write_time` change prend
  `content_version + 1` et redevient à analyser ; sinon il est inchangé (compteur exact).
- Compatible Windows (chemins UNC, `normcase` pour les clés), zéro dépendance réseau hors LLM,
  dépendances minimales : `docfuse`, `httpx`. Stdlib pour SQLite, TOML, JSON, argparse.

## 3. Architecture

```
src/docia/
├── cli.py            argparse : init | ingest | plan | run | status | export | retry
├── config.py         docia.toml (tomllib) → Config (dataclass), validation
├── models.py         dataclasses partagées (FileRow, BlockSpec, FileAnalysis, …)
├── db.py             SQLite (WAL), schéma versionné, requêtes (upsert, sélection, statuts)
├── ingest/
│   └── smbeagle_csv.py   parseur CSV 19 colonnes à guillemets sélectifs (porté du legacy)
├── filter.py         exclusions (extensions, taille, dossiers système) + score de priorité
├── blocks/
│   └── builder.py    DocFuse : run_analysis(split_context) → split_by_budget → blocs .md
├── llm/
│   ├── schema.py     JSON Schema de sortie (validé xgrammar) + prompt system + prompt_hash
│   ├── client.py     httpx async, N en vol, retries ; transports vllm | openwebui
│   └── parse.py      validation de la réponse, corrélation file_ref → fichiers du bloc
├── pipeline.py       run : sélection → blocs → envoi → persistance → statuts
└── prompts/docia_v3.md
```

## 4. Base SQLite

| Table | Rôle |
|---|---|
| `meta` | `schema_version` |
| `scans` | un import CSV : compteurs nouveaux / modifiés / inchangés |
| `files` | un chemin = une ligne (`path` UNIQUE, clé `normcase`) ; métadonnées SMBeagle ; `content_version` ; `status` ∈ pending, excluded, queued, done, error ; `exclusion_reason`, `priority_score` |
| `runs` | un `run` (config figée en JSON, modèle, prompt_hash) |
| `blocks` | une partie DocFuse : chemin du `.md`, tokens, statut built / sent / done / error, tentatives, usage, latence |
| `block_files` | fichiers d'un bloc : `file_ref` exact, statut d'extraction, `oversized` |
| `analyses` | résultat par fichier : 5 domaines à plat (sécurité, RGPD, finance, juridique, **conservation**) + JSON brut + `segments` ; UNIQUE (`file_id`, `content_version`, `prompt_hash`, `model`) |
| `segment_analyses` | analyses par segment d'un fichier découpé (agrégées ensuite) |
| `prompts` | profils de prompt nommés (texte, empreinte, actif) — le prompt est une variable |
| `reviews` | vérification humaine : `to_review` / `validated` / `corrected`, corrections, commentaire, vérificateur |

Migrations : liste ordonnée `_MIGRATIONS` (v1 socle, v2 segments, v3 conservation/prompts/revues, v4 index)
appliquée à l'ouverture ; jamais d'`ALTER` implicite ailleurs. Vues partagées CLI/GUI/rapport : `views.py`.

## 5. Blocs (DocFuse)

1. Sélection des fichiers `pending` (ordre : `priority_score` desc, puis chemin) par lots de
   `batch_files` (défaut 200) existants sur disque (sinon → `error: introuvable`).
2. `run_analysis(input_path=[…], split_context=True, context_limit=block_tokens,
   tokenizer_engine=…, recursive=False)` puis `split_by_budget(result)`.
3. Une partie = un bloc : `write_markdown_corpus(result, path, margin, part=…)` dans
   `work_dir/run_<id>/block_<n>.md` ; `block_files` reçoit `file_ref = result.files[i].relative_path`.
4. Fichiers non extraits (`result.ignored`, statut `error`) → `files.status = error` + raison.
4 bis. **Doublons exacts** (DocFuse remplace le texte du doublon par un renvoi, D-064) : le doublon
   n'est pas envoyé ; le pipeline **copie l'analyse de l'original** (`db.copy_analysis`) — jamais
   « N/A ». Si l'original n'est pas encore analysé, le doublon repasse `pending` et sera analysé
   pour lui-même au run suivant.
5. **Très gros fichiers — ni tronqués, ni en erreur.** Un fichier seul au-delà de
   `blocks.max_file_tokens` (dérivé de `llm.max_context_tokens`, défaut 250 000, aligné sur
   `--max-model-len` = contexte natif du modèle, 262144 pour Qwen3.8 — on ne se bride pas) est découpé en K **segments complets** aux limites de paragraphes,
   un bloc par segment (`## SOURCE: nom [partie i/K]`). Les K analyses vont dans
   `segment_analyses`, puis `llm/aggregate.py` produit l'analyse du fichier (`analyses.segments = K`)
   par règle **conservatrice** : sécurité et RGPD = maximum des segments, finance/juridique = type
   non-`none` le plus sûr, listes = union, résumé « analysé en K parties » + résumés. La sévérité ne
   peut être que sur-estimée. Décision utilisateur du 30/08 : une analyse partielle présentée comme
   complète serait une mauvaise interprétation en puissance — refusée.

## 6. LLM

- Requête : system = `prompts/docia_v3.md` ; user = le bloc `.md` ; `temperature 0` ;
  `max_tokens = 400 × nb_fichiers + 500` (plafonné) ; `response_format = json_schema`.
- Transport `vllm` : `POST {base_url}/chat/completions` (base `http://host:8000/v1`).
- Transport `openwebui` : `POST {base_url}/chat/completions` (base `http://host:8080/api`), le
  bloc passe en `files:[{"type":"text","context":"full","name":"block_001.md","file":{"data":{"content":…}}}]`,
  clé API `Authorization: Bearer sk-…` ; la clé `sources` de la réponse est ignorée.
- Concurrence : `asyncio.Semaphore(max_in_flight)` ; timeouts (connect 10 s, read
  `timeout_s`, défaut 900) ; retries 3 avec backoff sur 5xx / 429 / timeouts, jamais sur 4xx.
- Modèles à raisonnement : `llm.enable_thinking` envoie `chat_template_kwargs.enable_thinking`
  et ajoute `thinking_budget_tokens` à `max_tokens` ; `parse.strip_thinking` ignore un bloc
  `<think>…</think>` resté dans la réponse (serveur sans `--reasoning-parser`).
- `prompt_hash = sha256(prompt + schéma + modèle)[:16]`.

## 7. Validation de la réponse

`parse.py` : `json.loads` → structure (`files` liste, clés requises, énumérations, entiers
0–100) → corrélation `file_ref` : exact, sinon nom de base unique, sinon `error: file_ref inconnu`.
Fichiers du bloc absents de la réponse → `error: absent de la réponse` (retentés une fois dans un
bloc plus petit, puis `error`). Entrées en trop → journalisées, ignorées.

## 8. Leçons du banc du 30/08 (intégrées)

- Pas de `disable_any_whitespace` côté serveur ; `amounts[].value` en `number` ; échelle 0–100
  dite dans le prompt ; `maxItems` sur les tableaux ; schéma sans `pattern`+`maxLength`.
- Blocs de 16–64K : au-delà, MTP et prefix cache se dégradent, et un bloc trop gros = trop de
  fichiers perdus en cas d'échec.

## 9. Étapes

- 4a (ce chantier) : cœur + CLI + tests (serveur OpenAI factice) + test d'intégration contre
  vLLM/open-webui s'ils tournent.
- 4b : GUI CustomTkinter (calquée sur DocFuse), PyInstaller `Docia.spec`, banc réel sur un partage.

## 10. v3.1 — utilisateur et administrateur (30/08, retour utilisateur)

Le cœur (v3.0) analysait sans donner à l'humain les moyens de piloter ni de vérifier. Ajouts :

| Lot | Contenu | Modules |
|---|---|---|
| A — socle | 5ᵉ domaine **`retention`** (`required`, `years`, `basis` ∈ proof/legal/fiscal/rh/contractual/none, `justification`, `confidence`) ; **profils de prompt** en base (`prompts` : nom, texte, hash, date, actif) avec CLI `docia prompt list/show/save/use/reset` ; table **`reviews`** (statut à vérifier/validé/corrigé + commentaire + correction de classe) ; schéma v3 | `llm/schema.py`, `db.py`, `llm/aggregate.py`, `llm/parse.py`, `prompts/`, `cli.py` |
| B — restitution | **vues SQL** partagées (`views.py`) ; `docia report` → HTML autonome + Markdown : classification × partage/répertoire/propriétaire/extension, top sensibles, familles de doublons (FastHash+taille, volume récupérable), âge/taille, erreurs/exclusions ; export **Excel** | `report/`, `cli.py` |
| C — outils | `docia bench` (débit LLM : N blocs synthétiques en parallèle, prefill/decode tok/s, latence, JSON valides, coût du thinking, fichiers/heure estimés) ; `docia quick <fichier|dossier>` (analyse immédiate sans CSV) | `bench.py`, `quick.py`, `cli.py` |
| D — GUI | onglets Source / Prompt (éditeur, compteur de tokens, tester sur un fichier) / Analyse / Résultats & vérification (fiche fichier, statut de revue) / Statistiques / LLM & bench | `gui/` (découpé par onglet) |

Règles : une seule source de vérité (vues SQL) pour CLI, GUI et rapport ; aucune trace Python à l'écran ; tout changement de prompt est visible (l'empreinte fait partie de la clé d'analyse). Thinking activé par défaut (qualité).

## 11. Pilotage à distance, campagnes, sauvegarde (30/08, retour utilisateur) — **fait en v3.2**

- **Couche service** (`service.py`, 18 tests) : toute opération est une fonction typée sans Tk ni
  argparse — `campaign_status`, `import_scan`, `plan`, `run_campaign` (événements `RunEvent` :
  fichiers faits/total, blocs, durée, **temps restant estimé**, fichiers/heure), `reanalyze`
  (`scope` = `errors` | `all` | `pending_only` | `filter` + `where` security/rgpd/owner/extension/
  path_like), `backup_database` / `list_backups` / `restore_database`, `recent_campaigns` /
  `remember_campaign` / `forget_campaign`, `docia_home()` (`$DOCIA_HOME`, `%APPDATA%/docia`,
  `~/.config/docia`). CLI et GUI sont des clients minces (`gui/service_shim.py` ne fait que la
  mise en forme des messages). Étape suivante (v4) : `docia serve` = API REST (FastAPI) exposant
  ces fonctions 1 : 1 ; un serveur web central pilote les briques ; les postes Windows deviennent
  des **agents d'extraction** (DocFuse) qui envoient des blocs et reçoivent des ordres ; base
  centrale PostgreSQL (le SQL des vues est standard).
- **Campagne** = une base SQLite par périmètre (partage, service, date) ; liste des récentes dans
  `recent.json` (20 max, chemins absolus, CSV d'origine). Relancer : *rescan* (nouveau CSV, seuls
  les fichiers modifiés repartent), *nouveau prompt/modèle* (automatique via l'empreinte),
  *réanalyse forcée* (`docia reanalyze --scope errors|all|filter --where security=C3`) précédée
  d'une sauvegarde automatique ; les vérifications humaines (`reviews`) sont conservées.
- **Sauvegarde** : API `sqlite3.Connection.backup` (cohérente pendant un run), fichiers horodatés
  `<db>.backups/<db>_AAAAMMJJ-HHMMSS[_étiquette].sqlite`, écrits en `.tmp` puis publiés par
  `os.replace` — une sauvegarde interrompue (coupure, arrêt du poste) ne laisse pas un fichier
  tronqué que la liste présenterait comme « la plus récente ». Rotation (10 par défaut) sur les
  seules sauvegardes **courantes**, et **seulement celles de la campagne concernée** : le tri
  reconnaît le nom exact de la base, sinon `audit.sqlite` emportait les copies de
  `audit_2024_direction.sqlite`.
- **Copies de sûreté** (`SAFETY_LABEL_PREFIX = "avant_"`) : posées juste avant une opération
  destructrice — `avant_migration_vN`, `avant_restauration`, `avant_reanalyse_*`. Elles sont
  **listées** (on doit pouvoir les restaurer) mais **jamais tournées** : une rotation qui les
  emporte supprime exactement le filet dont on a besoin quand l'opération a mal tourné. Rançon
  assumée : elles s'accumulent et se suppriment à la main.
- **Restauration** : la source est d'abord copiée en `.tmp` à côté de la base, *avant* la copie de
  sûreté `avant_restauration` ; puis `os.replace` (journaux `-wal/-shm` retirés). Ordre non
  négociable — restaurer une sauvegarde *par-dessus elle-même* détruisait la source pendant
  l'étape de sûreté et ne laissait plus rien à restaurer.

## 12. GUI v3.2 — refonte rapide (30/08, « pas beau, pas user friendly »)

Revue en trois personas (utilisateur, administrateur, direction) sur captures : jargon, dataclasses
brutes à l'écran, boutons tronqués, zones vides, aucun parcours. Refonte Tk conservée (« à revoir
plus tard » en web v4), sans logique métier dans `gui/` :

| Écran | Contenu |
|---|---|
| Bandeau | campagne ouverte (Ouvrir… / Récentes / Nouvelle…), interrupteur **mode administrateur** |
| Accueil | 6 tuiles cliquables (fichiers, analysés, sensibles, récupérables, non accédés, vérifiés) ; 4 étapes guidées **Source → Serveur LLM → Analyse → Consulter** ; barre de progression + *écoulé / fichiers/h / restant ≈* (RunEvent) ; **Relancer** (manquants / erreurs / tout, avec confirmation et sauvegarde) ; **Analyse rapide** |
| Résultats | filtres en français, tableau `ttk.Treeview` (1 000 lignes, lignes teintées C3/C2/C1, analysés d'abord du plus sensible au moins), fiche avec **pastilles** (sécurité, RGPD, conservation, vérification), **Valider** / **Corriger…** (champs repliés) / « à vérifier » |
| Statistiques | sous-onglets **Hygiène / Risque / Conservation / Vérification** : tuiles, graphique en barres (`tk.Canvas`), sélecteur de vue, tableau |
| Rapports | HTML, Markdown, Excel, Power BI, CSV, JSON + « Ouvrir le dernier document » ; **Sauvegarde de la base** (dossier, sauvegarder, restaurer, liste) |
| admin | Prompt (profils, test sur un fichier), Serveur & performances (réglages, connexion, bench) |
| pied | dernière ligne du journal + journal complet dépliable |

Modules : `theme.py` (palette de sévérité commune au rapport HTML, libellés FR, formats),
`widgets.py` (KpiTile, Badge, Card, BarChart, `Table` — qui porte l'**identité** de ses lignes,
`sort_rows` pure —, ReadOnlyText), `helpers.py` (fonctions pures testées : avancement, titre de
campagne, mise en forme des valeurs), `lazy.py` (`LazyScreen` : le patron *jeton + campagne
capturée + `_dirty` remis dans `apply`* que partagent Accueil, Résultats et Statistiques, testable
sans écran), `service_shim.py` (toute **écriture** de campagne, aucun Tk), `dialogs.py` (ce qui a
besoin de Tk *et* de la CLI : produire un document). Thème clair, police 13.

## 13. Budget de raisonnement (banc du 30/08, soir — « il ne faut pas regarder le souci de budget de thinking ? »)

**Constat.** docia *réservait* 12 000 tokens de raisonnement dans `max_tokens` sans rien imposer :
le modèle pouvait tout dépenser en `<think>` et tronquer le JSON (contourné par un renvoi à budget
doublé). Le gabarit Qwen3.8 met `reasoning_effort` à **`xhigh` par défaut** ; vLLM (build local du
06/08, `--reasoning-parser qwen3`) accepte `thinking_token_budget` par requête et force `</think>`
au-delà (sonde : budget 40 → 40 tokens de raisonnement, réponse quand même rendue).

**Mesures** (`scratchpad/think/`, 8 blocs réels de 1,9 K à 84 K tokens de prompt, 1 à 8 fichiers,
`max_tokens` 40 000 = non contraint) :

| réglage | raisonnement (tokens) | JSON valides | temps (bloc 17 K, 8 fichiers) | classes |
|---|---|---|---|---|
| low | 300 – 1 900 | 8/8 | 107 s | sur-classe (bulletins de paie **C3**) |
| medium | 500 – 4 400 | 8/8 | 237 s* / 106 s | C2/high, stable |
| xhigh (sans budget) | 1 500 – **18 000** | **6/8** — 2 réponses **vides** après `</think>` (`finish=stop`, 1 token) | 345 s | C2/high |
| xhigh + budget 6 000 | 5 999 (saturé 5×/6) | 6/6 | 190 s | idem medium ± 1 classe |
| medium + budget 2 000 | ≤ 1 999 | 6/6 | 106 s | identiques à medium 6 000 |

\* mesure bruitée (4 requêtes en vol dont un prefill de 84 K).

**Décisions.** `reasoning_effort = "medium"` ; `thinking_budget_tokens = 6 000`, envoyé comme
`thinking_token_budget` (transport vLLM) **et** réservé dans `max_tokens` ; `xhigh` reste
disponible (onglet Serveur) mais n'est sûr qu'avec le budget imposé, pour 2–3× le temps et sans
gain de classification observé. Le renvoi à budget doublé sur `finish_reason=length` est conservé
comme filet. `docia bench` mesure désormais le raisonnement (`reasoning_content`). Voir aussi §15 (comptage exact avant envoi).

**Corollaire corrigé.** `llm.max_context_tokens` = longueur servie (`--max-model-len`, 262 144) ;
le pipeline réserve en dessous `2 000 + 2 × (plancher + quota/fichier + budget de raisonnement)`
pour que le segment le plus gros laisse la place de la réponse même doublée ; le client borne
`max_tokens` à la place restante (`clamp_to_context`). Avant : segment ≤ 244 000 + sortie 27 000
= 271 000 > 262 144 → vLLM aurait refusé la requête (400).


## 14. Étape 0 — scanner piloté par docia (30/08, décision utilisateur)

Pas de GUI séparée pour smbeagle : **docia pilote `SMBeagle.exe` en sous-processus** (`scan.py`,
sans Tk ni argparse ; `service.scan_campaign` = scanner → import → plan ; CLI `docia scan` ; Accueil
« 1 · Source » à deux modes : scanner maintenant / importer un CSV). Le scanner est trouvé via
`scan.smbeagle_path`, puis à côté de l'exécutable docia, puis le PATH. Contrat avec
smbeagle_enriched (implémenté de son côté) : `--progress-json` (lignes `{"event":"progress",
"stage","hosts","shares","files","elapsed_s"}` puis `done`/`error`), `--manifest chemin.json`
(options, cibles, compteurs, colonnes), codes de retour 0/1/2/3, `--preserve-access-time`.
Sans progression JSON, docia relaie les lignes texte et compte les lignes du CSV.

**Rescan** (« attention si on relance une analyse ») : `upsert_files` ne remet en analyse que les
fichiers dont empreinte/taille/date de modification changent (`content_version + 1`) ; les
vérifications humaines restent ; schéma **v5** ajoute `files.access_time_first` (première date
d'accès observée, réinitialisée seulement quand le contenu change) et `scans.kind/manifest_json/
scanner_elapsed_s`. Les vues « ancienneté » et « candidats au nettoyage » utilisent
`COALESCE(access_time_first, access_time)` : l'audit (hachage, signature, extraction) ne rajeunit
pas les fichiers inchangés.

**Statistiques sur gros parc** (« l'onglet Statistiques semble figé ») : schéma **v6**. Les vues
d'ancienneté reformataient les dates ligne par ligne (`CASE … substr …`) — aucun index utilisable,
un balayage complet par seuil. Les dates sont désormais normalisées à l'écriture en clés
`yyyymmdd` (`files.access_key` = `COALESCE(access_time_first, access_time)`, `files.write_key` =
`last_write_time`), indexées avec la taille (index couvrants) ; `stale_files` cumule des
histogrammes issus d'une seule requête. Les vues qui croisent fichiers et analyses partent
d'`analyses` (une minorité des fichiers est analysée) au lieu de balayer `files`, la matrice de
classification n'agrège en SQL que l'axe demandé, et `overview` ne demande que des totaux au lieu
de reconstruire chaque vue détaillée. `ANALYZE` à la fin de chaque scan donne au planificateur les
cardinalités réelles. Sur 200 000 fichiers / 40 000 analyses : 26,5 s → 2,4 s pour l'onglet complet.

**Import d'un gros CSV** (« l'intégration dure très très longtemps sur un fichier de 250 Mo ») :
la lecture du CSV n'y était pour rien (10,6 s pour 934 028 lignes) ; tout le temps partait dans la
maintenance des onze index secondaires de `files`, ligne à ligne. `Database.bulk_load()` relit leurs
définitions dans `sqlite_master`, les supprime le temps du chargement, élargit `cache_size` et
`temp_store`, puis les recrée d'un bloc (l'index UNIQUE implicite de `path_key` n'est jamais touché :
c'est lui qui rend l'upsert immédiat). Les mises à jour « fichier inchangé » d'un rescan passent en
`executemany`. Sur le CSV de 252 Mo : **168,5 s → 53,4 s** à l'import, 48,6 s → 41,0 s au rescan, base
identique au bit près (hachage des lignes triées) et compteurs inchangés. Un import tué en plein vol
laisserait la base sans index : `Database` vérifie à chaque ouverture que les index de `FILES_INDEXES`
sont là et reconstruit ceux qui manquent (une lecture de `sqlite_master`, ~1 ms). `import_csv` accepte
enfin un rappel `progress` (lignes lues, pourcentage estimé par les octets lus) que la CLI et la
fenêtre affichent toutes les 2 s ou 50 000 lignes : « intégration en cours » ne reste plus muet
pendant des minutes.

**Exe Windows complet** (« attention à ne pas oublier les paquets ») : les extracteurs DocFuse
importent leurs bibliothèques **paresseusement** (pypdf, pdfminer, pypdfium2, python-docx, python-pptx,
openpyxl, lxml, bs4, striprtf, ftfy, oxmsg, office_oxide…) — invisibles pour PyInstaller → l'exe
démarrait puis plantait au premier .docx/.pdf. `Docia.spec` les ratisse (`collect_all`, données et
binaires natifs compris) et la CI Windows exécute l'exe sur les fixtures DocFuse (`quick --dry-run`)
et ouvre/ferme la fenêtre (`gui --smoke`, onglets admin compris) : un paquet manquant casse la CI,
pas le poste de l'utilisateur.

**OCR embarqué (30/08, correction demandée par l'utilisateur : « sinon tu ne peux pas classer
les documents proprement »)** : `Docia.spec` embarque Tesseract comme `DocFuse-OCR.spec`
(`tesseract/tesseract.exe` + `tesseract/tessdata/`, chemin attendu par DocFuse dans `_MEIPASS`,
`TESSDATA_PREFIX` posé par DocFuse) ; le build échoue si `TESSERACT_HOME` ou `fra.traineddata`
manquent (`DOCIA_NO_OCR=1` = build local explicitement sans OCR). L'OCR est automatique côté
DocFuse (pages classées ocr/mixed, moteur résolu s'il est disponible). La CI génère un PDF
image-only (`scripts/make_scanned_pdf.py`), retire Tesseract du PATH et exige que le bloc
produit par `Docia.exe quick --dry-run` contienne le texte reconnu.

## 15. Test complet local et comptage exact des tokens (30/08, nuit)

`scripts/e2e_local.sh` (+ `scripts/e2e_check.py`, 28 contrôles) rejoue toute la chaîne sur cette
machine : build du scanner, corpus (fixtures DocFuse + **PDF scanné généré** + doublon exact + gros
fichier), vLLM démarré puis arrêté, `docia scan` → `run` → rapport HTML / Excel / Power BI, puis
vérifications en base : OCR relu par la LLM (bulletin scanné → paie, RGPD élevé, conservation RH),
doublon hérité, OLE hachés, segments agrégés, 0 fichier et 0 bloc en erreur, documents produits.
Premier passage : 25/27 — le gros fichier échouait : `approx` (octets/4) estimait 202 388 tokens là
où le tokenizer réel en comptait 266 402 (> 262 144) → vLLM 400 sur chaque segment.

**Correctifs** : `blocks.tokenizer_engine = "openai"` (o200k, embarqué) et coefficient de sécurité
des segments (`SEGMENT_SAFETY` : 0,85 o200k/tekken, 0,6 approx) ; surtout, **comptage exact avant
envoi** : `LLMClient.check_fits` interroge vLLM `/tokenize` (transport vllm) et lève
`BlockTooLongError` (réel, place, ratio) sans envoyer ; le pipeline mémorise le ratio réel/estimé
par fichier, remet le fichier `pending` **sans consommer de tentative**, puis lance une **seconde
passe** dans le même run avec `max_file_tokens / ratio × 0,9` (`RunReport.files_resplit`).
Testé avec le faux serveur (`tokens_per_char` = 0,5 : segments refusés puis re-découpés, fichier
`done`, 0 erreur).

## 16. Charge : borner la mémoire, pas la campagne (31/08, premier audit réel)

Le premier audit sur un vrai serveur Windows a montré un défaut de **classe**, invisible sur les
jeux de test : plusieurs opérations chargeaient toute la campagne en mémoire Python. Profilé sur
une campagne réaliste de **934 028 fichiers** (CSV SMBeagle de 252 Mo, base de 1,05 Go) :

| opération | avant | après |
|---|---|---|
| `run` (sélection des fichiers à analyser) | 13,0 s — **1 721 Mo**, retenus pendant tout le run | 2,7 s — **45 Mo** |
| `export --format xlsx` | 333 s — **11 741 Mo** | 230 s — **205 Mo** |
| `export --format json` | 48 s — **6 935 Mo** | 45 s — **22 Mo** |
| `export --format powerbi` | 39 s — **2 691 Mo** | 36 s — **127 Mo** |
| `export --format csv` | 27 s — **1 329 Mo** | 26 s — **22 Mo** |
| `plan` | 59 s — **1 069 Mo** | 47 s — **68 Mo** |
| onglet Résultats (chargement) | 14,6 s — **1 129 Mo** | 4,1 s — **68 Mo** |
| onglet Résultats (100 validations) | ~24 min | **1,5 s** |
| import du CSV de 252 Mo | 168 s | **53 s** |
| vues statistiques (200 000 fichiers) | 26,5 s | **2,4 s** |

**Règle de conception qui en découle** : *toute opération de docia doit être bornée en mémoire,
quelle que soit la taille de la campagne*. Concrètement — parcourir les curseurs SQLite en flux,
écrire par lots (`executemany`), descendre filtres, tri et limite dans SQL, ne jamais faire
`list(...)` sur une table entière, et matérialiser des identifiants (8 octets) plutôt que des
lignes complètes quand seule l'itération compte. Avant de déclarer une opération « rapide », la
profiler avec `/usr/bin/time -v` sur une campagne de cet ordre, pas sur les fixtures : le serveur
cible a 8 à 16 Go partagés avec d'autres services, et 12 Go demandés le font tomber.

Mécanismes ajoutés : `Database.bulk_load()` (index secondaires retirés le temps d'un import, avec
marqueur en base pour qu'une seconde connexion ne les reconstruise pas au milieu), colonnes de
dates matérialisées et indexées (schéma v6), `select_pending_ids` / `files_by_ids`, filtres SQL de
l'onglet Résultats, classeur Excel en écriture seule (avec troncature explicite à la limite d'un
million de lignes d'Excel, qui renvoie vers `powerbi` ou `csv`).

Corollaire de robustesse traité dans le même lot : les migrations de schéma sont désormais
**atomiques** (jouées instruction par instruction dans une vraie transaction — `executescript()`
validait implicitement et laissait la base à moitié migrée, donc inouvrable, après une coupure),
et leur sauvegarde préalable est horodatée et passe par l'API `sqlite3.backup`, qui inclut le WAL.
