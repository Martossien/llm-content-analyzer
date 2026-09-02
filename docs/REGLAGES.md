# Référence des réglages (`docia.toml` et onglet « Serveur & performances »)

Chaque réglage a été vérifié dans le code (30/08) : la colonne « où il agit » cite le module qui
l'applique — il n'y a **aucun réglage mort**. Trois natures :

- **requête** : envoyé au serveur LLM à chaque requête — il pilote, quel que soit le script de
  lancement du serveur ;
- **local** : n'agit que sur le poste (extraction, découpage, fichiers) ;
- **descriptif** : décrit le serveur, doit correspondre à son lancement.

Les valeurs par défaut viennent des bancs du 30/08 (voir `DESIGN_V3.md` §13–§15) : un utilisateur
n'a rien à changer ; l'administrateur règle une fois l'adresse, le modèle et la clé.

## Racine

| Clé | Défaut | Nature | Effet — où il agit |
|---|---|---|---|
| `db_path` | `docia.sqlite` | local | le fichier **campagne** (base SQLite) ; tout est rangé à côté (`.blocks/`, `.scans/`, `.backups/`) — partout |
| `prompt_path` | vide | local | prompt système lu depuis un fichier ; vide = prompt embarqué ou profil actif en base (`pipeline.resolve_system_prompt`) |

## `[llm]` — onglet Serveur & performances

| Clé (libellé interface) | Défaut | Nature | Effet — où il agit |
|---|---|---|---|
| `transport` (Transport) | `vllm` | descriptif | `vllm` = API OpenAI directe ; `openwebui` = API native open-webui (fichier joint, clé de compte ; le budget de raisonnement n'y est **pas** relayé) — `llm/client.py` |
| `base_url` (URL de base) | `http://127.0.0.1:8000/v1` | descriptif | adresse du serveur LLM — en production, une **autre machine** : `http://serveur:8000/v1` ; seul flux réseau, texte uniquement |
| `api_key` (Clé API) | vide | descriptif | envoyée en `Authorization: Bearer` (`resolved_api_key`) : clé `sk-…` d'un compte open-webui, ou vide en vLLM sans `--api-key` (repli : variable `DOCIA_API_KEY`, puis `dummy`) |
| `model` (Modèle) | `qwen38` | descriptif | doit être le `--served-model-name` du serveur |
| `max_in_flight` (Requêtes en vol) | 8 | requête | blocs envoyés **en même temps** (sémaphore du client) ; 8 nourrit le GPU en continu, au-delà on allonge la file. En mode adaptatif : le **plafond** en nombre de requêtes |
| `adaptive` (Alimentation adaptative) | faux | local | régule les **tokens en vol** au lieu d'envoyer `max_in_flight` blocs quoi qu'il arrive (`llm/pacer.py`) : montée par crans de +25 % tant que le débit ne baisse pas, retour au cran d'avant et palier quand il baisse, descente si le serveur ralentit (autre usage du GPU), sonde de moins en moins fréquente en palier, budget divisé par deux sur détresse (coupure/délai après les tentatives, **préemptions vLLM** lues dans `/metrics` toutes les 30 s). Le débit (tokens de prompt rendus/s) est mesuré par fenêtres de 5 blocs **durant au moins deux latences médianes**, comparé à une moyenne glissante, et une fenêtre où le budget n'a pas contraint (file vide, plafond de requêtes atteint avant les tokens) est écartée — mesure du 02/09 sur 4×3090 : le débit brut d'une fenêtre allait de 579 à 3 797 tok/s selon le contenu des blocs, et sans ces garde-fous le budget dérivait vers le bas en fin de campagne. Le budget trouvé est mémorisé par serveur + modèle (`pacer.json` dans le dossier de configuration) et sert de départ au run suivant. À préférer dès que le serveur est partagé ou change (5090 NVFP4+MTP vs 3090 FP8) ; le mode fixe reste le défaut, reproductible. Sur 4×3090 FP8, 8 segments de 73 K n'occupent que 25 % du cache KV : c'est `max_in_flight` qui borne, le budget monte au plafond — on peut alors monter `max_in_flight` à 12–16 et laisser le régulateur trouver la limite |
| `adaptive_start_tokens` | 0 | local | budget de départ du mode adaptatif ; 0 = mémorisé, sinon la moitié du plafond (`max_in_flight` × plafond par fichier). Plancher : un segment entier — partir plus bas sérialise tout (mesuré le 02/09 : vLLM à 1 requête en vol, génération 55 tok/s) |
| `timeout_s` (Timeout) | 900 | local | patience de lecture d'un bloc **ordinaire** ; un gros bloc obtient en plus `tokens / 200` s (un segment de 200 K tokens : +1 000 s), pour ne pas couper puis renvoyer ce que le serveur sert lentement |
| `max_retries` | 3 | local | tentatives sur erreur réseau/5xx, backoff exponentiel (`_analyze`) |
| `temperature` | 0.0 | requête | 0 = réponses reproductibles, voulu pour de la classification |
| `max_tokens_floor` / `max_tokens_per_file` | 800 / 700 | requête | budget de **réponse** : plancher + quota par fichier du bloc (5 domaines + justifications) — `max_tokens_for` |
| `max_tokens_cap` | 32 000 | requête | plafond du budget de réponse, quel que soit le nombre de fichiers |
| `max_context_tokens` (Contexte du modèle) | 262 144 | descriptif **auto-vérifié** | doit égaler `--max-model-len` ; docia lit la valeur servie (`GET /v1/models`) au début du run, avertit et **se borne** à elle ; borne aussi `max_tokens` par bloc (`clamp_to_context`) |
| `enable_thinking` (Raisonnement) | vrai | requête | active le raisonnement (`chat_template_kwargs`) — qualité mesurée supérieure |
| `thinking_budget_tokens` (Budget de raisonnement) | 6 000 | requête | **imposé** à vLLM (`thinking_token_budget` : coupe le `<think>` net) ET réservé dans `max_tokens` — la réponse JSON a toujours sa place |
| `reasoning_effort` (Effort de raisonnement) | `medium` | requête | `low` sur-classe, `medium` = même qualité que `xhigh` en 2–3× moins de temps, `xhigh` sûr seulement grâce au budget imposé |

## `[blocks]` — découpage sur le poste

| Clé (libellé) | Défaut | Nature | Effet — où il agit |
|---|---|---|---|
| `block_tokens` (Tokens par bloc) | 32 000 | local | taille visée d'un bloc multi-fichiers : moins de requêtes vs échecs isolés (16–64 K raisonnable) — `blocks/builder.py` |
| `margin` | 0.15 | local | marge ajoutée à l'estimation de tokens avant de remplir un bloc |
| `tokenizer_engine` (Comptage des tokens) | `openai` | local | moteur d'**estimation** : `openai` (o200k, précis, embarqué), `mistral`, `approx` (octets/4, sous-estime Qwen de ~30 %) ; le comptage **exact** est de toute façon fait par le serveur avant envoi |
| `batch_files` (Fichiers par lot DocFuse) | 200 | local | rythme de l'extraction : fichiers passés à DocFuse par appel — **ne borne pas la mémoire** |
| `batch_bytes` (Mémoire par lot DocFuse) | 64 Mio | local | **plafond mémoire** : le lot se ferme dès que le cumul des tailles source dépasse ce budget, même si `batch_files` n'est pas atteint (`blocks/builder.py:split_by_bytes`). Mesure du 01/09 sur 600 Mo de fichiers : pic de 2 135 Mo sans budget, **360 Mo** avec 64 Mio, à durée identique — le pic vaut ≈ 5× le budget. Un fichier plus gros que le budget est traité **seul**, jamais écarté. `0` = aucun plafond (déconseillé). Dans l'interface, le champ est en **mégaoctets**. |
| `work_dir` | vide | local | dossier des blocs `.md` ; vide = `<campagne>.blocks/` |
| `max_file_share` (Part du contexte par fichier) | 0.3 | local | part du contexte servi (moins la réserve prompt + réponse) qu'un fichier seul — et donc une requête — peut occuper avant découpage en segments (`pipeline.file_cap`). 0,3 sur 262 K ≈ 73 K tokens : au-dessus du plus gros document bureautique courant, en dessous de ce qui écroule le débit (préremplissage superlinéaire, cache KV d'une requête de 200 K ≈ 26 Go qui évince les autres). Borne aussi `block_tokens`. 1,0 = comportement d'avant le 02/09 |
| `max_file_tokens` | 0 (auto) | local | seuil **estimé** de candidature au découpage ; 0 = `max_file_share` × contexte disponible × sécurité du moteur (`pipeline.segment_budget`). En vLLM, c'est le **compte exact du serveur** (`/tokenize`) qui tranche ensuite entre « entier » et « découpé » (`blocks/policy.py`) : un fichier estimé trop long mais qui tient part entier, un fichier trop long est découpé en segments calibrés sur le rapport estimation/réel mesuré. Une valeur explicite vaut aussi plafond exact |
| `keep_blocks` | vrai | local | conserve les `.md` après le run (diagnostic, reprise). **Ces `.md` contiennent le texte intégral des documents analysés, en clair** (voir le guide, §8 « Où est le texte des documents »). `false` = chaque bloc est effacé dès qu'il est traité. |

## `[filter]` — ce qui n'est pas analysé (exclu par règle, jamais par oubli)

| Clé | Défaut | Effet — où il agit |
|---|---|---|
| `excluded_extensions` | ~35 ext. (images, médias, archives, binaires, bases…) | exclusion avec raison visible — `filter.exclusion_reason` |
| `min_size_bytes` / `max_size_bytes` | 100 o / 100 Mo | trop petit pour avoir du sens / trop gros pour le poste |
| `excluded_dir_markers` | dossiers système (`$RECYCLE.BIN`, `System Volume Information`, `AppData`…) | exclusion par segment de chemin |

Le **score de priorité** (type, taille, âge, mots-clés du nom) ordonne ce qui reste : les fichiers
les plus probablement sensibles sont analysés d'abord.

## `[scan]` — scanner SMBeagle piloté

| Clé (libellé) | Défaut | Effet — où il agit |
|---|---|---|
| `smbeagle_path` (Scanner SMBeagle) | vide | vide = `SMBeagle.exe` à côté de `Docia.exe`, puis PATH (`scan.find_smbeagle`) |
| `preserve_access_time` (Dates d'accès) | vrai | `--preserve-access-time` : le scan ne « rajeunit » pas les dates de dernier accès (droit d'écrire les attributs requis ; sinon signalé, non bloquant — docia garde de toute façon la première date observée) |
| `skip_acls` | faux | `-A` : plus rapide, sans colonnes lecture/écriture |
| `exclude_hidden_shares` | vrai | ignore `C$`, `ADMIN$`… en mode SMB |
| `domain` / `username` | vides | compte SMB explicite (mode SMB seulement) ; le **mot de passe n'est jamais écrit** dans `docia.toml` : variable `DOCIA_SMB_PASSWORD` |

## Ce qui n'est PAS ici (serveur seulement)

GPU, parallélisme (TP/PP), KV cache fp8, backend `xgrammar`, parser de raisonnement : ces choix
appartiennent au script de lancement du serveur (`bench_vllm/serve_qwen38.sh`) — l'exe n'a pas à
les connaître.
