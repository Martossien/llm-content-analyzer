import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Iterator
import re
import yaml
import pandas as pd

logger = logging.getLogger(__name__)

class SMBeagleCSVParser:
    """Parser spécialisé pour les CSV SMBeagle avec guillemets sélectifs."""

    QUOTED_COLUMNS = {0, 1, 2, 3, 4, 5, 12, 15, 16, 17, 18}
    UNQUOTED_COLUMNS = {6, 7, 8, 9, 10, 11, 13, 14}

    @staticmethod
    def parse_csv_line(line: str) -> List[str]:
        """Parse une ligne CSV en tenant compte des guillemets sélectifs."""
        if not line.strip():
            return []

        fields: List[str] = []
        current = ""
        in_quotes = False
        i = 0

        while i < len(line):
            char = line[i]
            
            if char == '"':
                if in_quotes:
                    # Vérifier si c'est un guillemet échappé ""
                    if i + 1 < len(line) and line[i + 1] == '"':
                        current += '"'
                        i += 1  # Skip le prochain guillemet
                    else:
                        # Fin des guillemets
                        in_quotes = False
                else:
                    # Début des guillemets
                    in_quotes = True
            elif char == "," and not in_quotes:
                # Séparateur trouvé en dehors des guillemets
                fields.append(current)
                current = ""
            else:
                current += char
            
            i += 1

        # Ajouter le dernier champ
        fields.append(current)
        return fields

    @staticmethod
    def validate_csv_line_format(line: str, line_number: int) -> List[str]:
        """Valide le format d'une ligne CSV en vérifiant les guillemets sur la ligne brute."""
        errors: List[str] = []
        
        # Parse la ligne pour obtenir les champs
        fields = SMBeagleCSVParser.parse_csv_line(line)
        
        if len(fields) != 19:
            errors.append(f"Line {line_number}: {len(fields)} fields instead of 19")
            return errors
        
        # Maintenant, re-parse manuellement pour vérifier les guillemets sur la ligne originale
        current_pos = 0
        field_index = 0
        
        for field in fields:
            # Trouver le début du champ dans la ligne originale
            while current_pos < len(line) and line[current_pos] == ' ':
                current_pos += 1
                
            if current_pos >= len(line):
                break
                
            # Vérifier si le champ commence par des guillemets
            starts_with_quote = line[current_pos] == '"'
            
            # Vérifier selon les règles
            if field_index in SMBeagleCSVParser.QUOTED_COLUMNS:
                if not starts_with_quote:
                    errors.append(f"Line {line_number}, column {field_index}: should have quotes")
            else:
                if starts_with_quote:
                    errors.append(f"Line {line_number}, column {field_index}: should not have quotes")
            
            # Avancer la position après ce champ
            if starts_with_quote:
                # Sauter jusqu'au guillemet fermant
                current_pos += 1  # Sauter le guillemet ouvrant
                while current_pos < len(line):
                    if line[current_pos] == '"':
                        # Vérifier si c'est un guillemet échappé
                        if current_pos + 1 < len(line) and line[current_pos + 1] == '"':
                            current_pos += 2  # Sauter les guillemets échappés
                        else:
                            current_pos += 1  # Sauter le guillemet fermant
                            break
                    else:
                        current_pos += 1
            else:
                # Sauter jusqu'à la virgule suivante
                while current_pos < len(line) and line[current_pos] != ',':
                    current_pos += 1
            
            # Sauter la virgule
            if current_pos < len(line) and line[current_pos] == ',':
                current_pos += 1
                
            field_index += 1
            
        return errors

    @classmethod
    def clean_field_value(cls, field_value: str, field_index: int) -> str:
        """Nettoie une valeur de champ selon les règles SMBeagle."""
        value = field_value

        if field_index in cls.QUOTED_COLUMNS:
            # Enlever les guillemets si présents
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1].replace('""', '"')
            else:
                logger.warning(
                    "Colonne %s devrait avoir des guillemets: %s",
                    field_index,
                    value,
                )
        else:
            # Vérifier qu'il n'y a pas de guillemets
            if value.startswith('"') and value.endswith('"'):
                logger.warning(
                    "Colonne %s ne devrait pas avoir de guillemets: %s",
                    field_index,
                    value,
                )
                value = value[1:-1]

        return value.strip()


def parse_csv_with_smbeagle_format(
    csv_file: Path, chunk_size: int = 10000
) -> Iterator[List[Dict[str, Any]]]:
    """Parse un CSV SMBeagle en respectant les guillemets sélectifs."""
    headers = [
        "Name",
        "Host", 
        "Extension",
        "Username",
        "Hostname",
        "UNCDirectory",
        "CreationTime",
        "LastWriteTime",
        "Readable",
        "Writeable",
        "Deletable",
        "DirectoryType",
        "Base",
        "FileSize",
        "AccessTime",
        "FileAttributes",
        "Owner",
        "FastHash",
        "FileSignature",
    ]

    parser = SMBeagleCSVParser()
    
    with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
        # Sauter l'en-tête
        header_line = f.readline()
        
        batch: List[Dict[str, Any]] = []
        
        for line_num, line in enumerate(f, start=2):
            line = line.strip()
            if not line:  # Ignorer les lignes vides
                continue
                
            fields = parser.parse_csv_line(line)
            
            if len(fields) != 19:
                logger.warning(
                    "Ligne %s: %s colonnes au lieu de 19", line_num, len(fields)
                )
                continue

            # Nettoyer les champs selon les règles
            cleaned = [
                parser.clean_field_value(field, idx) for idx, field in enumerate(fields)
            ]

            row_dict = dict(zip(headers, cleaned))
            batch.append(row_dict)

            if len(batch) >= chunk_size:
                yield batch
                batch = []

        if batch:
            yield batch


class CSVParser:
    """Parse les fichiers CSV SMBeagle vers SQLite."""

    def __init__(self, config_path: Path) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        
        module_cfg = cfg.get("modules", {}).get("csv_parser", {})
        self.chunk_size = module_cfg.get("chunk_size", 10000)
        self.validation_strict = module_cfg.get("validation_strict", True)
        self.encoding = module_cfg.get("encoding", "utf-8")
        
        self.required_columns = [
            "Name",
            "Host",
            "Extension", 
            "Username",
            "Hostname",
            "UNCDirectory",
            "CreationTime",
            "LastWriteTime",
            "Readable",
            "Writeable",
            "Deletable",
            "DirectoryType",
            "Base",
            "FileSize",
            "Owner",
            "FastHash",
            "AccessTime",
            "FileAttributes",
            "FileSignature",
        ]

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        """Create or migrate the fichiers table to store all SMBeagle columns."""
        expected_columns = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "host": "TEXT",
            "extension": "TEXT",
            "username": "TEXT",
            "hostname": "TEXT",
            "unc_directory": "TEXT",
            "creation_time": "TEXT",
            "last_write_time": "TEXT",
            "readable": "BOOLEAN",
            "writeable": "BOOLEAN",
            "deletable": "BOOLEAN",
            "directory_type": "TEXT",
            "base": "TEXT",
            "path": "TEXT UNIQUE NOT NULL",
            "file_size": "INTEGER NOT NULL",
            "owner": "TEXT",
            "fast_hash": "TEXT",
            "access_time": "TEXT",
            "file_attributes": "TEXT",
            "file_signature": "TEXT",
            "last_modified": "TEXT NOT NULL",
            "status": "TEXT DEFAULT 'pending'",
            "exclusion_reason": "TEXT",
            "priority_score": "INTEGER DEFAULT 0",
            "special_flags": "TEXT",
            "processed_at": "TIMESTAMP",
        }

        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS fichiers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                host TEXT,
                extension TEXT,
                username TEXT,
                hostname TEXT,
                unc_directory TEXT,
                creation_time TEXT,
                last_write_time TEXT,
                readable BOOLEAN,
                writeable BOOLEAN,
                deletable BOOLEAN,
                directory_type TEXT,
                base TEXT,
                path TEXT UNIQUE NOT NULL,
                file_size INTEGER NOT NULL,
                owner TEXT,
                fast_hash TEXT,
                access_time TEXT,
                file_attributes TEXT,
                file_signature TEXT,
                last_modified TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                exclusion_reason TEXT,
                priority_score INTEGER DEFAULT 0,
                special_flags TEXT,
                processed_at TIMESTAMP
            )
            """
        )

        # Migration for existing databases: add missing columns
        cursor.execute("PRAGMA table_info(fichiers)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        
        for col, col_type in expected_columns.items():
            if col not in existing_cols:
                cursor.execute(f"ALTER TABLE fichiers ADD COLUMN {col} {col_type}")

        # Indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON fichiers(status)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_fast_hash ON fichiers(fast_hash)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_priority_score ON fichiers(priority_score DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_extension ON fichiers(extension)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_host ON fichiers(host)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_username ON fichiers(username)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_name ON fichiers(name)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_unc_directory ON fichiers(unc_directory)"
        )

        conn.commit()

    def validate_csv_format(self, csv_file: Path) -> List[str]:
        """Valide le format CSV SMBeagle en vérifiant l'en-tête et quelques lignes."""
        errors: List[str] = []
        
        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                # Vérifier l'en-tête
                header_line = f.readline().strip()
                actual_headers = [c.strip().strip('"') for c in header_line.split(",")]

                if len(actual_headers) != 19:
                    errors.append(f"Expected 19 columns, found {len(actual_headers)}")

                missing = set(self.required_columns) - set(actual_headers)
                if missing:
                    errors.append(f"Missing columns: {missing}")

                # Vérifier les premières lignes de données
                for i, line in enumerate(f):
                    if i >= 5:  # Limiter à 5 lignes pour la validation
                        break
                        
                    line = line.strip()
                    if not line:  # Ignorer les lignes vides
                        continue
                        
                    # Valider le format de cette ligne
                    line_errors = SMBeagleCSVParser.validate_csv_line_format(line, i + 2)
                    errors.extend(line_errors)

        except Exception as exc:
            errors.append(f"File reading error: {exc}")

        return errors

    def transform_metadata_from_dict(self, row_dict: Dict[str, str]) -> Dict[str, Any]:
        """Transforme un dict issu du parser SMBeagle en données prêtes pour SQLite."""
        unc_dir = row_dict.get("UNCDirectory", "").strip()
        name = row_dict.get("Name", "").strip()

        # Construire le path
        if unc_dir.endswith("\\") or unc_dir.endswith("/"):
            path = f"{unc_dir}{name}"
        else:
            sep = "\\" if "\\" in unc_dir else "/"
            path = f"{unc_dir}{sep}{name}"

        if len(path) > 32767:
            logger.warning("Path très long tronqué: %s caractères", len(path))
            path = path[:32767]

        def safe_bool(value: str) -> bool:
            return str(value).strip().lower() == "true"

        try:
            file_size = int(row_dict.get("FileSize", "0") or "0")
        except ValueError:
            logger.warning("Invalid FileSize: %s", row_dict.get("FileSize"))
            file_size = 0

        return {
            "name": name[:255],
            "host": row_dict.get("Host", "").strip(),
            "extension": (
                ("." + row_dict.get("Extension", "").strip().lower().lstrip("."))
                if row_dict.get("Extension") else ""
            ),
            "username": row_dict.get("Username", "").strip(),
            "hostname": row_dict.get("Hostname", "").strip(),
            "unc_directory": unc_dir,
            "creation_time": row_dict.get("CreationTime", "").strip(),
            "last_write_time": row_dict.get("LastWriteTime", "").strip(),
            "readable": safe_bool(row_dict.get("Readable", "False")),
            "writeable": safe_bool(row_dict.get("Writeable", "False")),
            "deletable": safe_bool(row_dict.get("Deletable", "False")),
            "directory_type": row_dict.get("DirectoryType", "").strip(),
            "base": row_dict.get("Base", "").strip(),
            "path": path,
            "file_size": file_size,
            "owner": row_dict.get("Owner", "").strip(),
            "fast_hash": row_dict.get("FastHash", "").strip(),
            "access_time": row_dict.get("AccessTime", "").strip(),
            "file_attributes": row_dict.get("FileAttributes", "").strip(),
            "file_signature": row_dict.get("FileSignature", "").strip(),
            "last_modified": row_dict.get("LastWriteTime", "")
            or row_dict.get("CreationTime", ""),
        }

    def transform_metadata(self, row: "pd.Series") -> Dict[str, Any]:
        """Compatibilité ancienne API utilisant pandas."""
        return self.transform_metadata_from_dict(row.to_dict())

    def parse_csv(
        self,
        csv_file: Path,
        db_file: Path,
        chunk_size: int = 10000,
    ) -> Dict[str, Any]:
        """Parse le CSV et importe dans SQLite sans altérer les chemins."""
        chunk = chunk_size or self.chunk_size
        start = time.perf_counter()
        total_files = 0
        imported_files = 0
        errors: List[str] = []
        validation_stats = {"invalid_rows": 0}

        conn = sqlite3.connect(db_file)
        self._ensure_schema(conn)

        try:
            # Validation du format si requise
            validation_errors = self.validate_csv_format(csv_file)
            if validation_errors and self.validation_strict:
                conn.close()
                return {
                    "total_files": 0,
                    "imported_files": 0,
                    "errors": validation_errors,
                    "processing_time": time.perf_counter() - start,
                    "validation_stats": validation_stats,
                }

            # Import des données
            for batch in parse_csv_with_smbeagle_format(csv_file, chunk):
                total_files += len(batch)
                
                for row_dict in batch:
                    try:
                        data = self.transform_metadata_from_dict(row_dict)
                        
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO fichiers (
                                name, host, extension, username, hostname,
                                unc_directory, creation_time, last_write_time,
                                readable, writeable, deletable, directory_type,
                                base, path, file_size, owner, fast_hash,
                                access_time, file_attributes, file_signature,
                                last_modified
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                data["name"],
                                data["host"],
                                data["extension"],
                                data["username"],
                                data["hostname"],
                                data["unc_directory"],
                                data["creation_time"],
                                data["last_write_time"],
                                data["readable"],
                                data["writeable"],
                                data["deletable"],
                                data["directory_type"],
                                data["base"],
                                data["path"],
                                data["file_size"],
                                data["owner"],
                                data["fast_hash"],
                                data["access_time"],
                                data["file_attributes"],
                                data["file_signature"],
                                data["last_modified"],
                            ),
                        )
                        imported_files += 1
                        
                    except Exception as exc:
                        logger.warning("Erreur lors de l'insertion: %s", exc)
                        validation_stats["invalid_rows"] += 1
                        errors.append(str(exc))

                conn.commit()

        finally:
            conn.close()

        processing_time = time.perf_counter() - start

        return {
            "total_files": total_files,
            "imported_files": imported_files,
            "errors": errors,
            "processing_time": processing_time,
            "validation_stats": validation_stats,
        }

