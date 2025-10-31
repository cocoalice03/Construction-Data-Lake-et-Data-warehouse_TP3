"""Small helper to manage access rights for the student data lake project.

It creates a very small table `data_lake_permissions` in MySQL and exposes
three simple commands:
    - create-table : ensure the table exists
    - grant        : add or update a permission for a user and a folder
    - list         : display current permissions

Example:
    python permissions_manager.py create-table --mysql-password mypass
    python permissions_manager.py grant --email alice@example.com \
        --folder streams/transaction_stream --permission read --granted-by admin@example.com \
        --mysql-password mypass
    python permissions_manager.py list --mysql-password mypass
"""
from __future__ import annotations

import argparse
import mysql.connector
from mysql.connector import Error

from sync_to_mysql import MYSQL_CONFIG


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS data_lake_permissions (
    permission_id INT AUTO_INCREMENT PRIMARY KEY,
    user_email VARCHAR(255) NOT NULL,
    folder_path VARCHAR(255) NOT NULL,
    permission_type ENUM('read', 'write', 'admin') NOT NULL DEFAULT 'read',
    granted_by VARCHAR(255) NOT NULL,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at DATE NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE KEY uniq_user_folder (user_email, folder_path, permission_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


class PermissionStore:
    def __init__(self, password: str) -> None:
        config = MYSQL_CONFIG.copy()
        config["password"] = password
        self.conn = mysql.connector.connect(**config)
        self.cursor = self.conn.cursor(dictionary=True)

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()

    def ensure_table(self) -> None:
        self.cursor.execute(CREATE_TABLE_SQL)
        self.conn.commit()

    def grant(self, email: str, folder: str, permission: str, granted_by: str, expires_at: str | None) -> None:
        sql = """
        INSERT INTO data_lake_permissions (user_email, folder_path, permission_type, granted_by, expires_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            granted_by = VALUES(granted_by),
            granted_at = CURRENT_TIMESTAMP,
            expires_at = VALUES(expires_at),
            is_active = TRUE;
        """
        self.cursor.execute(sql, (email, folder, permission, granted_by, expires_at))
        self.conn.commit()

    def list_permissions(self) -> list[dict]:
        self.cursor.execute("SELECT * FROM data_lake_permissions ORDER BY user_email, folder_path")
        return self.cursor.fetchall()


+def main() -> None:
+    parser = argparse.ArgumentParser(description="Tiny permissions helper for the data lake.")
+    subparsers = parser.add_subparsers(dest="command", required=True)
+
+    table_parser = subparsers.add_parser("create-table", help="Créer la table data_lake_permissions.")
+    table_parser.add_argument("--mysql-password", required=True)
+
+    grant_parser = subparsers.add_parser("grant", help="Accorder une permission à un utilisateur.")
+    grant_parser.add_argument("--mysql-password", required=True)
+    grant_parser.add_argument("--email", required=True)
+    grant_parser.add_argument("--folder", required=True)
+    grant_parser.add_argument("--permission", choices=["read", "write", "admin"], default="read")
+    grant_parser.add_argument("--granted-by", required=True)
+    grant_parser.add_argument("--expires-at", help="Date d'expiration optionnelle (YYYY-MM-DD)")
+
+    list_parser = subparsers.add_parser("list", help="Lister les permissions existantes.")
+    list_parser.add_argument("--mysql-password", required=True)
+
+    args = parser.parse_args()
+
+    try:
+        store = PermissionStore(password=args.mysql_password)
+    except Error as err:
+        raise SystemExit(f"Connexion MySQL impossible : {err}")
+
+    try:
+        if args.command == "create-table":
+            store.ensure_table()
+            print("Table data_lake_permissions vérifiée/créée.")
+        elif args.command == "grant":
+            store.grant(args.email, args.folder, args.permission, args.granted_by, args.expires_at)
+            print(f"Permission '{args.permission}' accordée à {args.email} sur {args.folder}.")
+        elif args.command == "list":
+            rows = store.list_permissions()
+            if not rows:
+                print("Aucune permission enregistrée.")
+            else:
+                for row in rows:
+                    print(
+                        f"- {row['user_email']} -> {row['folder_path']} ({row['permission_type']})"
+                    )
+    finally:
+        store.close()
+
+
+if __name__ == "__main__":
+    main()
+PY
