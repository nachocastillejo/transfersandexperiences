import sqlite3
import os
from flask import current_app, g

DATABASE_FILENAME = 'conversations.db'

def get_db_path():
    """Returns the absolute path to the database file."""
    # Place the database in the 'db' directory at the root of the project
    # Adjust this if your 'db' directory is elsewhere or you prefer a different location
    if hasattr(current_app, 'root_path'):
        # Inside Flask app context
        # current_app.root_path is typically the 'app' directory path.
        # os.path.dirname(current_app.root_path) gives the project root (e.g., 'Secretary').
        project_root = os.path.dirname(current_app.root_path)
        return os.path.join(project_root, 'db', DATABASE_FILENAME)
    else:
        # Outside Flask app context (e.g., scripts)
        # Assumes this util is in 'app/utils/' and project root is two levels up
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        return os.path.join(project_root, 'db', DATABASE_FILENAME)

def get_db():
    """
    Connects to the application's configured database. The connection
    is unique for each request and will be reused if this is called
    again.
    Stores the connection in Flask's 'g' object if in app context.
    """
    db_path = get_db_path()
    # Ensure database directory and file exist before connecting
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    if not os.path.exists(db_path):
        # Create empty file; actual schema ensured by init_db
        open(db_path, 'a').close()
    if current_app and 'db' not in g:
        g.db = sqlite3.connect(
            db_path,
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row  # Access columns by name
        return g.db
    elif not current_app:
        # For use in scripts outside Flask app context
        conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        return conn
    return g.db # Return existing connection from g

def close_db(e=None):
    """
    If this request connected to the database, close the
    connection.
    """
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    db_path = get_db_path()
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Created database directory: {db_dir}")

    # Use a temporary direct connection for initialization
    # to avoid issues with Flask's 'g' object if called outside a request
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                project_name TEXT,
                sender_name TEXT,
                wa_id TEXT NOT NULL,
                direction TEXT NOT NULL, -- 'inbound', 'outbound_bot', 'outbound_agent'
                message_text TEXT,
                model TEXT,
                whatsapp_message_id TEXT, -- For outbound messages
                status TEXT, -- e.g., 'read', 'sent', 'delivered', 'failed', 'ignored_paused'
                response_time_seconds REAL,
                attempt_count INTEGER,
                required_action TEXT,
                error_message TEXT,
                phone_number_id TEXT
            )
        """)
        # Catalog of conversation statuses
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS status_definitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        except sqlite3.Error as _e:
            (current_app.logger.error if current_app else print)(f"Error ensuring status_definitions table: {_e}")
        # Ensure optional columns exist (lightweight migrations)
        try:
            cursor.execute("PRAGMA table_info(messages);")
            existing_cols = {row[1] for row in cursor.fetchall()}
            if 'estado_conversacion' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN estado_conversacion TEXT")
            if 'phone_number_id' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN phone_number_id TEXT")
            if 'model' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN model TEXT")
            # Media support columns
            if 'media_type' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN media_type TEXT")
            if 'media_url' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN media_url TEXT")
            if 'media_filename' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN media_filename TEXT")
            if 'media_mime_type' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN media_mime_type TEXT")
            if 'media_size_bytes' not in existing_cols:
                cursor.execute("ALTER TABLE messages ADD COLUMN media_size_bytes INTEGER")
        except sqlite3.Error:
            # If PRAGMA or ALTER fails, continue; core table exists
            pass
        # Create helpful indexes (idempotent)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_wa_id ON messages(wa_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_direction ON messages(direction)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_wa_id_timestamp ON messages(wa_id, timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_whatsapp_id ON messages(whatsapp_message_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_sender_name ON messages(sender_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_phone_number_id ON messages(phone_number_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_phone_wa_ts ON messages(phone_number_id, wa_id, timestamp)")
        except sqlite3.Error as idx_error:
            (current_app.logger.error if current_app else print)(f"Error creating indexes: {idx_error}")
        # Enable WAL mode for better concurrency
        # This pragma is persistent for the database file.
        # It's good practice to set it during initialization.
        try:
            cursor.execute("PRAGMA journal_mode=WAL;")
            # You can verify it by fetching the result: conn.execute("PRAGMA journal_mode;").fetchone()
            # This should return ('wal',)
            current_app.logger.info("SQLite journal_mode set to WAL.") if current_app else print("SQLite journal_mode set to WAL.")
        except sqlite3.Error as wal_error:
            (current_app.logger.error if current_app else print)(f"Error setting journal_mode to WAL: {wal_error}")

        conn.commit()
        print(f"Database initialized/checked at {db_path}")
    except sqlite3.Error as e:
        print(f"Error initializing database: {e}")
        if conn: # Rollback in case of partial table creation or other errors
            conn.rollback()
    finally:
        if conn:
            conn.close()

def init_app(app):
    """Register database functions with the Flask app."""
    app.teardown_appcontext(close_db)
    # You could add a CLI command to initialize the DB here if desired
    # For example: app.cli.add_command(init_db_command)
    # For now, we can call init_db() manually or at app startup.
    with app.app_context(): # Ensure an app context exists for init_db
        init_db()

if __name__ == '__main__':
    # This allows running `python -m app.utils.database_utils` to initialize the DB
    print("Initializing database directly from script...")
    # Correct CWD adjustment for script execution when file is in app/utils/
    original_cwd = os.getcwd()
    # Script is in app/utils/, project_root is ../../ from here
    project_root_for_script = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    os.chdir(project_root_for_script) 
    init_db()
    os.chdir(original_cwd) 
    print("Database initialization attempt complete.") 