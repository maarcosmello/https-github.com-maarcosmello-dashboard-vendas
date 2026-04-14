
import os
import random
import re
import sqlite3
import string
from datetime import date, datetime, timedelta
from functools import wraps
from io import BytesIO
from urllib.parse import urlparse

from flask import (
    Flask,
    abort,
    flash,
    g,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

try:
    from openpyxl import Workbook
except ImportError:
    Workbook = None


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "commission.db")

PAYMENT_FORMATS = {
    "avista": "A vista",
    "parcelado": "Parcelado",
    "recorrencia": "Recorrencia",
}
INSTALLMENT_STATUSES = {
    "confirmado": "Confirmado",
    "cancelado": "Cancelado",
    "atrasado": "Atrasado",
}
ROLES = ("admin", "seller", "viewer")
ROLE_LABELS = {"admin": "Administrador", "seller": "Vendedor", "viewer": "Visualizador"}
REQUEST_STATUSES = ("pendente", "aprovado", "recusado")

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'seller', 'viewer')),
    password_hash TEXT NOT NULL,
    header_label TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS courses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    default_commission_percent REAL NOT NULL CHECK (default_commission_percent >= 0 AND default_commission_percent <= 100),
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (company_id, name),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_phone TEXT,
    customer_email TEXT,
    company_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    payment_format TEXT NOT NULL CHECK (payment_format IN ('avista', 'parcelado', 'recorrencia')),
    installments_count INTEGER NOT NULL CHECK (installments_count >= 1),
    total_value REAL NOT NULL CHECK (total_value > 0),
    commission_percent REAL NOT NULL CHECK (commission_percent >= 0 AND commission_percent <= 100),
    total_commission_expected REAL NOT NULL,
    notes TEXT,
    created_by INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT,
    FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE RESTRICT,
    FOREIGN KEY (seller_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS sale_installments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id INTEGER NOT NULL,
    installment_number INTEGER NOT NULL,
    due_date TEXT NOT NULL,
    month_key TEXT NOT NULL,
    installment_value REAL NOT NULL,
    commission_value REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('confirmado', 'cancelado', 'atrasado')),
    paid_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (sale_id, installment_number),
    FOREIGN KEY (sale_id) REFERENCES sales(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS viewer_course_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    viewer_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    request_note TEXT,
    status TEXT NOT NULL CHECK (status IN ('pendente', 'aprovado', 'recusado')),
    requested_at TEXT NOT NULL,
    reviewed_at TEXT,
    reviewed_by INTEGER,
    approval_days INTEGER,
    is_permanent INTEGER NOT NULL DEFAULT 0,
    UNIQUE (viewer_id, course_id, status),
    FOREIGN KEY (viewer_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE,
    FOREIGN KEY (reviewed_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS viewer_course_access (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    viewer_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    granted_by INTEGER NOT NULL,
    granted_at TEXT NOT NULL,
    expires_at TEXT,
    is_permanent INTEGER NOT NULL DEFAULT 0,
    UNIQUE (viewer_id, course_id),
    FOREIGN KEY (viewer_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE,
    FOREIGN KEY (granted_by) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_sales_sale_date ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_company ON sales(company_id);
CREATE INDEX IF NOT EXISTS idx_sales_course ON sales(course_id);
CREATE INDEX IF NOT EXISTS idx_sales_seller ON sales(seller_id);

CREATE INDEX IF NOT EXISTS idx_installments_sale_id ON sale_installments(sale_id);
CREATE INDEX IF NOT EXISTS idx_installments_due_date ON sale_installments(due_date);
CREATE INDEX IF NOT EXISTS idx_installments_month_key ON sale_installments(month_key);
CREATE INDEX IF NOT EXISTS idx_installments_status ON sale_installments(status);
CREATE INDEX IF NOT EXISTS idx_requests_viewer ON viewer_course_requests(viewer_id);
CREATE INDEX IF NOT EXISTS idx_requests_course ON viewer_course_requests(course_id);
CREATE INDEX IF NOT EXISTS idx_access_viewer ON viewer_course_access(viewer_id);
CREATE INDEX IF NOT EXISTS idx_access_course ON viewer_course_access(course_id);
"""


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-this-secret-in-production")
app.config["DATABASE"] = DB_PATH
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"


def database_needs_reset(db_path):
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        table_rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = {row[0] for row in table_rows}
        required_tables = {
            "users",
            "companies",
            "courses",
            "sales",
            "sale_installments",
            "viewer_course_requests",
            "viewer_course_access",
        }
        if not required_tables.issubset(table_names):
            conn.close()
            return True

        course_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(courses)").fetchall()
        }
        user_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(users)").fetchall()
        }
        sale_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(sales)").fetchall()
        }
        installment_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(sale_installments)").fetchall()
        }

        sales_sql_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='sales'"
        ).fetchone()
        installments_sql_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='sale_installments'"
        ).fetchone()
        conn.close()

        course_ok = {"company_id", "default_commission_percent"}.issubset(course_cols)
        user_ok = {"header_label"}.issubset(user_cols)
        sale_ok = {"customer_name", "payment_format", "installments_count", "total_commission_expected"}.issubset(
            sale_cols
        )
        installment_ok = {"installment_number", "month_key", "commission_value"}.issubset(installment_cols)

        sales_sql = (sales_sql_row[0] or "").lower() if sales_sql_row else ""
        installments_sql = (installments_sql_row[0] or "").lower() if installments_sql_row else ""
        payment_check_ok = "parcelado" in sales_sql
        status_check_ok = "confirmado" in installments_sql and "atrasado" in installments_sql
        return not (course_ok and user_ok and sale_ok and installment_ok and payment_check_ok and status_check_ok)
    except sqlite3.Error:
        return True


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    legacy_users = []

    if database_needs_reset(DB_PATH):
        try:
            legacy_users = read_legacy_users(DB_PATH)
        except sqlite3.Error:
            legacy_users = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(DATA_DIR, f"commission_legacy_{timestamp}.db")
        os.replace(DB_PATH, backup_path)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SCHEMA_SQL)
        restore_users(conn, legacy_users)
        seed_defaults(conn)
        conn.commit()
    finally:
        conn.close()


def read_legacy_users(db_path):
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        ).fetchone()
        if not table:
            return []
        rows = conn.execute(
            "SELECT id, username, full_name, role, password_hash, is_active, created_at FROM users"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def restore_users(conn, users):
    if not users:
        return
    existing = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if existing:
        return
    for user in users:
        conn.execute(
            """
            INSERT INTO users (id, username, full_name, role, password_hash, header_label, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                user["id"],
                user["username"],
                user["full_name"],
                user["role"] if user["role"] in ROLES else "viewer",
                user["password_hash"],
                user["is_active"],
                user["created_at"],
            ),
        )


def seed_defaults(conn):
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if user_count == 0:
        return

    company_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    if company_count == 0:
        now = now_iso()
        cursor = conn.execute(
            "INSERT INTO companies (name, is_active, created_at, updated_at) VALUES (?, 1, ?, ?)",
            ("Empresa Padrao", now, now),
        )
        company_id = cursor.lastrowid
        conn.execute(
            """
            INSERT INTO courses (company_id, name, default_commission_percent, is_active, created_at, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            """,
            (company_id, "Curso Padrao", 10, now, now),
        )


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON;")
    return g.db


@app.teardown_appcontext
def close_db(_error):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


def normalize_username(username):
    return (username or "").strip().lower()


def clean_text(value, max_len=255):
    return (value or "").strip()[:max_len]


def validate_username(username):
    value = (username or "").strip()
    if not value:
        return False, "Usuario e obrigatorio."
    if any(ch.isspace() for ch in value):
        return False, "Usuario nao pode conter espacos."
    return True, ""


def validate_password(password, confirm_password=None):
    if len(password or "") < 8:
        return False, "Senha deve ter no minimo 8 caracteres."
    if confirm_password is not None and password != confirm_password:
        return False, "Senha invalida ou confirmacao diferente."
    return True, ""


def generate_temporary_password(length=12):
    charset = string.ascii_letters + string.digits + "!@#$%&*?"
    return "".join(random.choice(charset) for _ in range(length))


def parse_float(value, field_name, minimum=None, maximum=None, allow_empty=False):
    raw = (value or "").strip().replace(",", ".")
    if not raw:
        if allow_empty:
            return None
        raise ValueError(f"O campo '{field_name}' e obrigatorio.")
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ValueError(f"O campo '{field_name}' deve ser numerico.") from exc
    if minimum is not None and parsed < minimum:
        raise ValueError(f"O campo '{field_name}' deve ser maior ou igual a {minimum}.")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"O campo '{field_name}' deve ser menor ou igual a {maximum}.")
    return parsed


def parse_int(value, field_name, minimum=1, allow_empty=False):
    raw = (value or "").strip()
    if not raw:
        if allow_empty:
            return None
        raise ValueError(f"O campo '{field_name}' e obrigatorio.")
    if not raw.isdigit():
        raise ValueError(f"O campo '{field_name}' deve ser inteiro.")
    parsed = int(raw)
    if parsed < minimum:
        raise ValueError(f"O campo '{field_name}' e invalido.")
    return parsed


def parse_date(value, field_name):
    raw = (value or "").strip()
    if not raw:
        raise ValueError(f"O campo '{field_name}' e obrigatorio.")
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"O campo '{field_name}' deve estar no formato YYYY-MM-DD.") from exc
    return parsed.isoformat()


def parse_month(value):
    raw = (value or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}", raw):
        return raw
    return ""


def parse_year(value):
    raw = (value or "").strip()
    if re.fullmatch(r"\d{4}", raw):
        return raw
    return ""


def current_user():
    return g.get("current_user")


@app.before_request
def load_current_user():
    user_id = session.get("user_id")
    g.current_user = None
    if not user_id:
        return
    db = get_db()
    user = db.execute(
        "SELECT id, username, full_name, role, header_label, is_active FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if user and user["is_active"]:
        g.current_user = user
    else:
        session.clear()


def login_required(view_fn):
    @wraps(view_fn)
    def wrapped(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login"))
        return view_fn(*args, **kwargs)

    return wrapped


def roles_required(*roles):
    def decorator(view_fn):
        @wraps(view_fn)
        def wrapped(*args, **kwargs):
            user = current_user()
            if not user:
                return redirect(url_for("login"))
            if user["role"] not in roles:
                abort(403)
            return view_fn(*args, **kwargs)

        return wrapped

    return decorator


def csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = os.urandom(24).hex()
        session["_csrf_token"] = token
    return token


def validate_csrf():
    token = request.form.get("csrf_token", "")
    if not token or token != session.get("_csrf_token"):
        abort(400, "Token CSRF invalido.")


@app.context_processor
def inject_globals():
    user = current_user()
    default_header = "Sistema de Comissionamento"
    header_label = default_header
    if user and user["header_label"]:
        header_label = user["header_label"]
    return {
        "current_user": user,
        "role_labels": ROLE_LABELS,
        "payment_formats": PAYMENT_FORMATS,
        "installment_statuses": INSTALLMENT_STATUSES,
        "request_statuses": REQUEST_STATUSES,
        "header_label": header_label,
        "csrf_token": csrf_token,
    }


@app.template_filter("currency")
def currency_filter(value):
    value = float(value or 0)
    formatted = f"{value:,.2f}"
    return f"R$ {formatted}".replace(",", "X").replace(".", ",").replace("X", ".")


@app.template_filter("date_br")
def date_br_filter(value):
    if not value:
        return "-"
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
        return parsed.strftime("%d/%m/%Y")
    except ValueError:
        return value


@app.template_filter("month_br")
def month_br_filter(value):
    if not value:
        return "-"
    try:
        parsed = datetime.strptime(value + "-01", "%Y-%m-%d")
        names = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
        return f"{names[parsed.month - 1]}/{parsed.year}"
    except ValueError:
        return value


def add_months(iso_date, months):
    base = datetime.strptime(iso_date, "%Y-%m-%d").date()
    month_index = base.month - 1 + months
    year = base.year + month_index // 12
    month = month_index % 12 + 1
    day = min(
        base.day,
        [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][
            month - 1
        ],
    )
    return date(year, month, day).isoformat()


def split_installments(total_value, count):
    cents = int(round(total_value * 100))
    base = cents // count
    remainder = cents % count
    values = []
    for idx in range(count):
        value_cents = base + (1 if idx < remainder else 0)
        values.append(value_cents / 100.0)
    return values


def generate_installments(total_value, commission_percent, installments_count, base_date):
    values = split_installments(total_value, installments_count)
    installments = []
    for index, installment_value in enumerate(values, start=1):
        due_date = add_months(base_date, index - 1)
        commission_value = round((installment_value * commission_percent) / 100, 2)
        installments.append(
            {
                "installment_number": index,
                "due_date": due_date,
                "month_key": due_date[:7],
                "installment_value": round(installment_value, 2),
                "commission_value": commission_value,
            }
        )
    return installments


def can_edit_sale(user, seller_id):
    if user["role"] == "admin":
        return True
    if user["role"] == "seller" and user["id"] == seller_id:
        return True
    return False


def has_any_user():
    db = get_db()
    row = db.execute("SELECT COUNT(*) AS total FROM users").fetchone()
    return row["total"] > 0


def fetch_companies(include_inactive=False):
    db = get_db()
    sql = "SELECT id, name, is_active FROM companies"
    if not include_inactive:
        sql += " WHERE is_active = 1"
    sql += " ORDER BY name COLLATE NOCASE ASC"
    return db.execute(sql).fetchall()


def fetch_courses(include_inactive=False):
    db = get_db()
    sql = """
    SELECT c.id, c.company_id, c.name, c.default_commission_percent, c.is_active, comp.name AS company_name
    FROM courses c
    INNER JOIN companies comp ON comp.id = c.company_id
    """
    if not include_inactive:
        sql += " WHERE c.is_active = 1 AND comp.is_active = 1"
    sql += " ORDER BY comp.name COLLATE NOCASE ASC, c.name COLLATE NOCASE ASC"
    return db.execute(sql).fetchall()


def fetch_sellers():
    db = get_db()
    return db.execute(
        """
        SELECT id, full_name, username, role
        FROM users
        WHERE is_active = 1 AND role IN ('admin', 'seller')
        ORDER BY full_name COLLATE NOCASE ASC
        """
    ).fetchall()


def parse_filters(args, user):
    filters = {
        "sale_date_start": "",
        "company_id": None,
        "course_id": None,
        "seller_id": None,
        "status": "",
        "payment_format": "",
        "customer_name": "",
        "min_value": None,
        "max_value": None,
        "search": "",
    }

    start_sale = (args.get("sale_date_start") or "").strip()
    if start_sale:
        try:
            filters["sale_date_start"] = parse_date(start_sale, "Data da venda inicial")
        except ValueError:
            pass

    for key, label in (("company_id", "Empresa"), ("course_id", "Curso")):
        try:
            filters[key] = parse_int(args.get(key), label, allow_empty=True)
        except ValueError:
            filters[key] = None

    if user["role"] in ("admin", "viewer"):
        try:
            filters["seller_id"] = parse_int(args.get("seller_id"), "Vendedor", allow_empty=True)
        except ValueError:
            filters["seller_id"] = None
    else:
        filters["seller_id"] = user["id"]

    status = clean_text(args.get("status"), 30)
    if status in INSTALLMENT_STATUSES:
        filters["status"] = status

    payment_format = clean_text(args.get("payment_format"), 20)
    if payment_format in PAYMENT_FORMATS:
        filters["payment_format"] = payment_format

    filters["customer_name"] = clean_text(args.get("customer_name"), 120)
    filters["search"] = clean_text(args.get("search"), 120)

    try:
        filters["min_value"] = parse_float(args.get("min_value"), "Valor minimo", minimum=0, allow_empty=True)
    except ValueError:
        filters["min_value"] = None
    try:
        filters["max_value"] = parse_float(args.get("max_value"), "Valor maximo", minimum=0, allow_empty=True)
    except ValueError:
        filters["max_value"] = None

    return filters


def filters_to_query(filters, user):
    query = {}
    for key in (
        "sale_date_start",
        "company_id",
        "course_id",
        "seller_id",
        "status",
        "payment_format",
        "customer_name",
        "min_value",
        "max_value",
        "search",
    ):
        value = filters.get(key)
        if value in (None, ""):
            continue
        if key == "seller_id" and user["role"] == "seller":
            continue
        query[key] = value
    return query


def fetch_customer_names(user):
    db = get_db()
    if user["role"] == "seller":
        rows = db.execute(
            """
            SELECT DISTINCT customer_name
            FROM sales
            WHERE seller_id = ?
            ORDER BY customer_name COLLATE NOCASE ASC
            """,
            (user["id"],),
        ).fetchall()
    elif user["role"] == "viewer":
        allowed_courses = get_viewer_accessible_course_ids(user["id"])
        if not allowed_courses:
            return []
        placeholders = ",".join("?" for _ in allowed_courses)
        rows = db.execute(
            f"""
            SELECT DISTINCT customer_name
            FROM sales
            WHERE course_id IN ({placeholders})
            ORDER BY customer_name COLLATE NOCASE ASC
            """,
            allowed_courses,
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT DISTINCT customer_name
            FROM sales
            ORDER BY customer_name COLLATE NOCASE ASC
            """
        ).fetchall()
    return [row["customer_name"] for row in rows]


def get_viewer_accessible_course_ids(viewer_id):
    db = get_db()
    today = date.today().isoformat()
    rows = db.execute(
        """
        SELECT course_id
        FROM viewer_course_access
        WHERE viewer_id = ?
          AND (is_permanent = 1 OR expires_at IS NULL OR expires_at >= ?)
        """,
        (viewer_id, today),
    ).fetchall()
    return [row["course_id"] for row in rows]


def fetch_installment_rows(filters, user, recurring_only=False):
    db = get_db()
    sql = """
    SELECT
      i.id AS installment_id,
      i.sale_id,
      i.installment_number,
      i.due_date,
      i.month_key,
      i.installment_value,
      i.commission_value,
      i.status AS installment_status,
      i.paid_at,
      s.sale_date,
      s.customer_name,
      s.customer_phone,
      s.customer_email,
      s.payment_format,
      s.installments_count,
      s.total_value,
      s.commission_percent,
      s.total_commission_expected,
      s.seller_id,
      s.notes,
      comp.name AS company_name,
      c.name AS course_name,
      u.full_name AS seller_name
    FROM sale_installments i
    INNER JOIN sales s ON s.id = i.sale_id
    INNER JOIN companies comp ON comp.id = s.company_id
    INNER JOIN courses c ON c.id = s.course_id
    INNER JOIN users u ON u.id = s.seller_id
    WHERE 1=1
    """
    params = []

    if user["role"] == "seller":
        sql += " AND s.seller_id = ?"
        params.append(user["id"])
    elif filters["seller_id"]:
        sql += " AND s.seller_id = ?"
        params.append(filters["seller_id"])

    if filters["sale_date_start"]:
        sql += " AND s.sale_date >= ?"
        params.append(filters["sale_date_start"])
    if filters["company_id"]:
        sql += " AND s.company_id = ?"
        params.append(filters["company_id"])
    if filters["course_id"]:
        sql += " AND s.course_id = ?"
        params.append(filters["course_id"])
    if filters["status"]:
        sql += " AND i.status = ?"
        params.append(filters["status"])
    if filters["payment_format"]:
        sql += " AND s.payment_format = ?"
        params.append(filters["payment_format"])
    if recurring_only:
        sql += " AND s.payment_format = 'recorrencia'"
    if filters["customer_name"]:
        sql += " AND lower(s.customer_name) = lower(?)"
        params.append(filters["customer_name"])
    if filters["min_value"] is not None:
        sql += " AND i.installment_value >= ?"
        params.append(filters["min_value"])
    if filters["max_value"] is not None:
        sql += " AND i.installment_value <= ?"
        params.append(filters["max_value"])
    if filters["search"]:
        like = f"%{filters['search']}%"
        sql += """
        AND (
          lower(s.customer_name) LIKE lower(?)
          OR lower(COALESCE(s.customer_phone, '')) LIKE lower(?)
          OR lower(COALESCE(s.customer_email, '')) LIKE lower(?)
          OR lower(comp.name) LIKE lower(?)
          OR lower(c.name) LIKE lower(?)
          OR lower(u.full_name) LIKE lower(?)
          OR lower(COALESCE(s.notes, '')) LIKE lower(?)
        )
        """
        params.extend([like, like, like, like, like, like, like])

    if user["role"] == "viewer":
        allowed_courses = get_viewer_accessible_course_ids(user["id"])
        if not allowed_courses:
            return []
        placeholders = ",".join("?" for _ in allowed_courses)
        sql += f" AND s.course_id IN ({placeholders})"
        params.extend(allowed_courses)

    sql += " ORDER BY i.due_date DESC, i.installment_number DESC"
    rows = db.execute(sql, params).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["can_edit"] = can_edit_sale(user, row["seller_id"])
        result.append(item)
    return result


def summarize_dashboard(rows):
    totals = {
        "count_installments": len(rows),
        "projected_value": 0.0,
        "projected_commission": 0.0,
        "confirmed_value": 0.0,
        "confirmed_commission": 0.0,
        "canceled_value": 0.0,
        "canceled_commission": 0.0,
        "overdue_value": 0.0,
        "overdue_commission": 0.0,
    }
    by_month = {}
    by_year = {}
    by_course = {}

    for row in rows:
        value = float(row["installment_value"])
        commission = float(row["commission_value"])
        status = row["installment_status"]
        due_date = row["due_date"]

        if status != "cancelado":
            totals["projected_value"] += value
            totals["projected_commission"] += commission
            month_key = row["month_key"]
            by_month.setdefault(month_key, {"value": 0.0, "commission": 0.0})
            by_month[month_key]["value"] += value
            by_month[month_key]["commission"] += commission

            year_key = month_key[:4]
            by_year.setdefault(year_key, {"value": 0.0, "commission": 0.0})
            by_year[year_key]["value"] += value
            by_year[year_key]["commission"] += commission

            course = row["course_name"]
            by_course.setdefault(course, 0.0)
            by_course[course] += commission

        if status == "confirmado":
            totals["confirmed_value"] += value
            totals["confirmed_commission"] += commission
        elif status == "cancelado":
            totals["canceled_value"] += value
            totals["canceled_commission"] += commission
        elif status == "atrasado":
            totals["overdue_value"] += value
            totals["overdue_commission"] += commission

    for key in totals:
        totals[key] = round(totals[key], 2) if isinstance(totals[key], float) else totals[key]

    month_keys = sorted(by_month.keys())
    year_keys = sorted(by_year.keys())
    course_items = sorted(by_course.items(), key=lambda pair: pair[1], reverse=True)

    charts = {
        "monthly": {
            "labels": [month_to_label(k) for k in month_keys],
            "value": [round(by_month[k]["value"], 2) for k in month_keys],
            "commission": [round(by_month[k]["commission"], 2) for k in month_keys],
        },
        "yearly": {
            "labels": year_keys,
            "value": [round(by_year[k]["value"], 2) for k in year_keys],
            "commission": [round(by_year[k]["commission"], 2) for k in year_keys],
        },
        "course": {
            "labels": [item[0] for item in course_items],
            "commission": [round(item[1], 2) for item in course_items],
        },
    }
    return totals, charts


def month_to_label(month_key):
    try:
        parsed = datetime.strptime(month_key + "-01", "%Y-%m-%d")
        names = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
        return f"{names[parsed.month - 1]}/{parsed.year}"
    except ValueError:
        return month_key


def valid_internal_return(path):
    if not path:
        return False
    parsed = urlparse(path)
    if parsed.netloc:
        return False
    return path.startswith("/dashboard")


def validate_email(email):
    if not email:
        return True
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email))


@app.route("/")
def home():
    if not has_any_user():
        return redirect(url_for("setup_admin"))
    if current_user():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/setup-admin", methods=["GET", "POST"])
def setup_admin():
    if has_any_user():
        return redirect(url_for("login"))

    if request.method == "POST":
        validate_csrf()
        username = normalize_username(request.form.get("username"))
        full_name = clean_text(request.form.get("full_name"), 80)
        password = request.form.get("password") or ""
        password_confirm = request.form.get("password_confirm") or ""

        valid_user, user_message = validate_username(username)
        if not valid_user:
            flash(user_message, "error")
            return render_template("setup_admin.html")
        if len(full_name) < 3:
            flash("Nome completo muito curto.", "error")
            return render_template("setup_admin.html")
        valid_password, password_message = validate_password(password, password_confirm)
        if not valid_password:
            flash(password_message, "error")
            return render_template("setup_admin.html")

        db = get_db()
        db.execute(
            """
            INSERT INTO users (username, full_name, role, password_hash, is_active, created_at)
            VALUES (?, ?, 'admin', ?, 1, ?)
            """,
            (username, full_name, generate_password_hash(password), now_iso()),
        )
        db.commit()
        flash("Administrador criado. Faca login.", "success")
        return redirect(url_for("login"))

    return render_template("setup_admin.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if not has_any_user():
        return redirect(url_for("setup_admin"))
    if current_user():
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        validate_csrf()
        username = normalize_username(request.form.get("username"))
        password = request.form.get("password") or ""
        db = get_db()
        user = db.execute(
            "SELECT id, full_name, role, password_hash, is_active FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if not user or not user["is_active"] or not check_password_hash(user["password_hash"], password):
            flash("Usuario ou senha invalidos.", "error")
            return render_template("login.html")

        session.clear()
        session["user_id"] = user["id"]
        session["_csrf_token"] = os.urandom(24).hex()
        flash(f"Bem-vindo, {user['full_name']}!", "success")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        validate_csrf()
        username = normalize_username(request.form.get("username"))
        full_name = clean_text(request.form.get("full_name"), 80)

        valid_user, user_message = validate_username(username)
        if not valid_user:
            flash(user_message, "error")
            return render_template("forgot_password.html")

        db = get_db()
        user = db.execute(
            """
            SELECT id, username, full_name, is_active
            FROM users
            WHERE username = ?
            """,
            (username,),
        ).fetchone()
        if not user or not user["is_active"]:
            flash("Usuario nao encontrado ou inativo.", "error")
            return render_template("forgot_password.html")

        if user["full_name"].strip().lower() != full_name.strip().lower():
            flash("Nome completo nao confere com o usuario informado.", "error")
            return render_template("forgot_password.html")

        new_password = generate_temporary_password(12)
        db.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (generate_password_hash(new_password), user["id"]),
        )
        db.commit()
        return render_template(
            "forgot_password.html",
            generated_password=new_password,
            username=username,
        )

    return render_template("forgot_password.html")


@app.post("/profile/header")
@login_required
def update_header_label():
    validate_csrf()
    user = current_user()
    next_label = clean_text(request.form.get("header_label"), 80)
    db = get_db()
    db.execute("UPDATE users SET header_label = ? WHERE id = ?", (next_label or None, user["id"]))
    db.commit()
    flash("Cabecalho pessoal atualizado.", "success")
    return redirect(url_for("dashboard"))


@app.post("/logout")
@login_required
def logout():
    validate_csrf()
    session.clear()
    flash("Sessao encerrada.", "success")
    return redirect(url_for("login"))


@app.get("/dashboard")
@login_required
def dashboard():
    return render_dashboard_view(recurring_only=False)


@app.get("/dashboard/recorrencia")
@login_required
def dashboard_recorrencia():
    return render_dashboard_view(recurring_only=True)


def render_dashboard_view(recurring_only=False):
    user = current_user()
    filters = parse_filters(request.args, user)
    rows = fetch_installment_rows(filters, user, recurring_only=recurring_only)
    totals, charts = summarize_dashboard(rows)

    companies = fetch_companies()
    courses = fetch_courses()
    sellers = fetch_sellers() if user["role"] in ("admin", "viewer") else []
    customer_names = fetch_customer_names(user)
    viewer_accesses = []
    if user["role"] == "viewer":
        db = get_db()
        today = date.today().isoformat()
        viewer_accesses = db.execute(
            """
            SELECT a.course_id, c.name AS course_name, comp.name AS company_name, a.is_permanent, a.expires_at
            FROM viewer_course_access a
            INNER JOIN courses c ON c.id = a.course_id
            INNER JOIN companies comp ON comp.id = c.company_id
            WHERE a.viewer_id = ?
              AND (a.is_permanent = 1 OR a.expires_at IS NULL OR a.expires_at >= ?)
            ORDER BY comp.name, c.name
            """,
            (user["id"], today),
        ).fetchall()

    edit_sale = None
    edit_id_raw = (request.args.get("edit_sale") or "").strip()
    if edit_id_raw.isdigit():
        edit_id = int(edit_id_raw)
        db = get_db()
        sale = db.execute(
            """
            SELECT s.*
            FROM sales s
            WHERE s.id = ?
            """,
            (edit_id,),
        ).fetchone()
        if sale and can_edit_sale(user, sale["seller_id"]):
            edit_sale = dict(sale)
        elif sale:
            flash("Voce nao tem permissao para editar esta venda.", "error")

    sale_form = {
        "sale_id": edit_sale["id"] if edit_sale else "",
        "sale_date": edit_sale["sale_date"] if edit_sale else date.today().isoformat(),
        "customer_name": edit_sale["customer_name"] if edit_sale else "",
        "customer_phone": edit_sale["customer_phone"] if edit_sale and edit_sale["customer_phone"] else "",
        "customer_email": edit_sale["customer_email"] if edit_sale and edit_sale["customer_email"] else "",
        "company_id": edit_sale["company_id"] if edit_sale else "",
        "course_id": edit_sale["course_id"] if edit_sale else "",
        "payment_format": edit_sale["payment_format"] if edit_sale else "avista",
        "installments_count": edit_sale["installments_count"] if edit_sale else 1,
        "total_value": edit_sale["total_value"] if edit_sale else "",
        "commission_percent": edit_sale["commission_percent"] if edit_sale else "",
        "seller_id": edit_sale["seller_id"] if edit_sale else (user["id"] if user["role"] == "seller" else ""),
        "notes": edit_sale["notes"] if edit_sale and edit_sale["notes"] else "",
    }

    filter_payload = filters_to_query(filters, user)
    if recurring_only:
        filter_payload["payment_format"] = "recorrencia"
    export_url = url_for("export_xlsx", **filter_payload)
    current_path = request.full_path.rstrip("?")

    return render_template(
        "dashboard.html",
        filters=filters,
        rows=rows,
        totals=totals,
        charts=charts,
        companies=companies,
        courses=courses,
        sellers=sellers,
        customer_names=customer_names,
        sale_form=sale_form,
        can_write=user["role"] in ("admin", "seller"),
        export_url=export_url,
        current_path=current_path,
        recurring_only=recurring_only,
        viewer_accesses=viewer_accesses,
    )


@app.post("/sales/save")
@login_required
def save_sale():
    user = current_user()
    if user["role"] == "viewer":
        abort(403)
    validate_csrf()

    sale_id_raw = (request.form.get("sale_id") or "").strip()
    return_to = (request.form.get("return_to") or "").strip()

    try:
        sale_date = parse_date(request.form.get("sale_date"), "Data da venda")
        customer_name = clean_text(request.form.get("customer_name"), 120)
        if len(customer_name) < 2:
            raise ValueError("Informe um nome de cliente valido.")
        customer_phone = clean_text(request.form.get("customer_phone"), 40)
        customer_email = clean_text(request.form.get("customer_email"), 120)
        if not validate_email(customer_email):
            raise ValueError("Email invalido.")

        company_id = parse_int(request.form.get("company_id"), "Empresa")
        course_id = parse_int(request.form.get("course_id"), "Curso")
        payment_format = clean_text(request.form.get("payment_format"), 20)
        if payment_format not in PAYMENT_FORMATS:
            raise ValueError("Formato de pagamento invalido.")
        total_value = parse_float(request.form.get("total_value"), "Valor total", minimum=0.01)
        commission_percent = parse_float(
            request.form.get("commission_percent"), "Percentual de comissao", minimum=0, maximum=100
        )
        installments_count = parse_int(request.form.get("installments_count"), "Quantidade de parcelas", minimum=1)
        if payment_format == "avista":
            installments_count = 1
        notes = clean_text(request.form.get("notes"), 1500)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("dashboard"))

    db = get_db()
    company = db.execute("SELECT id FROM companies WHERE id = ? AND is_active = 1", (company_id,)).fetchone()
    if not company:
        flash("Empresa invalida ou inativa.", "error")
        return redirect(url_for("dashboard"))
    course = db.execute(
        "SELECT id FROM courses WHERE id = ? AND company_id = ? AND is_active = 1",
        (course_id, company_id),
    ).fetchone()
    if not course:
        flash("Curso invalido para a empresa selecionada.", "error")
        return redirect(url_for("dashboard"))

    if user["role"] == "admin":
        try:
            seller_id = parse_int(request.form.get("seller_id"), "Vendedor")
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("dashboard"))
        seller = db.execute(
            "SELECT id, role, is_active FROM users WHERE id = ?",
            (seller_id,),
        ).fetchone()
        if not seller or not seller["is_active"] or seller["role"] not in ("admin", "seller"):
            flash("Vendedor invalido.", "error")
            return redirect(url_for("dashboard"))
    else:
        seller_id = user["id"]

    installments = generate_installments(
        total_value=total_value,
        commission_percent=commission_percent,
        installments_count=installments_count,
        base_date=sale_date,
    )
    total_commission_expected = round(sum(item["commission_value"] for item in installments), 2)
    timestamp = now_iso()

    if sale_id_raw:
        if not sale_id_raw.isdigit():
            flash("Venda invalida para edicao.", "error")
            return redirect(url_for("dashboard"))
        sale_id = int(sale_id_raw)
        existing = db.execute("SELECT id, seller_id FROM sales WHERE id = ?", (sale_id,)).fetchone()
        if not existing:
            flash("Venda nao encontrada.", "error")
            return redirect(url_for("dashboard"))
        if not can_edit_sale(user, existing["seller_id"]):
            abort(403)

        db.execute(
            """
            UPDATE sales
            SET sale_date = ?,
                customer_name = ?,
                customer_phone = ?,
                customer_email = ?,
                company_id = ?,
                course_id = ?,
                seller_id = ?,
                payment_format = ?,
                installments_count = ?,
                total_value = ?,
                commission_percent = ?,
                total_commission_expected = ?,
                notes = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                sale_date,
                customer_name,
                customer_phone or None,
                customer_email or None,
                company_id,
                course_id,
                seller_id,
                payment_format,
                installments_count,
                total_value,
                commission_percent,
                total_commission_expected,
                notes or None,
                timestamp,
                sale_id,
            ),
        )
        db.execute("DELETE FROM sale_installments WHERE sale_id = ?", (sale_id,))
        new_sale_id = sale_id
        flash("Venda atualizada com sucesso.", "success")
    else:
        cursor = db.execute(
            """
            INSERT INTO sales (
              sale_date, customer_name, customer_phone, customer_email, company_id, course_id,
              seller_id, payment_format, installments_count, total_value, commission_percent,
              total_commission_expected, notes, created_by, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sale_date,
                customer_name,
                customer_phone or None,
                customer_email or None,
                company_id,
                course_id,
                seller_id,
                payment_format,
                installments_count,
                total_value,
                commission_percent,
                total_commission_expected,
                notes or None,
                user["id"],
                timestamp,
                timestamp,
            ),
        )
        new_sale_id = cursor.lastrowid
        flash("Venda cadastrada com sucesso.", "success")

    for item in installments:
        db.execute(
            """
            INSERT INTO sale_installments (
              sale_id, installment_number, due_date, month_key, installment_value,
              commission_value, status, paid_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'confirmado', NULL, ?, ?)
            """,
            (
                new_sale_id,
                item["installment_number"],
                item["due_date"],
                item["month_key"],
                item["installment_value"],
                item["commission_value"],
                timestamp,
                timestamp,
            ),
        )

    db.commit()
    if valid_internal_return(return_to):
        return redirect(return_to)
    return redirect(url_for("dashboard"))


@app.post("/sales/<int:sale_id>/delete")
@login_required
def delete_sale(sale_id):
    user = current_user()
    if user["role"] == "viewer":
        abort(403)
    validate_csrf()

    db = get_db()
    sale = db.execute("SELECT id, seller_id FROM sales WHERE id = ?", (sale_id,)).fetchone()
    if not sale:
        flash("Venda nao encontrada.", "error")
        return redirect(url_for("dashboard"))
    if not can_edit_sale(user, sale["seller_id"]):
        abort(403)

    db.execute("DELETE FROM sales WHERE id = ?", (sale_id,))
    db.commit()
    flash("Venda e parcelas removidas com sucesso.", "success")
    return redirect(url_for("dashboard"))


@app.post("/installments/<int:installment_id>/status")
@login_required
def update_installment_status(installment_id):
    user = current_user()
    if user["role"] == "viewer":
        abort(403)
    validate_csrf()

    next_status = clean_text(request.form.get("status"), 20)
    if next_status not in INSTALLMENT_STATUSES:
        flash("Status de parcela invalido.", "error")
        return redirect(url_for("dashboard"))

    db = get_db()
    row = db.execute(
        """
        SELECT i.id, i.status, s.seller_id
        FROM sale_installments i
        INNER JOIN sales s ON s.id = i.sale_id
        WHERE i.id = ?
        """,
        (installment_id,),
    ).fetchone()
    if not row:
        flash("Parcela nao encontrada.", "error")
        return redirect(url_for("dashboard"))
    if not can_edit_sale(user, row["seller_id"]):
        abort(403)

    paid_at = now_iso() if next_status == "confirmado" else None
    db.execute(
        "UPDATE sale_installments SET status = ?, paid_at = ?, updated_at = ? WHERE id = ?",
        (next_status, paid_at, now_iso(), installment_id),
    )
    db.commit()
    flash("Status da parcela atualizado.", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/users", methods=["GET", "POST"])
@roles_required("admin")
def manage_users():
    db = get_db()
    if request.method == "POST":
        validate_csrf()
        action = clean_text(request.form.get("action"), 30)

        if action == "create":
            username = normalize_username(request.form.get("username"))
            full_name = clean_text(request.form.get("full_name"), 80)
            role = clean_text(request.form.get("role"), 20)
            password = request.form.get("password") or ""
            confirm = request.form.get("password_confirm") or ""

            valid_user, user_message = validate_username(username)
            if not valid_user:
                flash(user_message, "error")
                return redirect(url_for("manage_users"))
            if role not in ROLES:
                flash("Perfil invalido.", "error")
                return redirect(url_for("manage_users"))
            if len(full_name) < 3:
                flash("Nome completo muito curto.", "error")
                return redirect(url_for("manage_users"))
            valid_password, password_message = validate_password(password, confirm)
            if not valid_password:
                flash(password_message, "error")
                return redirect(url_for("manage_users"))

            exists = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if exists:
                flash("Usuario ja existe.", "error")
                return redirect(url_for("manage_users"))

            db.execute(
                """
                INSERT INTO users (username, full_name, role, password_hash, is_active, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (username, full_name, role, generate_password_hash(password), now_iso()),
            )
            db.commit()
            flash("Usuario criado.", "success")
            return redirect(url_for("manage_users"))

        if action == "update_role":
            try:
                user_id = parse_int(request.form.get("user_id"), "Usuario")
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_users"))
            role = clean_text(request.form.get("role"), 20)
            if role not in ROLES:
                flash("Perfil invalido.", "error")
                return redirect(url_for("manage_users"))

            target = db.execute("SELECT id, role FROM users WHERE id = ?", (user_id,)).fetchone()
            if not target:
                flash("Usuario nao encontrado.", "error")
                return redirect(url_for("manage_users"))
            if target["role"] == "admin" and role != "admin":
                active_admins = db.execute(
                    "SELECT COUNT(*) AS total FROM users WHERE role = 'admin' AND is_active = 1"
                ).fetchone()["total"]
                if active_admins <= 1:
                    flash("Nao e possivel remover o ultimo admin ativo.", "error")
                    return redirect(url_for("manage_users"))

            db.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))
            db.commit()
            flash("Perfil atualizado.", "success")
            return redirect(url_for("manage_users"))

        if action == "toggle_active":
            try:
                user_id = parse_int(request.form.get("user_id"), "Usuario")
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_users"))

            target = db.execute("SELECT id, role, is_active FROM users WHERE id = ?", (user_id,)).fetchone()
            if not target:
                flash("Usuario nao encontrado.", "error")
                return redirect(url_for("manage_users"))
            if target["id"] == current_user()["id"] and target["is_active"]:
                flash("Nao e possivel desativar o proprio usuario conectado.", "error")
                return redirect(url_for("manage_users"))
            if target["role"] == "admin" and target["is_active"]:
                active_admins = db.execute(
                    "SELECT COUNT(*) AS total FROM users WHERE role = 'admin' AND is_active = 1"
                ).fetchone()["total"]
                if active_admins <= 1:
                    flash("Nao e possivel desativar o ultimo admin ativo.", "error")
                    return redirect(url_for("manage_users"))

            db.execute("UPDATE users SET is_active = ? WHERE id = ?", (0 if target["is_active"] else 1, user_id))
            db.commit()
            flash("Status do usuario atualizado.", "success")
            return redirect(url_for("manage_users"))

        if action == "reset_password":
            try:
                user_id = parse_int(request.form.get("user_id"), "Usuario")
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_users"))
            new_password = request.form.get("new_password") or ""
            valid_password, password_message = validate_password(new_password)
            if not valid_password:
                flash(password_message, "error")
                return redirect(url_for("manage_users"))

            db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (generate_password_hash(new_password), user_id))
            db.commit()
            flash("Senha atualizada.", "success")
            return redirect(url_for("manage_users"))

    users = db.execute(
        "SELECT id, username, full_name, role, is_active, created_at FROM users ORDER BY created_at DESC"
    ).fetchall()
    return render_template("users.html", users=users)


@app.route("/admin/companies", methods=["GET", "POST"])
@roles_required("admin")
def manage_companies():
    db = get_db()
    if request.method == "POST":
        validate_csrf()
        action = clean_text(request.form.get("action"), 30)

        if action == "create":
            name = clean_text(request.form.get("name"), 120)
            if len(name) < 2:
                flash("Nome da empresa invalido.", "error")
                return redirect(url_for("manage_companies"))
            exists = db.execute("SELECT id FROM companies WHERE lower(name) = lower(?)", (name,)).fetchone()
            if exists:
                flash("Empresa ja existe.", "error")
                return redirect(url_for("manage_companies"))
            now = now_iso()
            db.execute(
                "INSERT INTO companies (name, is_active, created_at, updated_at) VALUES (?, 1, ?, ?)",
                (name, now, now),
            )
            db.commit()
            flash("Empresa criada.", "success")
            return redirect(url_for("manage_companies"))

        if action == "update":
            try:
                company_id = parse_int(request.form.get("company_id"), "Empresa")
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_companies"))
            name = clean_text(request.form.get("name"), 120)
            if len(name) < 2:
                flash("Nome da empresa invalido.", "error")
                return redirect(url_for("manage_companies"))
            duplicate = db.execute(
                "SELECT id FROM companies WHERE lower(name) = lower(?) AND id <> ?",
                (name, company_id),
            ).fetchone()
            if duplicate:
                flash("Ja existe outra empresa com este nome.", "error")
                return redirect(url_for("manage_companies"))
            db.execute("UPDATE companies SET name = ?, updated_at = ? WHERE id = ?", (name, now_iso(), company_id))
            db.commit()
            flash("Empresa atualizada.", "success")
            return redirect(url_for("manage_companies"))

        if action == "toggle_active":
            try:
                company_id = parse_int(request.form.get("company_id"), "Empresa")
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_companies"))
            row = db.execute("SELECT id, is_active FROM companies WHERE id = ?", (company_id,)).fetchone()
            if not row:
                flash("Empresa nao encontrada.", "error")
                return redirect(url_for("manage_companies"))
            db.execute("UPDATE companies SET is_active = ?, updated_at = ? WHERE id = ?", (0 if row["is_active"] else 1, now_iso(), company_id))
            db.commit()
            flash("Status da empresa atualizado.", "success")
            return redirect(url_for("manage_companies"))

    companies = fetch_companies(include_inactive=True)
    return render_template("companies.html", companies=companies)


@app.route("/admin/courses", methods=["GET", "POST"])
@roles_required("admin")
def manage_courses():
    db = get_db()
    if request.method == "POST":
        validate_csrf()
        action = clean_text(request.form.get("action"), 30)

        if action == "create":
            try:
                company_id = parse_int(request.form.get("company_id"), "Empresa")
                default_percent = parse_float(
                    request.form.get("default_commission_percent"),
                    "Comissao padrao",
                    minimum=0,
                    maximum=100,
                )
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_courses"))
            name = clean_text(request.form.get("name"), 120)
            if len(name) < 2:
                flash("Nome do curso invalido.", "error")
                return redirect(url_for("manage_courses"))
            exists = db.execute(
                "SELECT id FROM courses WHERE company_id = ? AND lower(name) = lower(?)",
                (company_id, name),
            ).fetchone()
            if exists:
                flash("Curso ja existe nesta empresa.", "error")
                return redirect(url_for("manage_courses"))
            now = now_iso()
            db.execute(
                """
                INSERT INTO courses (company_id, name, default_commission_percent, is_active, created_at, updated_at)
                VALUES (?, ?, ?, 1, ?, ?)
                """,
                (company_id, name, default_percent, now, now),
            )
            db.commit()
            flash("Curso criado.", "success")
            return redirect(url_for("manage_courses"))

        if action == "update":
            try:
                course_id = parse_int(request.form.get("course_id"), "Curso")
                company_id = parse_int(request.form.get("company_id"), "Empresa")
                default_percent = parse_float(
                    request.form.get("default_commission_percent"),
                    "Comissao padrao",
                    minimum=0,
                    maximum=100,
                )
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_courses"))
            name = clean_text(request.form.get("name"), 120)
            if len(name) < 2:
                flash("Nome do curso invalido.", "error")
                return redirect(url_for("manage_courses"))
            duplicate = db.execute(
                "SELECT id FROM courses WHERE company_id = ? AND lower(name) = lower(?) AND id <> ?",
                (company_id, name, course_id),
            ).fetchone()
            if duplicate:
                flash("Ja existe outro curso com este nome na empresa.", "error")
                return redirect(url_for("manage_courses"))
            db.execute(
                """
                UPDATE courses
                SET company_id = ?, name = ?, default_commission_percent = ?, updated_at = ?
                WHERE id = ?
                """,
                (company_id, name, default_percent, now_iso(), course_id),
            )
            db.commit()
            flash("Curso atualizado.", "success")
            return redirect(url_for("manage_courses"))

        if action == "toggle_active":
            try:
                course_id = parse_int(request.form.get("course_id"), "Curso")
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("manage_courses"))
            row = db.execute("SELECT id, is_active FROM courses WHERE id = ?", (course_id,)).fetchone()
            if not row:
                flash("Curso nao encontrado.", "error")
                return redirect(url_for("manage_courses"))
            db.execute("UPDATE courses SET is_active = ?, updated_at = ? WHERE id = ?", (0 if row["is_active"] else 1, now_iso(), course_id))
            db.commit()
            flash("Status do curso atualizado.", "success")
            return redirect(url_for("manage_courses"))

    companies = fetch_companies(include_inactive=False)
    courses = fetch_courses(include_inactive=True)
    return render_template("courses.html", companies=companies, courses=courses)


@app.route("/viewer/course-access", methods=["GET", "POST"])
@roles_required("viewer")
def viewer_course_access():
    user = current_user()
    db = get_db()

    if request.method == "POST":
        validate_csrf()
        try:
            course_id = parse_int(request.form.get("course_id"), "Curso")
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("viewer_course_access"))

        note = clean_text(request.form.get("request_note"), 300)
        course = db.execute(
            "SELECT id FROM courses WHERE id = ? AND is_active = 1",
            (course_id,),
        ).fetchone()
        if not course:
            flash("Curso invalido para solicitacao.", "error")
            return redirect(url_for("viewer_course_access"))

        already_access = db.execute(
            "SELECT id FROM viewer_course_access WHERE viewer_id = ? AND course_id = ?",
            (user["id"], course_id),
        ).fetchone()
        if already_access:
            flash("Voce ja possui acesso a este curso.", "success")
            return redirect(url_for("viewer_course_access"))

        pending = db.execute(
            """
            SELECT id
            FROM viewer_course_requests
            WHERE viewer_id = ? AND course_id = ? AND status = 'pendente'
            """,
            (user["id"], course_id),
        ).fetchone()
        if pending:
            flash("Ja existe uma solicitacao pendente para este curso.", "error")
            return redirect(url_for("viewer_course_access"))

        db.execute(
            """
            INSERT INTO viewer_course_requests (
                viewer_id, course_id, request_note, status, requested_at
            ) VALUES (?, ?, ?, 'pendente', ?)
            """,
            (user["id"], course_id, note or None, now_iso()),
        )
        db.commit()
        flash("Solicitacao enviada ao administrador.", "success")
        return redirect(url_for("viewer_course_access"))

    today = date.today().isoformat()
    courses = fetch_courses(include_inactive=False)
    active_accesses = db.execute(
        """
        SELECT a.course_id, c.name AS course_name, comp.name AS company_name, a.is_permanent, a.expires_at
        FROM viewer_course_access a
        INNER JOIN courses c ON c.id = a.course_id
        INNER JOIN companies comp ON comp.id = c.company_id
        WHERE a.viewer_id = ?
          AND (a.is_permanent = 1 OR a.expires_at IS NULL OR a.expires_at >= ?)
        ORDER BY comp.name, c.name
        """,
        (user["id"], today),
    ).fetchall()
    requests = db.execute(
        """
        SELECT r.id, r.course_id, r.request_note, r.status, r.requested_at, r.reviewed_at, r.approval_days, r.is_permanent,
               c.name AS course_name, comp.name AS company_name
        FROM viewer_course_requests r
        INNER JOIN courses c ON c.id = r.course_id
        INNER JOIN companies comp ON comp.id = c.company_id
        WHERE r.viewer_id = ?
        ORDER BY r.requested_at DESC
        """,
        (user["id"],),
    ).fetchall()
    return render_template(
        "viewer_course_access.html",
        courses=courses,
        active_accesses=active_accesses,
        requests=requests,
    )


@app.route("/admin/course-requests", methods=["GET", "POST"])
@roles_required("admin")
def admin_course_requests():
    db = get_db()
    admin = current_user()

    if request.method == "POST":
        validate_csrf()
        action = clean_text(request.form.get("action"), 30)
        try:
            request_id = parse_int(request.form.get("request_id"), "Solicitacao")
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("admin_course_requests"))

        request_row = db.execute(
            """
            SELECT id, viewer_id, course_id, status
            FROM viewer_course_requests
            WHERE id = ?
            """,
            (request_id,),
        ).fetchone()
        if not request_row:
            flash("Solicitacao nao encontrada.", "error")
            return redirect(url_for("admin_course_requests"))
        if request_row["status"] != "pendente":
            flash("Esta solicitacao ja foi analisada.", "error")
            return redirect(url_for("admin_course_requests"))

        if action == "deny":
            db.execute(
                """
                UPDATE viewer_course_requests
                SET status = 'recusado', reviewed_at = ?, reviewed_by = ?
                WHERE id = ?
                """,
                (now_iso(), admin["id"], request_id),
            )
            db.commit()
            flash("Solicitacao recusada.", "success")
            return redirect(url_for("admin_course_requests"))

        if action == "approve":
            permanent = request.form.get("is_permanent") == "1"
            approval_days_raw = (request.form.get("approval_days") or "").strip()
            approval_days = None
            expires_at = None
            if not permanent:
                try:
                    approval_days = parse_int(approval_days_raw, "Dias de acesso", minimum=1)
                except ValueError as exc:
                    flash(str(exc), "error")
                    return redirect(url_for("admin_course_requests"))
                expires_at = (date.today() + timedelta(days=approval_days)).isoformat()

            db.execute(
                """
                INSERT INTO viewer_course_access (
                    viewer_id, course_id, granted_by, granted_at, expires_at, is_permanent
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(viewer_id, course_id)
                DO UPDATE SET
                    granted_by = excluded.granted_by,
                    granted_at = excluded.granted_at,
                    expires_at = excluded.expires_at,
                    is_permanent = excluded.is_permanent
                """,
                (
                    request_row["viewer_id"],
                    request_row["course_id"],
                    admin["id"],
                    now_iso(),
                    expires_at,
                    1 if permanent else 0,
                ),
            )
            db.execute(
                """
                UPDATE viewer_course_requests
                SET status = 'aprovado',
                    reviewed_at = ?,
                    reviewed_by = ?,
                    approval_days = ?,
                    is_permanent = ?
                WHERE id = ?
                """,
                (now_iso(), admin["id"], approval_days, 1 if permanent else 0, request_id),
            )
            db.commit()
            flash("Solicitacao aprovada.", "success")
            return redirect(url_for("admin_course_requests"))

        flash("Acao invalida para solicitacao.", "error")
        return redirect(url_for("admin_course_requests"))

    requests = db.execute(
        """
        SELECT r.id, r.status, r.request_note, r.requested_at, r.reviewed_at, r.approval_days, r.is_permanent,
               u.full_name AS viewer_name, u.username AS viewer_username,
               c.name AS course_name, comp.name AS company_name
        FROM viewer_course_requests r
        INNER JOIN users u ON u.id = r.viewer_id
        INNER JOIN courses c ON c.id = r.course_id
        INNER JOIN companies comp ON comp.id = c.company_id
        ORDER BY
            CASE r.status WHEN 'pendente' THEN 0 WHEN 'aprovado' THEN 1 ELSE 2 END,
            r.requested_at DESC
        """
    ).fetchall()
    return render_template("admin_course_requests.html", requests=requests)


@app.get("/export/xlsx")
@login_required
def export_xlsx():
    if Workbook is None:
        flash("Instale openpyxl para exportar xlsx: pip install openpyxl", "error")
        return redirect(url_for("dashboard"))

    user = current_user()
    filters = parse_filters(request.args, user)
    rows = fetch_installment_rows(filters, user)
    if not rows:
        flash("Nao ha dados para exportar.", "error")
        return redirect(url_for("dashboard"))

    totals, charts = summarize_dashboard(rows)
    wb = Workbook()
    ws = wb.active
    ws.title = "Parcelas Filtradas"
    ws.append(
        [
            "Data Venda",
            "Cliente",
            "Telefone",
            "Email",
            "Empresa",
            "Curso",
            "Vendedor",
            "Pagamento",
            "Parcela",
            "Qtd Parcelas",
            "Vencimento",
            "Mes",
            "Valor Parcela",
            "Comissao %",
            "Comissao Parcela",
            "Valor Total Venda",
            "Comissao Total Venda",
            "Status Pagamento",
            "Observacoes",
        ]
    )

    for row in rows:
        ws.append(
            [
                row["sale_date"],
                row["customer_name"],
                row["customer_phone"] or "",
                row["customer_email"] or "",
                row["company_name"],
                row["course_name"],
                row["seller_name"],
                PAYMENT_FORMATS.get(row["payment_format"], row["payment_format"]),
                row["installment_number"],
                row["installments_count"],
                row["due_date"],
                row["month_key"],
                float(row["installment_value"]),
                float(row["commission_percent"]),
                float(row["commission_value"]),
                float(row["total_value"]),
                float(row["total_commission_expected"]),
                INSTALLMENT_STATUSES.get(row["installment_status"], row["installment_status"]),
                row["notes"] or "",
            ]
        )

    ws2 = wb.create_sheet("Resumo")
    ws2.append(["Indicador", "Valor"])
    ws2.append(["Parcelas filtradas", totals["count_installments"]])
    ws2.append(["Previsto (valor)", totals["projected_value"]])
    ws2.append(["Previsto (comissao)", totals["projected_commission"]])
    ws2.append(["Confirmado (valor)", totals["confirmed_value"]])
    ws2.append(["Confirmado (comissao)", totals["confirmed_commission"]])
    ws2.append(["Cancelado (valor)", totals["canceled_value"]])
    ws2.append(["Cancelado (comissao)", totals["canceled_commission"]])
    ws2.append(["Atrasado (valor)", totals["overdue_value"]])
    ws2.append(["Atrasado (comissao)", totals["overdue_commission"]])
    ws2.append(["Exportado em", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])

    ws3 = wb.create_sheet("Projecao Mensal")
    ws3.append(["Mes", "Valor", "Comissao"])
    for idx, month_label in enumerate(charts["monthly"]["labels"]):
        ws3.append([month_label, charts["monthly"]["value"][idx], charts["monthly"]["commission"][idx]])

    ws4 = wb.create_sheet("Projecao Anual")
    ws4.append(["Ano", "Valor", "Comissao"])
    for idx, year_label in enumerate(charts["yearly"]["labels"]):
        ws4.append([year_label, charts["yearly"]["value"][idx], charts["yearly"]["commission"][idx]])

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    file_name = f"comissionamento_filtrado_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=file_name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.errorhandler(400)
def error_400(error):
    message = getattr(error, "description", "Requisicao invalida.")
    return render_template("error.html", title="Requisicao invalida", message=message), 400


@app.errorhandler(403)
def error_403(_error):
    return render_template("error.html", title="Acesso negado", message="Voce nao tem permissao para esta acao."), 403


@app.errorhandler(404)
def error_404(_error):
    return render_template("error.html", title="Nao encontrado", message="Recurso nao encontrado."), 404


init_db()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
