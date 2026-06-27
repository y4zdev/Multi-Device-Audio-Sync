use rusqlite::{params, Connection, Result};
use std::sync::Mutex;
use uuid::Uuid;
use bcrypt::{hash, DEFAULT_COST};

pub struct Db {
    conn: Mutex<Connection>,
}

#[derive(Debug, Clone)]
pub struct User {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub role: String,
}

impl Db {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL
            )",
            [],
        )?;
        
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            )",
            [],
        )?;
        
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn init_default_admin(&self) {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT count(*) FROM users").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        if count == 0 {
            let hash_a = hash("admin", DEFAULT_COST).unwrap();
            conn.execute(
                "INSERT INTO users (id, username, password_hash, role) VALUES (?1, ?2, ?3, ?4)",
                params![Uuid::new_v4().to_string(), "admin", hash_a, "admin"],
            ).unwrap();
            
            let hash_s = hash("speaker", DEFAULT_COST).unwrap();
            conn.execute("INSERT INTO users (id, username, password_hash, role) VALUES (?1, ?2, ?3, ?4)", params![Uuid::new_v4().to_string(), "speaker", hash_s, "speaker"]).unwrap();
            let hash_m = hash("mic", DEFAULT_COST).unwrap();
            conn.execute("INSERT INTO users (id, username, password_hash, role) VALUES (?1, ?2, ?3, ?4)", params![Uuid::new_v4().to_string(), "mic", hash_m, "mic"]).unwrap();
            let hash_c = hash("controller", DEFAULT_COST).unwrap();
            conn.execute("INSERT INTO users (id, username, password_hash, role) VALUES (?1, ?2, ?3, ?4)", params![Uuid::new_v4().to_string(), "controller", hash_c, "controller"]).unwrap();
        }
    }
    
    pub fn get_user(&self, username: &str) -> Result<Option<User>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, username, password_hash, role FROM users WHERE username = ?1")?;
        let user = stmt.query_row(params![username], |row| {
            Ok(User {
                id: row.get(0)?,
                username: row.get(1)?,
                password_hash: row.get(2)?,
                role: row.get(3)?,
            })
        });
        
        match user {
            Ok(u) => Ok(Some(u)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
    
    pub fn get_user_by_id(&self, id: &str) -> Result<Option<User>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, username, password_hash, role FROM users WHERE id = ?1")?;
        let user = stmt.query_row(params![id], |row| {
            Ok(User {
                id: row.get(0)?,
                username: row.get(1)?,
                password_hash: row.get(2)?,
                role: row.get(3)?,
            })
        });
        
        match user {
            Ok(u) => Ok(Some(u)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn create_session(&self, user_id: &str) -> Result<String> {
        let session_id = Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        let expires_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64 + 86400 * 7;
        conn.execute(
            "INSERT INTO sessions (id, user_id, expires_at) VALUES (?1, ?2, ?3)",
            params![session_id, user_id, expires_at],
        )?;
        Ok(session_id)
    }

    pub fn get_user_by_session(&self, session_id: &str) -> Result<Option<User>> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        let mut stmt = conn.prepare("SELECT user_id FROM sessions WHERE id = ?1 AND expires_at > ?2")?;
        let user_id: String = match stmt.query_row(params![session_id, now], |row| row.get(0)) {
            Ok(id) => id,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e),
        };
        drop(stmt);
        drop(conn);
        self.get_user_by_id(&user_id)
    }
    
    pub fn get_all_users(&self) -> Result<Vec<User>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, username, password_hash, role FROM users")?;
        let rows = stmt.query_map([], |row| {
            Ok(User {
                id: row.get(0)?,
                username: row.get(1)?,
                password_hash: row.get(2)?,
                role: row.get(3)?,
            })
        })?;
        let mut users = Vec::new();
        for r in rows { users.push(r?); }
        Ok(users)
    }

    pub fn create_user(&self, username: &str, password_hash: &str, role: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users (id, username, password_hash, role) VALUES (?1, ?2, ?3, ?4)",
            params![Uuid::new_v4().to_string(), username, password_hash, role],
        )?;
        Ok(())
    }
}
