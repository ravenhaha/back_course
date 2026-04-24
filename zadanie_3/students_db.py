import csv
import hashlib
import os
import secrets
from pathlib import Path
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, create_engine, func
from sqlalchemy.orm import DeclarativeBase, Session


# ─── 1. Модели данных ────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class StudentRecord(Base):
    __tablename__ = "student_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    last_name = Column(String, nullable=False)
    first_name = Column(String, nullable=False)
    faculty = Column(String, nullable=False)
    course = Column(String, nullable=False)
    score = Column(Integer, nullable=False)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False, unique=True, index=True)
    password_hash = Column(String, nullable=False)
    password_salt = Column(String, nullable=False)


class UserSession(Base):
    __tablename__ = "user_sessions"

    session_id = Column(String, primary_key=True)
    user_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


# ─── 2. Хэширование паролей (stdlib) ─────────────────────────────────────────

_PBKDF2_ITER = 200_000


def hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), _PBKDF2_ITER
    )
    return digest.hex(), salt


def verify_password(password: str, password_hash: str, salt: str) -> bool:
    calc, _ = hash_password(password, salt)
    return secrets.compare_digest(calc, password_hash)


# ─── 3. Репозиторий студентов ────────────────────────────────────────────────

class StudentRepository:
    def __init__(self, db_url: str = "sqlite:///students.db") -> None:
        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)

    def insert(self, record: StudentRecord) -> None:
        with Session(self.engine) as session:
            session.add(record)
            session.commit()
            session.refresh(record)

    def insert_many(self, records: list[StudentRecord]) -> None:
        with Session(self.engine) as session:
            session.add_all(records)
            session.commit()

    def load_from_csv(self, filepath: str | Path) -> int:
        records: list[StudentRecord] = []
        with open(filepath, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(
                    StudentRecord(
                        last_name=row["Фамилия"].strip(),
                        first_name=row["Имя"].strip(),
                        faculty=row["Факультет"].strip(),
                        course=row["Курс"].strip(),
                        score=int(row["Оценка"].strip()),
                    )
                )
        self.insert_many(records)
        return len(records)

    def select_all(self) -> list[StudentRecord]:
        with Session(self.engine) as session:
            return session.query(StudentRecord).all()

    def get_students_by_faculty(self, faculty: str) -> list[tuple[str, str]]:
        with Session(self.engine) as session:
            return (
                session.query(StudentRecord.last_name, StudentRecord.first_name)
                .filter(StudentRecord.faculty == faculty)
                .distinct()
                .order_by(StudentRecord.last_name, StudentRecord.first_name)
                .all()
            )

    def get_unique_courses(self) -> list[str]:
        with Session(self.engine) as session:
            rows = (
                session.query(StudentRecord.course)
                .distinct()
                .order_by(StudentRecord.course)
                .all()
            )
            return [r[0] for r in rows]

    def get_avg_score_by_faculty(self, faculty: str) -> float:
        with Session(self.engine) as session:
            avg = (
                session.query(func.avg(StudentRecord.score))
                .filter(StudentRecord.faculty == faculty)
                .scalar()
            )
            return round(avg, 2) if avg is not None else 0.0

    def get_students_below_score(
        self, course: str, threshold: int = 30
    ) -> list[StudentRecord]:
        with Session(self.engine) as session:
            return (
                session.query(StudentRecord)
                .filter(
                    StudentRecord.course == course,
                    StudentRecord.score < threshold,
                )
                .order_by(StudentRecord.score)
                .all()
            )

    def get_by_id(self, record_id: int) -> StudentRecord | None:
        with Session(self.engine) as session:
            return session.get(StudentRecord, record_id)

    def update(self, record_id: int, fields: dict) -> StudentRecord | None:
        with Session(self.engine) as session:
            record = session.get(StudentRecord, record_id)
            if record is None:
                return None
            for key, value in fields.items():
                setattr(record, key, value)
            session.commit()
            session.refresh(record)
            return record

    def delete(self, record_id: int) -> bool:
        with Session(self.engine) as session:
            record = session.get(StudentRecord, record_id)
            if record is None:
                return False
            session.delete(record)
            session.commit()
            return True


# ─── 4. Репозиторий пользователей ────────────────────────────────────────────

class UserRepository:
    def __init__(self, db_url: str = "sqlite:///students.db") -> None:
        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)

    def create(self, username: str, password: str) -> User:
        password_hash, salt = hash_password(password)
        user = User(username=username, password_hash=password_hash, password_salt=salt)
        with Session(self.engine) as session:
            session.add(user)
            session.commit()
            session.refresh(user)
            return user

    def get_by_username(self, username: str) -> User | None:
        with Session(self.engine) as session:
            return session.query(User).filter(User.username == username).first()

    def get_by_id(self, user_id: int) -> User | None:
        with Session(self.engine) as session:
            return session.get(User, user_id)

    def authenticate(self, username: str, password: str) -> User | None:
        user = self.get_by_username(username)
        if user is None:
            return None
        if not verify_password(password, user.password_hash, user.password_salt):
            return None
        return user


# ─── 5. Репозиторий сессий ───────────────────────────────────────────────────

class SessionRepository:
    def __init__(self, db_url: str = "sqlite:///students.db") -> None:
        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)

    def create(self, user_id: int) -> str:
        session_id = secrets.token_urlsafe(32)
        with Session(self.engine) as db:
            db.add(UserSession(session_id=session_id, user_id=user_id))
            db.commit()
        return session_id

    def get_user_id(self, session_id: str) -> int | None:
        with Session(self.engine) as db:
            row = db.get(UserSession, session_id)
            return row.user_id if row else None

    def delete(self, session_id: str) -> bool:
        with Session(self.engine) as db:
            row = db.get(UserSession, session_id)
            if row is None:
                return False
            db.delete(row)
            db.commit()
            return True