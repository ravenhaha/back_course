import csv
from pathlib import Path

from sqlalchemy import Column, Integer, String, create_engine, func
from sqlalchemy.orm import DeclarativeBase, Session


# ─── 1. Модель данных ────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class StudentRecord(Base):
    """Одна запись из таблицы: студент + факультет + курс + оценка."""

    __tablename__ = "student_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    last_name = Column(String, nullable=False)
    first_name = Column(String, nullable=False)
    faculty = Column(String, nullable=False)
    course = Column(String, nullable=False)
    score = Column(Integer, nullable=False)

    def __repr__(self) -> str:
        return (
            f"StudentRecord(id={self.id}, "
            f"{self.last_name} {self.first_name}, "
            f"faculty={self.faculty!r}, course={self.course!r}, score={self.score})"
        )


# ─── 2. Репозиторий (INSERT / SELECT) ────────────────────────────────────────

class StudentRepository:
    def __init__(self, db_url: str = "sqlite:///students.db") -> None:
        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)

    # ── INSERT ──────────────────────────────────────────────────────────────

    def insert(self, record: StudentRecord) -> None:
        """Добавить одну запись."""
        with Session(self.engine) as session:
            session.add(record)
            session.commit()

    def insert_many(self, records: list[StudentRecord]) -> None:
        """Добавить список записей одной транзакцией."""
        with Session(self.engine) as session:
            session.add_all(records)
            session.commit()

    # ── 3. Загрузка из CSV ──────────────────────────────────────────────────

    def load_from_csv(self, filepath: str | Path) -> int:
        """
        Считать students.csv и сохранить все строки в БД.
        Возвращает количество добавленных записей.
        """
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

    # ── SELECT: общий ───────────────────────────────────────────────────────

    def select_all(self) -> list[StudentRecord]:
        with Session(self.engine) as session:
            return session.query(StudentRecord).all()

    # ── 4a. Студенты по факультету ──────────────────────────────────────────

    def get_students_by_faculty(self, faculty: str) -> list[tuple[str, str]]:
        """
        Вернуть уникальные пары (фамилия, имя) студентов заданного факультета.
        """
        with Session(self.engine) as session:
            rows = (
                session.query(StudentRecord.last_name, StudentRecord.first_name)
                .filter(StudentRecord.faculty == faculty)
                .distinct()
                .order_by(StudentRecord.last_name, StudentRecord.first_name)
                .all()
            )
            return rows

    # ── 4b. Уникальные курсы ────────────────────────────────────────────────

    def get_unique_courses(self) -> list[str]:
        """Вернуть отсортированный список уникальных курсов."""
        with Session(self.engine) as session:
            rows = (
                session.query(StudentRecord.course)
                .distinct()
                .order_by(StudentRecord.course)
                .all()
            )
            return [r[0] for r in rows]

    # ── Средний балл по факультету ──────────────────────────────────────────

    def get_avg_score_by_faculty(self, faculty: str) -> float:
        """Средний балл по всем курсам и студентам факультета."""
        with Session(self.engine) as session:
            avg = (
                session.query(func.avg(StudentRecord.score))
                .filter(StudentRecord.faculty == faculty)
                .scalar()
            )
            return round(avg, 2) if avg is not None else 0.0

    # ── 4c. *Студенты по курсу с оценкой ниже порога ───────────────────────

    def get_students_below_score(
        self, course: str, threshold: int = 30
    ) -> list[StudentRecord]:
        """
        Студенты, получившие оценку ниже threshold на заданном курсе.
        По умолчанию threshold=30.
        """
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

    # ── SELECT by id ─────────────────────────────────────────────────────────

    def get_by_id(self, record_id: int) -> StudentRecord | None:
        """Получить запись по первичному ключу."""
        with Session(self.engine) as session:
            return session.get(StudentRecord, record_id)

    # ── UPDATE ───────────────────────────────────────────────────────────────

    def update(self, record_id: int, fields: dict) -> StudentRecord | None:
        """
        Обновить поля записи с указанным id.
        fields — словарь {имя_поля: новое_значение}.
        Возвращает обновлённый объект или None, если запись не найдена.
        """
        with Session(self.engine) as session:
            record = session.get(StudentRecord, record_id)
            if record is None:
                return None
            for key, value in fields.items():
                setattr(record, key, value)
            session.commit()
            session.refresh(record)
            return record

    # ── DELETE ───────────────────────────────────────────────────────────────

    def delete(self, record_id: int) -> bool:
        """
        Удалить запись по id.
        Возвращает True при успехе, False если запись не найдена.
        """
        with Session(self.engine) as session:
            record = session.get(StudentRecord, record_id)
            if record is None:
                return False
            session.delete(record)
            session.commit()
            return True


# ─── Демонстрация ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    CSV_PATH = Path(__file__).parent.parent / "students.csv"
    DB_URL = "sqlite:///students.db"

    repo = StudentRepository(db_url=DB_URL)

    print("Загрузка данных из CSV...")
    count = repo.load_from_csv(CSV_PATH)
    print(f"  Добавлено записей: {count}\n")

    faculty = "АВТФ"
    print(f"Студенты факультета {faculty!r}:")
    for last, first in repo.get_students_by_faculty(faculty):
        print(f"  {last} {first}")

    print("\nУникальные курсы:")
    for course in repo.get_unique_courses():
        print(f"  {course}")

    print(f"\nСредний балл по факультету {faculty!r}: {repo.get_avg_score_by_faculty(faculty)}")

    course = "Мат. Анализ"
    print(f"\nСтуденты с оценкой < 30 на курсе {course!r}:")
    for rec in repo.get_students_below_score(course, threshold=30):
        print(f"  {rec.last_name} {rec.first_name} | {rec.faculty} | score={rec.score}")