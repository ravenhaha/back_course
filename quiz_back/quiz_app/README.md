# Quiz Service (Итоговый проект: “Сервис викторин / квизов”)

Backend на **FastAPI** + **PostgreSQL** + **Alembic** + **Redis Pub/Sub**.

Реализовано по заданию:
- роли пользователей: `admin` и `player`
- CRUD API: пользователи, вопросы, игры
- эндпойнт для приёма ответа команды на текущий вопрос с проверкой таймера **30 секунд**
- auth API: регистрация, аутентификация, смена пароля, обновление токена (refresh)
- “реальное время”: публикация текущего вопроса в Redis Pub/Sub канал игры + WebSocket подписка

## Быстрый запуск (Docker)

```bash
docker compose up --build
```

- Swagger: `http://localhost:8000/docs`

## Auth в Swagger

1. `POST /auth/register` (можно создать `admin` для простоты проверки)
2. `POST /auth/login` -> получишь `access_token`
3. В Swagger нажми **Authorize** и вставь: `Bearer <access_token>`

## Realtime

- `POST /games/{game_id}/start` публикует сообщение в Redis канал `quiz:game:{game_id}`
- WebSocket подписка: `GET /ws/games/{game_id}` (получает JSON-сообщения из Pub/Sub)

Auth flow in Swagger:

1. `POST /auth/register`
2. `POST /auth/login` (browser will store the HTTP-only cookie)
3. Call any `/students/*` endpoint

## Local Run (Without Docker)

Requirements: Python 3.12+ and a running PostgreSQL instance.

1. Install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
```

2. Set DB URL, run migrations, start the server:

```powershell
$env:DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/students"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\uvicorn.exe app.main:app --reload --host 127.0.0.1 --port 8000
```

## Tests

Run tests locally:

```powershell
.\.venv\Scripts\pytest.exe -q
```

Note: tests run against in-memory SQLite (FastAPI dependency override) to stay fast and isolated from PostgreSQL.

Or run tests inside Docker:

```bash
docker compose run --rm tests
```

## Request Examples (PowerShell)

```powershell
$base = "http://localhost:8000"

# Register
Invoke-RestMethod -Method Post "$base/auth/register" `
  -ContentType "application/json" `
  -Body (@{ username="alice"; password="password123" } | ConvertTo-Json)

# Login (stores HTTP-only cookie in $s)
Invoke-RestMethod -Method Post "$base/auth/login" `
  -ContentType "application/json" `
  -Body (@{ username="alice"; password="password123" } | ConvertTo-Json) `
  -SessionVariable s

# Current user
Invoke-RestMethod -Method Get "$base/auth/me" -WebSession $s

# Create student
Invoke-RestMethod -Method Post "$base/students/" `
  -ContentType "application/json" `
  -Body (@{ last_name="Ivanov"; first_name="Ivan"; faculty="IT"; course="1"; score=90 } | ConvertTo-Json) `
  -WebSession $s

# List students
Invoke-RestMethod -Method Get "$base/students/" -WebSession $s

# Get student by id (example id=1)
Invoke-RestMethod -Method Get "$base/students/1" -WebSession $s

# Update student (example id=1)
Invoke-RestMethod -Method Put "$base/students/1" `
  -ContentType "application/json" `
  -Body (@{ score=95 } | ConvertTo-Json) `
  -WebSession $s

# Analytics
Invoke-RestMethod -Method Get "$base/students/meta/courses" -WebSession $s
Invoke-RestMethod -Method Get "$base/students/faculty/IT" -WebSession $s
Invoke-RestMethod -Method Get "$base/students/meta/faculty/IT/avg-score" -WebSession $s
Invoke-RestMethod -Method Get "$base/students/meta/courses/1/below-score?threshold=96" -WebSession $s

# Delete student (example id=1)
Invoke-RestMethod -Method Delete "$base/students/1" -WebSession $s

# Logout
Invoke-RestMethod -Method Post "$base/auth/logout" -WebSession $s
```
