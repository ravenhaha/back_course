# Quiz Service

Backend итогового проекта по варианту "Сервис викторин / квизов".

Стек: **FastAPI**, **PostgreSQL**, **SQLAlchemy**, **Alembic**, **Redis Pub/Sub**, **Docker Compose**.

## Что реализовано

- роли пользователей: `admin` и `player`;
- регистрация, логин, refresh token, смена пароля;
- CRUD для пользователей, команд, вопросов и игр;
- хранение команд, игроков, вопросов, вариантов ответов, игр и ответов команд;
- прием ответа команды на текущий вопрос с проверкой таймера 30 секунд;
- фиксация результатов игры в базе данных;
- публикация текущего вопроса через Redis Pub/Sub;
- WebSocket-подписка на канал игры;
- Docker Compose для запуска API, PostgreSQL и Redis;
- тесты для основных API-сценариев и realtime WebSocket.

## Быстрый запуск

```powershell
cd D:\projectsWeb\fastapi_course\quiz_back\quiz_app
docker compose up --build
```

После запуска:

- API: `http://localhost:8000`
- Swagger: `http://localhost:8000/docs`
- встроенный UI: `http://localhost:8000/ui/`

При старте контейнера автоматически выполняются миграции и создаются демо-данные:

- администратор: `admin` / `admin123`;
- игроки: `player1` / `password123`, `player2` / `password123`;
- команды: `Team A`, `Team B`;
- несколько вопросов;
- одна уже завершенная игра с ответами и очками;
- одна draft-игра для ручной проверки realtime-сценария.

## Проверка в Postman

### 1. Login

```http
POST http://localhost:8000/auth/login
Content-Type: application/json
```

```json
{
  "username": "admin",
  "password": "admin123"
}
```

Скопируйте `access_token` из ответа.

Для защищенных запросов используйте:

```text
Authorization: Bearer <access_token>
```

### 2. Проверить текущего пользователя

```http
GET http://localhost:8000/auth/me
Authorization: Bearer <access_token>
```

### 3. Посмотреть команды и статистику

```http
GET http://localhost:8000/teams
Authorization: Bearer <access_token>
```

В демо-данных команды уже имеют сыгранную игру, очки, победы и поражения.

### 4. Посмотреть вопросы

```http
GET http://localhost:8000/questions
Authorization: Bearer <access_token>
```

### 5. Посмотреть игры

```http
GET http://localhost:8000/games
Authorization: Bearer <access_token>
```

В списке должна быть завершенная игра со статусом `finished` и черновая игра со статусом `draft`.

### 6. Посмотреть результаты завершенной игры

Сначала вызовите `GET /games`, найдите игру со статусом `finished` и подставьте ее `id`.

```http
GET http://localhost:8000/games/{finished_game_id}/results
Authorization: Bearer <access_token>
```

Ответ содержит команды, очки и ответы команд на вопросы:

```json
{
  "game_id": 1,
  "status": "finished",
  "teams": [
    {
      "team_id": 1,
      "name": "Team A",
      "points": 2
    }
  ],
  "answers": [
    {
      "team_name": "Team A",
      "question_text": "2+2?",
      "option_text": "4",
      "is_correct": true,
      "within_time": true,
      "elapsed_ms": 8200
    }
  ]
}
```

### 7. Проверить живую игру

Сначала вызовите `GET /games`, найдите игру со статусом `draft` и подставьте ее `id`.

```http
POST http://localhost:8000/games/{draft_game_id}/start
Authorization: Bearer <access_token>
```

Текущий вопрос:

```http
GET http://localhost:8000/games/{draft_game_id}/current-question
Authorization: Bearer <access_token>
```

Ответ команды:

```http
POST http://localhost:8000/games/{draft_game_id}/answer
Authorization: Bearer <access_token>
Content-Type: application/json
```

```json
{
  "team_id": 1,
  "option_id": 2
}
```

`option_id` берется из ответа `/questions` или `/games/{draft_game_id}/current-question`.

Следующий вопрос:

```http
POST http://localhost:8000/games/{draft_game_id}/next-question
Authorization: Bearer <access_token>
```

## WebSocket

В Postman откройте New -> WebSocket и подключитесь:

```text
ws://localhost:8000/ws/games/{draft_game_id}
```

После подключения вызовите:

```http
POST http://localhost:8000/games/{draft_game_id}/start
```

или:

```http
POST http://localhost:8000/games/{draft_game_id}/next-question
```

В WebSocket-вкладке должны приходить JSON-сообщения с текущим вопросом.

## Основные эндпоинты

Auth:

- `POST /auth/register`
- `POST /auth/login`
- `POST /auth/refresh`
- `POST /auth/change-password`
- `GET /auth/me`

Users:

- `GET /users`
- `POST /users`
- `GET /users/{user_id}`
- `PATCH /users/{user_id}`
- `DELETE /users/{user_id}`

Teams:

- `GET /teams`
- `POST /teams`
- `GET /teams/{team_id}`
- `PATCH /teams/{team_id}`
- `DELETE /teams/{team_id}`

Questions:

- `GET /questions`
- `POST /questions`
- `GET /questions/{question_id}`
- `PATCH /questions/{question_id}`
- `DELETE /questions/{question_id}`

Games:

- `GET /games`
- `POST /games`
- `GET /games/{game_id}`
- `GET /games/{game_id}/results`
- `PATCH /games/{game_id}`
- `DELETE /games/{game_id}`
- `POST /games/{game_id}/start`
- `POST /games/{game_id}/next-question`
- `GET /games/{game_id}/current-question`
- `POST /games/{game_id}/answer`

Realtime:

- `WS /ws/games/{game_id}`

## Локальный запуск без Docker

Нужны Python 3.12+, PostgreSQL и Redis.

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
$env:DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/quiz"
$env:REDIS_HOST="localhost"
$env:REDIS_PORT="6379"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\python.exe -m app.seed_demo
.\.venv\Scripts\uvicorn.exe app.main:app --reload --host 127.0.0.1 --port 8000
```

## Тесты

Локально:

```powershell
.\.venv\Scripts\pytest.exe -q
```

Через Docker:

```powershell
docker compose run --rm tests
```
