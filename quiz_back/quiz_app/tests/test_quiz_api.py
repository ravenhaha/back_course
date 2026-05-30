async def _register(client, username: str, password: str, role: str):
    r = await client.post(
        "/auth/register",
        json={"username": username, "password": password, "role": role},
    )
    assert r.status_code == 201, r.text
    return r.json()


async def _login(client, username: str, password: str):
    r = await client.post("/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


def _auth_headers(access_token: str):
    return {"Authorization": f"Bearer {access_token}"}


async def test_auth_tokens_and_me(client):
    await _register(client, "admin", "password123", "admin")
    tokens = await _login(client, "admin", "password123")
    assert "access_token" in tokens and "refresh_token" in tokens

    r = await client.get("/auth/me", headers=_auth_headers(tokens["access_token"]))
    assert r.status_code == 200
    assert r.json()["username"] == "admin"

    r = await client.post("/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
    assert r.status_code == 200
    new_tokens = r.json()
    assert new_tokens["access_token"] != tokens["access_token"]

    r = await client.post(
        "/auth/change-password",
        headers=_auth_headers(new_tokens["access_token"]),
        json={"current_password": "password123", "new_password": "password456"},
    )
    assert r.status_code == 204

    # old access token should be revoked after password change
    r = await client.get("/auth/me", headers=_auth_headers(new_tokens["access_token"]))
    assert r.status_code == 401

    tokens2 = await _login(client, "admin", "password456")
    r = await client.get("/auth/me", headers=_auth_headers(tokens2["access_token"]))
    assert r.status_code == 200


async def test_crud_and_game_flow(client):
    # admin bootstrap
    await _register(client, "admin2", "password123", "admin")
    tokens = await _login(client, "admin2", "password123")
    admin_h = _auth_headers(tokens["access_token"])

    # teams
    r = await client.post("/teams", headers=admin_h, json={"name": "Team A"})
    assert r.status_code == 201, r.text
    team_a = r.json()

    r = await client.post("/teams", headers=admin_h, json={"name": "Team B"})
    assert r.status_code == 201, r.text
    team_b = r.json()

    r = await client.get("/teams", headers=admin_h)
    assert r.status_code == 200
    assert len(r.json()) == 2

    # questions
    r = await client.post(
        "/questions",
        headers=admin_h,
        json={"text": "2+2?", "options": ["3", "4"], "correct_option_index": 1},
    )
    assert r.status_code == 201, r.text
    q1 = r.json()
    correct_opt_id = q1["options"][1]["id"]

    r = await client.post(
        "/questions",
        headers=admin_h,
        json={"text": "Capital of France?", "options": ["Paris", "Rome"], "correct_option_index": 0},
    )
    assert r.status_code == 201, r.text
    q2 = r.json()

    # game create
    r = await client.post(
        "/games",
        headers=admin_h,
        json={
            "scheduled_at": "2026-01-01T00:00:00Z",
            "team_ids": [team_a["id"], team_b["id"]],
            "question_ids": [q1["id"], q2["id"]],
        },
    )
    assert r.status_code == 201, r.text
    game = r.json()

    # start game (publishes current question into Redis channel)
    r = await client.post(f"/games/{game['id']}/start", headers=admin_h)
    assert r.status_code == 200, r.text
    start = r.json()
    assert start["current_question_id"] == q1["id"]

    # player register/login
    await _register(
        client,
        "player1",
        "password123",
        "player",
    )
    ptokens = await _login(client, "player1", "password123")
    player_h = _auth_headers(ptokens["access_token"])

    # submit answer (correct, within time)
    r = await client.post(
        f"/games/{game['id']}/answer",
        headers=player_h,
        json={"team_id": team_a["id"], "option_id": correct_opt_id},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_correct"] is True
    assert body["within_time"] is True

    # cannot answer the same question twice
    r = await client.post(
        f"/games/{game['id']}/answer",
        headers=player_h,
        json={"team_id": team_a["id"], "option_id": correct_opt_id},
    )
    assert r.status_code == 409

    # go next question and then finish
    r = await client.post(f"/games/{game['id']}/next-question", headers=admin_h)
    assert r.status_code == 200
    assert r.json()["current_question_id"] == q2["id"]

    r = await client.post(f"/games/{game['id']}/next-question", headers=admin_h)
    assert r.status_code == 200
    assert r.json()["status"] == "finished"


async def test_users_crud_admin_only(client):
    await _register(client, "admin3", "password123", "admin")
    tokens = await _login(client, "admin3", "password123")
    admin_h = _auth_headers(tokens["access_token"])

    r = await client.post("/users", headers=admin_h, json={"username": "u11", "password": "password123", "role": "player"})
    assert r.status_code == 201, r.text
    user = r.json()

    r = await client.get(f"/users/{user['id']}", headers=admin_h)
    assert r.status_code == 200

    r = await client.patch(f"/users/{user['id']}", headers=admin_h, json={"full_name": "Test User"})
    assert r.status_code == 200
    assert r.json()["full_name"] == "Test User"

    r = await client.delete(f"/users/{user['id']}", headers=admin_h)
    assert r.status_code == 204
