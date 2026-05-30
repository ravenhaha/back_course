import json


def test_ws_forwards_pubsub(sync_client):
    # bootstrap admin
    r = sync_client.post("/auth/register", json={"username": "admin_ws", "password": "password123", "role": "admin"})
    assert r.status_code == 201, r.text
    r = sync_client.post("/auth/login", json={"username": "admin_ws", "password": "password123"})
    assert r.status_code == 200, r.text
    access = r.json()["access_token"]
    h = {"Authorization": f"Bearer {access}"}

    # create team + question + game
    r = sync_client.post("/teams", headers=h, json={"name": "WS Team"})
    assert r.status_code == 201, r.text
    team_id = r.json()["id"]

    r = sync_client.post(
        "/questions",
        headers=h,
        json={"text": "WS Q?", "options": ["a", "b"], "correct_option_index": 0},
    )
    assert r.status_code == 201, r.text
    qid = r.json()["id"]

    r = sync_client.post(
        "/games",
        headers=h,
        json={"scheduled_at": "2026-01-01T00:00:00Z", "team_ids": [team_id], "question_ids": [qid]},
    )
    assert r.status_code == 201, r.text
    game_id = r.json()["id"]

    # start publishes the current question into Redis channel
    r = sync_client.post(f"/games/{game_id}/start", headers=h)
    assert r.status_code == 200, r.text

    with sync_client.websocket_connect(f"/ws/games/{game_id}") as ws:
        msg = ws.receive_text()
        payload = json.loads(msg)
        assert payload["type"] == "question"
        assert payload["game_id"] == game_id
        assert payload["question_id"] == qid

