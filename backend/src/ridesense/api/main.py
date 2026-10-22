"""REST/WebSocket API for commuter and administrator dashboards."""

from __future__ import annotations

import hmac
import hashlib
import base64
import binascii
import json
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from pydantic import BaseModel, Field

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ridesense.common.schemas import ControlCommand
from ridesense.agents.service import answer_query, refresh_advisories
from ridesense.common.journey import find_journey_options, find_transfer_options, search_stops
from ridesense.common.regions import REGIONS, allocate_fleet

WEB = Path(os.getenv("WEB_DIR", str(Path(__file__).resolve().parents[4] / "frontend")))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ridesense:ridesense-local-only@timescaledb:5432/ridesense")
SESSION_SECRET = os.getenv("SESSION_SECRET", "local-session-secret-change-before-deploy")
SESSION_COOKIE = "bus_pulse_session"
SESSION_TTL_SECONDS = 60 * 60 * 12
BUS_LIVE_TTL_SECONDS = int(os.getenv("BUS_LIVE_TTL_SECONDS", "75"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = ConnectionPool(DATABASE_URL, kwargs={"row_factory": dict_row}, min_size=1, max_size=8)
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS commuter_users (
                username text PRIMARY KEY,
                display_name text NOT NULL,
                password_hash text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now()
            )"""
        )
    app.state.kafka = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                                "client.id": "bus-pulse-api"})
    yield
    app.state.db.close()
    app.state.kafka.flush(5)


app = FastAPI(title="Bus Pulse", version="0.1.0", lifespan=lifespan)


class AssistantQuestion(BaseModel):
    question: str = Field(min_length=2, max_length=500)
    session_id: str | None = Field(default=None, max_length=80)
    selected_route_id: str | None = Field(default=None, max_length=120)
    from_stop_id: str | None = Field(default=None, max_length=100)
    to_stop_id: str | None = Field(default=None, max_length=100)


class AuthRequest(BaseModel):
    username: str = Field(min_length=3, max_length=80)
    password: str = Field(min_length=1, max_length=200)
    role: str = "commuter"
    display_name: str = Field(default="", max_length=120)


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _password_hash(password: str, salt: bytes | None = None) -> str:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000)
    return f"pbkdf2_sha256${_b64(salt)}${_b64(digest)}"


def _password_matches(password: str, stored: str) -> bool:
    try:
        algorithm, salt_text, digest_text = stored.split("$")
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.urlsafe_b64decode(salt_text + "=" * (-len(salt_text) % 4))
        expected = base64.urlsafe_b64decode(digest_text + "=" * (-len(digest_text) % 4))
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000)
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError, binascii.Error):
        return False


def _make_session(username: str, role: str) -> str:
    payload = _b64(json.dumps({"sub": username, "role": role,
                              "exp": int(datetime.now(timezone.utc).timestamp()) + SESSION_TTL_SECONDS},
                             separators=(",", ":")).encode())
    signature = _b64(hmac.new(SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{signature}"


def session_user(request: Request) -> dict | None:
    token = request.cookies.get(SESSION_COOKIE, "")
    try:
        payload, signature = token.split(".", 1)
        expected = _b64(hmac.new(SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            return None
        data = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        if data.get("exp", 0) < datetime.now(timezone.utc).timestamp() or data.get("role") not in {"admin", "commuter"}:
            return None
        return data
    except (ValueError, TypeError, json.JSONDecodeError, binascii.Error):
        return None


def require_admin(request: Request):
    user = session_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Administrator sign-in required")


def require_session(request: Request):
    user = session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Sign in to use the commuter simulation")
    return user


@app.post("/api/auth/register", status_code=201)
def register(body: AuthRequest):
    if body.role != "commuter":
        raise HTTPException(status_code=403, detail="Administrator accounts are configured by the operator")
    if len(body.password) < 8:
        raise HTTPException(status_code=422, detail="Commuter passwords must be at least 8 characters")
    username = body.username.strip().lower()
    if not username or not all(c.isalnum() or c in "._-" for c in username):
        raise HTTPException(status_code=422, detail="Username may contain letters, numbers, dots, dashes, and underscores")
    password_hash = _password_hash(body.password)
    try:
        with app.state.db.connection() as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO commuter_users(username,display_name,password_hash) VALUES (%s,%s,%s)",
                        (username, body.display_name.strip() or username, password_hash))
    except Exception as exc:
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(status_code=409, detail="That username is already registered") from exc
        raise
    return {"created": True, "username": username}


@app.post("/api/auth/login")
def login(body: AuthRequest, response: Response):
    username = body.username.strip().lower()
    if body.role == "admin":
        configured_user = os.getenv("ADMIN_USERNAME", "admin").strip().lower()
        configured_password = os.getenv("ADMIN_PASSWORD", "")
        valid = bool(configured_password) and hmac.compare_digest(username, configured_user) and hmac.compare_digest(body.password, configured_password)
        if not valid:
            raise HTTPException(status_code=401, detail="Administrator username or password is incorrect")
    elif body.role == "commuter":
        with app.state.db.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT password_hash FROM commuter_users WHERE username=%s", (username,))
            row = cur.fetchone()
        if not row or not _password_matches(body.password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Username or password is incorrect")
    else:
        raise HTTPException(status_code=422, detail="role must be commuter or admin")
    response.set_cookie(SESSION_COOKIE, _make_session(username, body.role), httponly=True, secure=os.getenv("COOKIE_SECURE", "false").lower() == "true", samesite="lax", max_age=SESSION_TTL_SECONDS, path="/")
    return {"authenticated": True, "user": {"username": username, "role": body.role}}


@app.get("/api/auth/me")
def auth_me(request: Request):
    user = session_user(request)
    return {"authenticated": bool(user), "user": ({"username": user["sub"], "role": user["role"]} if user else None)}


@app.post("/api/auth/logout")
def logout(response: Response):
    response.delete_cookie(SESSION_COOKIE, httponly=True,
                           secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
                           samesite="lax", path="/")
    return {"logged_out": True}


@app.get("/api/auth/logout")
def logout_from_link():
    response = RedirectResponse("/", status_code=303)
    response.delete_cookie(SESSION_COOKIE, httponly=True,
                          secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
                          samesite="lax", path="/")
    return response


@app.get("/api/health")
def health():
    try:
        with app.state.db.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) AS route_count FROM routes")
            route_count = cur.fetchone()["route_count"]
            cur.execute("SELECT count(*) AS live_buses,max(updated_at) AS latest_update FROM vehicle_latest WHERE updated_at > now() - (%s * interval '1 second')", (BUS_LIVE_TTL_SECONDS,))
            live = cur.fetchone()
            cur.execute("""SELECT running,paused,rush_hour,demand_profile,region_id,speedup,total_buses,active_routes,last_heartbeat
                         FROM simulation_status WHERE singleton_id=1""")
            simulation = cur.fetchone()
            if simulation:
                simulation = {**simulation, "running": bool(simulation["running"] and simulation["last_heartbeat"]
                             and simulation["last_heartbeat"].timestamp() > datetime.now(timezone.utc).timestamp() - 90),
                              "region_name": REGIONS.get(simulation["region_id"], REGIONS["whole_city"])["name"],
                              "regions": [{"id": key, "name": value["name"], "center": value["center"],
                                          "zoom": value["zoom"], "polygon": value["polygon"]}
                                          for key, value in REGIONS.items()]}
        return {"ok": True, "database": "ready", "route_count": route_count,
                "live_buses": live["live_buses"], "latest_update": live["latest_update"],
                "simulation": simulation}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}") from exc


@app.get("/api/network/routes")
def network_routes(q: str = Query(default="", max_length=80), usable_only: bool = True,
                   limit: int = Query(default=500, ge=1, le=5000),
                   region_id: str | None = Query(default=None, max_length=32)):
    term = f"%{q.strip()}%"
    normalized = "".join(character for character in q.upper() if character.isalnum())
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT route_id,short_name,long_name,usable,fail_reasons,simulated,trips_in_feed,
                      (SELECT count(*) FROM vehicle_latest v WHERE v.route_id=routes.route_id
                       AND v.updated_at>now()-(%s * interval '1 second')) AS live_buses
               FROM routes WHERE (%s = '' OR route_id ILIKE %s OR short_name ILIKE %s OR long_name ILIKE %s
                    OR regexp_replace(upper(route_id || short_name || long_name),'[^A-Z0-9]','','g') LIKE %s)
               AND (NOT %s OR usable=true)
               AND (%s::text IS NULL OR EXISTS (SELECT 1 FROM route_regions rr WHERE rr.route_id=routes.route_id AND rr.region_id=%s))
               ORDER BY short_name,route_id LIMIT %s""",
            (BUS_LIVE_TTL_SECONDS, q.strip(), term, term, term,
             f"%{normalized}%", usable_only, region_id, region_id, limit),
        )
        return {"routes": cur.fetchall(), "simulated": False, "source": "static_gtfs"}


@app.get("/api/network/geojson")
def network_geojson(usable_only: bool = True, simulated_only: bool = False,
                    route_id: str | None = None, region_id: str | None = None):
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT d.route_id,r.short_name,r.simulated,d.direction_id,d.usable,d.shape,d.length_m
               FROM route_directions d JOIN routes r USING(route_id)
               WHERE (NOT %s OR d.usable=true) AND (NOT %s OR r.simulated=true)
               AND (%s::text IS NULL OR r.route_id=%s)
               AND (%s::text IS NULL OR EXISTS (SELECT 1 FROM route_regions rr WHERE rr.route_id=r.route_id AND rr.region_id=%s))""",
            (usable_only, simulated_only, route_id, route_id, region_id, region_id),
        )
        features = []
        for row in cur.fetchall():
            features.append({
                "type": "Feature",
                "properties": {"route_id": row["route_id"], "short_name": row["short_name"],
                               "simulated": row["simulated"],
                               "direction_id": row["direction_id"], "usable": row["usable"],
                               "length_m": row.get("length_m")},
                "geometry": {"type": "LineString", "coordinates": row["shape"]},
            })
    return {"type": "FeatureCollection", "features": features, "simulated": False,
            "source": "static_gtfs"}


@app.get("/api/network/stops")
def network_stops(q: str = Query(default="", max_length=100),
                  limit: int = Query(default=30, ge=1, le=100)):
    with app.state.db.connection() as conn, conn.cursor() as cur:
        return {"stops": search_stops(cur, q, limit)}


@app.get("/api/network/stops/nearby")
def nearby_stops(lat: float = Query(ge=12.6, le=13.4), lon: float = Query(ge=77.2, le=78.1),
                 limit: int = Query(default=5, ge=1, le=20)):
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT s.stop_id,s.name,s.lat,s.lon,
                      sqrt(power((s.lat-%s)*111.32,2)+power((s.lon-%s)*111.32*cos(radians(%s)),2)) AS distance_km,
                      (SELECT count(DISTINCT rs.route_id) FROM route_stops rs WHERE rs.stop_id=s.stop_id) AS route_count
               FROM stops s WHERE s.lat BETWEEN 12.6 AND 13.4 AND s.lon BETWEEN 77.2 AND 78.1
               ORDER BY distance_km,route_count DESC LIMIT %s""",
            (lat, lon, lat, limit),
        )
        return {"stops": cur.fetchall(), "picked_at": {"lat": lat, "lon": lon}}


def _attach_journey_bus_info(cur, options: list[dict], *, region_id: str | None,
                             demand_profile: str = "normal", running: bool = True) -> list[dict]:
    for option in options:
        average_speed = {"normal": 14.0, "moderate": 12.0, "rush": 10.0}.get(demand_profile, 14.0)
        option["estimated_travel_min"] = round(
            max(1.0, float(option.get("journey_distance_km") or 0) /
                average_speed * 60 + max(0, len(option.get("journey_stops") or [])-1)*0.45), 1)
        if not running:
            option["nearest_bus"] = None
            option["estimated_total_min"] = option["estimated_travel_min"]
            continue
        cur.execute(
            """SELECT v.vehicle_id,v.route_id,v.direction_id,v.event_ts,v.lat,v.lon,v.progress_m,v.speed_mps,
                      v.advisory_extra,v.occ_est_pct,v.onboard_count,v.capacity,s.progress_m AS start_progress_m,d.length_m
               FROM vehicle_latest v
               JOIN route_stops s ON s.route_id=v.route_id AND s.direction_id=%s AND s.seq=%s
               JOIN route_directions d ON d.route_id=v.route_id AND d.direction_id=%s
               WHERE v.route_id=%s AND v.updated_at>now()-(%s * interval '1 second')
               ORDER BY v.event_ts DESC LIMIT 40""",
            (option["direction_id"], option["start_seq"], option["direction_id"], option["route_id"],
             BUS_LIVE_TTL_SECONDS),
        )
        candidates = []
        for bus in cur.fetchall():
            length = float(bus["length_m"] or option.get("route_length_m") or 0)
            start_progress = float(bus["start_progress_m"])
            bus_progress = float(bus["progress_m"])
            if bus["direction_id"] == option["direction_id"]:
                distance = start_progress-bus_progress if bus_progress <= start_progress else max(0, 2*length-bus_progress+start_progress)
            else:
                distance = max(0, length-bus_progress)+start_progress
            speed = float(bus["speed_mps"] or 0)
            eta = distance / max(speed, 3.5) / 60 + (0.0 if speed > 0.5 else 0.25)
            candidates.append({**bus, "distance_to_board_m": round(distance),
                               "eta_to_board_min": round(eta, 1), "moving": speed > 0.5})
        candidates.sort(key=lambda bus: (bus["distance_to_board_m"], bus["occ_est_pct"]))
        option["nearest_bus"] = candidates[0] if candidates else None
        option["estimated_total_min"] = round(option["estimated_travel_min"] +
            (candidates[0]["eta_to_board_min"] if candidates and running else 0), 1)
    return options


@app.get("/api/journey/routes")
def journey_routes(from_stop_id: str = Query(min_length=1, max_length=100),
                   to_stop_id: str = Query(min_length=1, max_length=100),
                   sort_by: str = Query(default="fastest", pattern="^(fastest|nearest)$")):
    if from_stop_id == to_stop_id:
        raise HTTPException(status_code=422, detail="Choose two different stops")
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT region_id,rush_hour,demand_profile,running,last_heartbeat FROM simulation_status WHERE singleton_id=1")
        status = cur.fetchone() or {"region_id": "whole_city", "rush_hour": False,
                                    "demand_profile": "normal", "running": False, "last_heartbeat": None}
        status["running"] = bool(status["running"] and status["last_heartbeat"] and
                                  status["last_heartbeat"].timestamp() > datetime.now(timezone.utc).timestamp()-90)
        routes = find_journey_options(cur, from_stop_id, to_stop_id, region_id=status["region_id"])
        routes = _attach_journey_bus_info(cur, routes, region_id=status["region_id"],
                                          demand_profile=status.get("demand_profile", "normal"),
                                          running=status["running"])
        if not routes:
            transfer_options = find_transfer_options(cur, from_stop_id, to_stop_id)
            for transfer in transfer_options:
                legs = _attach_journey_bus_info(
                    cur, [dict(transfer["leg1"]), dict(transfer["leg2"])],
                    region_id=status["region_id"], demand_profile=status.get("demand_profile", "normal"),
                    running=status["running"],
                )
                leg_travel = sum(float(leg["estimated_travel_min"]) for leg in legs)
                first_bus = legs[0].get("nearest_bus")
                first_wait = float(first_bus["eta_to_board_min"]) if first_bus else 0.0
                total_minutes = round(leg_travel + first_wait + 8.0, 1)
                transfer["legs"] = legs
                transfer["transfer_count"] = 1
                transfer["transfer_stop"] = {
                    "stop_id": transfer["transfer_stop_id"],
                    "name": transfer["transfer_stop_name"],
                    "lat": transfer["transfer_lat"], "lon": transfer["transfer_lon"],
                }
                transfer["short_name"] = f"{legs[0]['short_name']} → {legs[1]['short_name']}"
                transfer["long_name"] = f"Change buses at {transfer['transfer_stop_name']}"
                transfer["simulated"] = all(bool(leg["simulated"]) for leg in legs)
                transfer["journey_distance_km"] = float(transfer["total_distance_km"])
                transfer["live_buses"] = sum(int(leg.get("live_buses") or 0) for leg in legs)
                transfer["nearest_bus"] = first_bus
                transfer["estimated_travel_min"] = round(leg_travel + 8.0, 1)
                transfer["estimated_total_min"] = total_minutes
                routes.append(transfer)
        routes.sort(key=lambda route: (
            route["nearest_bus"]["eta_to_board_min"] if sort_by == "nearest" and route.get("nearest_bus") else 99999,
            route["estimated_total_min"] if sort_by == "fastest" else route["estimated_travel_min"],
            not route["simulated"], route["short_name"] or ""))
        return {"routes": routes, "from_stop_id": from_stop_id, "to_stop_id": to_stop_id,
                "match_radius_km": 0.45, "sort_by": sort_by,
                "simulation_region": status["region_id"], "simulation_running": status["running"],
                "used_transfer_fallback": bool(routes and routes[0].get("transfer_count"))}


@app.get("/api/buses")
def buses(route_id: str | None = None, limit: int = Query(default=200, ge=1, le=500)):
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT vehicle_id,route_id,region_id,direction_id,event_ts,lat,lon,progress_m,speed_mps,occ_est_pct,
                      seated_count,standing_count,supported_standing_count,delay_s,advisory_extra
               FROM vehicle_latest WHERE updated_at > now() - (%s * interval '1 second')
               AND (%s::text IS NULL OR route_id=%s) ORDER BY route_id,vehicle_id LIMIT %s""",
            (BUS_LIVE_TTL_SECONDS, route_id, route_id, limit),
        )
        return {"buses": cur.fetchall(), "simulated": True}


@app.get("/api/buses/{vehicle_id}")
def bus_detail(vehicle_id: str):
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM vehicle_latest WHERE vehicle_id=%s AND updated_at > now() - (%s * interval '1 second')", (vehicle_id, BUS_LIVE_TTL_SECONDS))
        vehicle = cur.fetchone()
        if vehicle is None:
            raise HTTPException(status_code=404, detail="Bus has no live simulated data")
        cur.execute(
            "SELECT window_start,occ_mean,occ_max,n_events FROM window_aggregates WHERE vehicle_id=%s ORDER BY window_start DESC LIMIT 12",
            (vehicle_id,),
        )
        trend = list(reversed(cur.fetchall()))
        cur.execute(
            "SELECT made_at,target,pred_pct,model_version,contributions FROM predictions WHERE vehicle_id=%s AND made_at>now()-interval '15 minutes' ORDER BY made_at DESC LIMIT 1",
            (vehicle_id,),
        )
        prediction = cur.fetchone()
        cur.execute("SELECT occ_est_pct FROM occupancy_state WHERE vehicle_id=%s AND event_ts>now()-interval '20 minutes' ORDER BY event_ts DESC LIMIT 40", (vehicle_id,))
        history = [float(row["occ_est_pct"]) for row in cur.fetchall() if row["occ_est_pct"] is not None]
    if history:
        import statistics
        spread = max(5.0, statistics.pstdev(history) if len(history) > 1 else 8.0)
        base = float(prediction["pred_pct"]) if prediction else statistics.fmean(history)
    else:
        spread, base = 8.0, float(vehicle["occ_est_pct"])
    base = max(0.0, min(120.0, base))
    capacity = int(vehicle.get("capacity") or 70)
    low_pct, high_pct = max(0.0, base-spread), min(120.0, base+spread)
    estimate = {"occupancy_pct_low": round(low_pct, 1),
                "occupancy_pct_high": round(high_pct, 1),
                "onboard_low": max(0, int(capacity*low_pct/100)),
                "onboard_high": max(0, int(capacity*high_pct/100)+1),
                "basis": prediction["model_version"] if prediction else "recent-stream trend baseline",
                "label": "rough simulated range; not a statistical confidence interval"}
    return {"vehicle": vehicle, "trend": trend, "prediction": prediction,
            "estimate": estimate, "simulated": True}


@app.get("/api/routes/{route_id}/summary")
def route_summary(route_id: str):
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT region_id FROM simulation_status WHERE singleton_id=1")
        sim = cur.fetchone() or {"region_id": "whole_city"}
        region_id = sim["region_id"]
        cur.execute("SELECT route_id,short_name,long_name,usable,simulated FROM routes WHERE route_id=%s", (route_id,))
        route = cur.fetchone()
        if route is None:
            raise HTTPException(status_code=404, detail="Unknown BMTC route")
        cur.execute("SELECT avg(occ_est_pct) AS mean_pct,max(occ_est_pct) AS max_pct,count(*) AS buses FROM vehicle_latest WHERE route_id=%s AND updated_at > now() - (%s * interval '1 second')", (route_id, BUS_LIVE_TTL_SECONDS))
        summary = cur.fetchone()
        cur.execute("SELECT vehicle_id,event_ts,lat,lon,progress_m,region_id,direction_id,occ_est_pct,speed_mps,capacity,seat_capacity,stand_capacity,support_capacity,vehicle_type,capacity_range_min,capacity_range_max,advisory_extra FROM vehicle_latest WHERE route_id=%s AND updated_at > now() - (%s * interval '1 second') ORDER BY event_ts DESC", (route_id, BUS_LIVE_TTL_SECONDS))
        route_buses = cur.fetchall()
        cur.execute("""SELECT rs.stop_id,s.name,s.lat,s.lon,rs.seq FROM route_stops rs JOIN stops s USING(stop_id)
                       JOIN route_directions d USING(route_id,direction_id)
                       WHERE rs.route_id=%s AND d.usable=true AND rs.direction_id=(
                         SELECT direction_id FROM route_directions WHERE route_id=%s AND usable=true ORDER BY direction_id LIMIT 1)
                       ORDER BY rs.seq LIMIT 200""", (route_id, route_id))
        stops = cur.fetchall()
    return {"route": route, "summary": summary, "buses": route_buses,
            "stops": stops, "simulated": route["simulated"]}


@app.get("/api/alerts")
def alerts(active_only: bool = True, limit: int = Query(default=100, ge=1, le=500),
           region_id: str | None = Query(default=None, max_length=32)):
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT * FROM alerts a WHERE (NOT %s OR (acknowledged_at IS NULL AND resolved_at IS NULL))
               AND (%s::text IS NULL OR EXISTS (SELECT 1 FROM route_regions rr
                   WHERE rr.route_id=a.route_id AND rr.region_id=%s))
               ORDER BY detected_at DESC LIMIT %s""",
            (active_only, region_id, region_id, limit),
        )
        return {"alerts": cur.fetchall()}


@app.post("/api/alerts/{event_id}/ack")
def acknowledge_alert(event_id: str, request: Request):
    require_admin(request)
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alerts SET acknowledged_at=now() WHERE event_id=%s AND acknowledged_at IS NULL RETURNING event_id",
                    (event_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="Alert is no longer open")
    return {"event_id": event_id, "acknowledged": True}


@app.post("/api/assistant/chat")
def assistant_chat(body: AssistantQuestion):
    with app.state.db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT region_id,running,rush_hour,demand_profile,last_heartbeat FROM simulation_status WHERE singleton_id=1")
            status = cur.fetchone()
        is_running = bool(status and status["running"] and status["last_heartbeat"] and
                          status["last_heartbeat"].timestamp() > datetime.now(timezone.utc).timestamp()-90)
        result = answer_query(conn, body.question.strip(), body.session_id,
                              context={"region_id": status["region_id"] if status else "whole_city",
                                       "running": is_running,
                                       "rush_hour": bool(status and status["rush_hour"]),
                                       "demand_profile": (status or {}).get("demand_profile", "normal"),
                                       "selected_route_id": body.selected_route_id,
                                       "from_stop_id": body.from_stop_id, "to_stop_id": body.to_stop_id})
    provider = result.pop("provider", "local-tool-router")
    return {**result, "simulated_data": True, "provider": provider}


@app.get("/api/admin/dq")
def admin_dq(request: Request):
    require_admin(request)
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM dq_metrics ORDER BY ts DESC LIMIT 1")
        latest = cur.fetchone()
    return {"metrics": latest}


@app.get("/api/admin/advisories")
def admin_advisories(request: Request):
    require_admin(request)
    with app.state.db.connection() as conn:
        refresh_advisories(conn)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT advisory_id,event_id,route_id,action,rationale,evidence_ids,source,created_at,status
                   FROM advisories WHERE status='pending' ORDER BY created_at DESC LIMIT 50"""
            )
            return {"advisories": cur.fetchall(), "human_approval_required": True}


@app.get("/api/admin/advisory-buses")
def admin_advisory_buses(request: Request, region_id: str | None = Query(default=None, max_length=32)):
    require_admin(request)
    if region_id is not None and region_id not in REGIONS:
        raise HTTPException(status_code=422, detail="Choose a supported Bengaluru simulation region")
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT v.vehicle_id,v.route_id,r.short_name,r.long_name,v.region_id,
                      v.direction_id,v.event_ts,v.updated_at,v.lat,v.lon,v.progress_m,v.speed_mps,
                      v.occ_est_pct,v.onboard_count,v.capacity,v.seated_count,v.standing_count,
                      v.supported_standing_count,v.advisory_extra
               FROM vehicle_latest v JOIN routes r USING(route_id)
               WHERE v.advisory_extra=true
                 AND v.updated_at>now()-(%s * interval '1 second')
                 AND (%s::text IS NULL OR v.region_id=%s OR v.region_id='whole_city'
                      OR EXISTS (SELECT 1 FROM route_regions live_rr
                                 WHERE live_rr.route_id=v.route_id AND live_rr.region_id=%s))
               ORDER BY v.updated_at DESC LIMIT 200""",
            (BUS_LIVE_TTL_SECONDS, region_id, region_id, region_id),
        )
        return {"buses": cur.fetchall(), "region_id": region_id,
                "updated_at": datetime.now(timezone.utc).isoformat(), "simulated": True}


@app.post("/api/admin/advisories/{advisory_id}/review")
def review_advisory(advisory_id: str, body: dict,
                    request: Request):
    require_admin(request)
    status = body.get("status")
    if status not in {"approved", "rejected"}:
        raise HTTPException(status_code=422, detail="status must be approved or rejected")
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT route_id,status FROM advisories WHERE advisory_id=%s FOR UPDATE", (advisory_id,))
        advisory = cur.fetchone()
        if advisory is None:
            raise HTTPException(status_code=404, detail="Advisory not found")
        if advisory["status"] != "pending":
            cur.execute("SELECT region_id FROM simulation_status WHERE singleton_id=1")
            previous_region = cur.fetchone()
            return {"advisory_id": advisory_id, "status": advisory["status"],
                    "route_id": str(advisory["route_id"]),
                    "region_id": previous_region["region_id"] if previous_region else None,
                    "already_processed": True,
                    "message": "This advisory was already processed; no duplicate bus request was sent."}
        if status == "approved":
            cur.execute("SELECT region_id FROM simulation_status WHERE singleton_id=1")
            simulation = cur.fetchone() or {"region_id": "whole_city"}
            cur.execute("SELECT region_id FROM route_regions WHERE route_id=%s ORDER BY region_id",
                        (advisory["route_id"],))
            route_regions = [row["region_id"] for row in cur.fetchall() if row["region_id"] in REGIONS]
            region_id = simulation["region_id"] if simulation["region_id"] in route_regions else (route_regions[0] if route_regions else None)
            if not region_id:
                raise HTTPException(status_code=409, detail="This route is not assigned to a supported simulation region")
            routes, max_buses = _region_fleet_plan(cur, region_id)
            route_by_id = {str(route["route_id"]): route for route in routes}
            selected = route_by_id.get(str(advisory["route_id"]))
            if selected is None:
                raise HTTPException(status_code=409, detail="The advisory route is not in the active simulated fleet")
            counts = {str(route["route_id"]): max(
                int(route["minimum_buses"]), int(route["live_buses"] or route["recommended_buses"]))
                for route in routes}
            counts[str(advisory["route_id"])] += 1
            if sum(counts.values()) > max_buses:
                raise HTTPException(status_code=422, detail=f"Adding a bus would exceed the configured {max_buses}-bus fleet limit")
            command = _control_command("set_fleet", region_id=region_id, route_bus_counts=counts,
                                       parameters={"advisory_extra_route_id": str(advisory["route_id"])})
            _publish_control(command)
        cur.execute("UPDATE advisories SET status=%s WHERE advisory_id=%s AND status='pending'",
                    (status, advisory_id))
    return {"advisory_id": advisory_id, "status": status,
            "route_id": str(advisory["route_id"]),
            "region_id": region_id if status == "approved" else None,
            "already_processed": False,
            "message": ("Approved: one advisory-marked bus was requested for this route." if status == "approved"
                        else "Advisory dismissed.")}


def _region_fleet_plan(cur, region_id: str) -> tuple[list[dict], int]:
    cur.execute(
        """SELECT r.route_id,r.short_name,r.long_name,r.simulated,r.usable,
                  max(d.length_m) AS length_m,
                  count(DISTINCT rs.stop_id) FILTER (WHERE COALESCE(shared.route_count,1)>1) AS connection_stops,
                  count(DISTINCT v.vehicle_id) AS live_buses
           FROM route_regions rr JOIN routes r USING(route_id)
           JOIN route_directions d ON d.route_id=r.route_id AND d.usable=true
           LEFT JOIN route_stops rs ON rs.route_id=d.route_id AND rs.direction_id=d.direction_id
           LEFT JOIN (SELECT stop_id,count(DISTINCT route_id) AS route_count FROM route_stops
                      JOIN routes USING(route_id) WHERE usable=true GROUP BY stop_id) shared USING(stop_id)
           LEFT JOIN vehicle_latest v ON v.route_id=r.route_id
                AND v.updated_at>now()-(%s * interval '1 second')
           WHERE rr.region_id=%s AND r.usable=true AND r.simulated=true
           GROUP BY r.route_id ORDER BY r.short_name,r.route_id""",
        (BUS_LIVE_TTL_SECONDS, region_id),
    )
    routes = cur.fetchall()
    max_buses = int(os.getenv("MAX_SIM_BUSES", "4000"))
    planned = allocate_fleet(
        [{"route_id": route["route_id"], "length_m": float(route["length_m"] or 0),
          "isolated": int(route["connection_stops"] or 0) <= 1} for route in routes], max_buses)
    for route in routes:
        route["minimum_buses"] = 2 if int(route["connection_stops"] or 0) <= 1 else 1
        route["recommended_buses"] = planned.get(route["route_id"], route["minimum_buses"])
    return routes, max_buses


def _control_command(action: str, *, region_id: str, route_bus_counts: dict[str, int] | None = None,
                     parameters: dict | None = None, requested_by: str = "administrator") -> ControlCommand:
    return ControlCommand(command_id=uuid.uuid4().hex, action=action,
                          issued_at=datetime.now(timezone.utc), requested_by=requested_by,
                          region_id=region_id, route_bus_counts=route_bus_counts or {},
                          parameters=parameters or {})


def _publish_control(command: ControlCommand):
    app.state.kafka.produce("control", key="simulator", value=command.model_dump_json().encode())
    if app.state.kafka.flush(5):
        raise HTTPException(status_code=503, detail="Simulator control queue did not drain; try again")


def _clear_live_vehicle_cache():
    """Drop obsolete latest-position rows before replacing the shared synthetic fleet."""
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM vehicle_latest")


@app.get("/api/admin/fleet")
def admin_fleet(request: Request, region_id: str = Query(default="whole_city", max_length=32)):
    require_admin(request)
    if region_id not in REGIONS:
        raise HTTPException(status_code=422, detail="Choose a supported Bengaluru simulation region")
    with app.state.db.connection() as conn, conn.cursor() as cur:
        routes, max_buses = _region_fleet_plan(cur, region_id)
    return {"routes": routes, "max_buses": max_buses,
            "minimum_buses_per_route": 1, "region_id": region_id,
            "region_name": REGIONS[region_id]["name"],
            "planned_total_buses": sum(route["recommended_buses"] for route in routes)}


@app.post("/api/commuter/simulation", status_code=202)
def commuter_simulation(body: dict, request: Request):
    user = require_session(request)
    route_id = str(body.get("route_id", "")).strip()
    demand_profile = str(body.get("demand_profile", "normal")).strip().lower()
    try:
        requested_buses = int(body.get("buses", 1))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Bus count must be a whole number") from exc
    if not route_id or not 1 <= requested_buses <= 25:
        raise HTTPException(status_code=422, detail="Choose a route and a bus count from 1 to 25")
    if demand_profile not in {"normal", "moderate", "high"}:
        raise HTTPException(status_code=422, detail="Crowding profile must be normal, moderate, or high")
    region_id = str(body.get("region_id", "")).strip()
    with app.state.db.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT region_id FROM simulation_status WHERE singleton_id=1")
        status = cur.fetchone() or {"region_id": "whole_city"}
        region_id = region_id or status["region_id"]
        if region_id not in REGIONS:
            raise HTTPException(status_code=422, detail="Choose a supported Bengaluru simulation region")
        routes, max_buses = _region_fleet_plan(cur, region_id)
    route_by_id = {str(route["route_id"]): route for route in routes}
    selected = route_by_id.get(route_id)
    if selected is None and region_id != "whole_city":
        # A transfer planner can return a valid simulated leg outside the
        # current regional view. Route demos switch the shared simulation, so
        # fall back to the city fleet when that route is not in the active area.
        with app.state.db.connection() as conn, conn.cursor() as cur:
            routes, max_buses = _region_fleet_plan(cur, "whole_city")
        route_by_id = {str(route["route_id"]): route for route in routes}
        selected = route_by_id.get(route_id)
        if selected is not None:
            region_id = "whole_city"
    if selected is None:
        raise HTTPException(status_code=409, detail="That route is not part of the simulated BMTC fleet. Choose a simulated route.")
    applied_count = max(int(selected["minimum_buses"]), requested_buses)
    if applied_count > max_buses:
        raise HTTPException(status_code=422, detail=f"This demo fleet would exceed the configured {max_buses}-bus limit")
    _publish_control(_control_command("route_demo", region_id=region_id,
                                      parameters={"route_id": route_id, "buses": applied_count,
                                                  "demand_profile": demand_profile},
                                      requested_by=str(user.get("sub", "commuter"))))
    return {"accepted": True, "region_id": region_id, "region_name": REGIONS[region_id]["name"],
            "route_id": route_id, "requested_buses": requested_buses,
            "applied_buses": applied_count, "total_buses": applied_count, "simulated": True,
            "demand_profile": demand_profile,
            "message": f"Started a {demand_profile} crowding route demo in {REGIONS[region_id]['name']} with {applied_count} buses. Other regional routes remain simulated."}


@app.post("/api/admin/simulation/command", status_code=202)
def simulation_command(body: dict, request: Request):
    require_admin(request)
    body = dict(body)
    body.setdefault("command_id", uuid.uuid4().hex)
    body.setdefault("issued_at", datetime.now(timezone.utc).isoformat())
    body.setdefault("requested_by", "administrator")
    try:
        command = ControlCommand.model_validate(body)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if command.action == "set_fleet":
        region_id = command.region_id or "whole_city"
        if region_id not in REGIONS:
            raise HTTPException(status_code=422, detail="Choose a supported Bengaluru simulation region")
        max_buses = int(os.getenv("MAX_SIM_BUSES", "4000"))
        if sum(command.route_bus_counts.values()) > max_buses:
            raise HTTPException(status_code=422, detail=f"Fleet exceeds configured limit of {max_buses}")
        with app.state.db.connection() as conn, conn.cursor() as cur:
            route_ids = list(command.route_bus_counts)
            cur.execute("""SELECT rr.route_id FROM route_regions rr JOIN routes r USING(route_id)
                         WHERE rr.region_id=%s AND rr.route_id=ANY(%s) AND r.usable=true AND r.simulated=true""",
                        (region_id, route_ids))
            valid = {row["route_id"] for row in cur.fetchall()}
        if valid != set(route_ids):
            raise HTTPException(status_code=422, detail="The fleet must contain only routes in the selected region")
        with app.state.db.connection() as conn, conn.cursor() as cur:
            cur.execute("""SELECT rr.route_id,max(d.length_m) AS length_m,
                           count(DISTINCT rs.stop_id) FILTER (WHERE COALESCE(shared.route_count,1)>1) AS connection_stops
                           FROM route_regions rr JOIN routes r USING(route_id)
                           JOIN route_directions d ON d.route_id=r.route_id AND d.usable=true
                           LEFT JOIN route_stops rs ON rs.route_id=d.route_id AND rs.direction_id=d.direction_id
                           LEFT JOIN (SELECT stop_id,count(DISTINCT route_id) AS route_count FROM route_stops
                                      JOIN routes USING(route_id) WHERE usable=true GROUP BY stop_id) shared USING(stop_id)
                           WHERE rr.region_id=%s AND r.usable=true AND r.simulated=true
                           GROUP BY rr.route_id""", (region_id,))
            route_metrics = cur.fetchall()
            cur.execute("""SELECT rr.route_id FROM route_regions rr JOIN routes r USING(route_id)
                         WHERE rr.region_id=%s AND r.usable=true AND r.simulated=true""", (region_id,))
            all_routes = {row["route_id"] for row in cur.fetchall()}
        if all_routes - set(route_ids):
            raise HTTPException(status_code=422, detail="The fleet command must include every route in the selected region")
        minimums = {row["route_id"]: (2 if int(row["connection_stops"] or 0) <= 1 else 1)
                    for row in route_metrics}
        if any(command.route_bus_counts.get(route_id, 0) < minimum for route_id, minimum in minimums.items()):
            raise HTTPException(status_code=422, detail="Each route needs one bus; isolated routes need at least two")
    elif command.action in {"start", "set_region", "rush_hour", "normal_hours", "moderate_hours", "reset"}:
        region_id = command.region_id or str(command.parameters.get("region_id", "whole_city"))
        if region_id not in REGIONS:
            raise HTTPException(status_code=422, detail="Choose a supported Bengaluru simulation region")
        command.region_id = region_id
    replace_fleet = command.action in {"set_fleet", "set_region", "reset"}
    if command.action == "start":
        with app.state.db.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT region_id FROM simulation_status WHERE singleton_id=1")
            current = cur.fetchone()
            replace_fleet = bool(current and current["region_id"] != command.region_id)
    if replace_fleet:
        _clear_live_vehicle_cache()
    _publish_control(command)
    return {"accepted": True, "command_id": command.command_id,
            "message": "Simulator will validate and acknowledge this command", "simulated": True}


@app.websocket("/ws")
async def websocket_updates(websocket: WebSocket, route_id: str | None = None,
                            route_ids: str | None = None):
    user = session_user(websocket)
    if not user:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    region_id = websocket.query_params.get("region_id")
    route_id_list = [item.strip() for item in (route_ids or "").split(",") if item.strip()] or None
    last_update = None
    first_snapshot = True
    try:
        while True:
            with app.state.db.connection() as conn, conn.cursor() as cur:
                query = """SELECT vehicle_id,route_id,region_id,direction_id,event_ts,updated_at,lat,lon,progress_m,
                               speed_mps,occ_est_pct,advisory_extra
                       FROM vehicle_latest WHERE updated_at > now() - (%s * interval '1 second')
                         AND (%s::text IS NULL OR route_id=%s)
                         AND (%s::text[] IS NULL OR route_id=ANY(%s))
                         AND (%s::text IS NULL OR region_id=%s OR region_id='whole_city'
                              OR EXISTS (SELECT 1 FROM route_regions live_rr
                                         WHERE live_rr.route_id=vehicle_latest.route_id AND live_rr.region_id=%s))
                         AND (%s::timestamptz IS NULL OR updated_at>%s::timestamptz)
                       ORDER BY updated_at DESC LIMIT 4000"""
                cur.execute(query, (BUS_LIVE_TTL_SECONDS, route_id, route_id, route_id_list, route_id_list,
                                    region_id, region_id, region_id, last_update, last_update))
                buses_now = cur.fetchall()
                if first_snapshot or buses_now:
                    if buses_now:
                        last_update = max(bus["updated_at"] for bus in buses_now)
                    chunk_size = 200
                    chunks = [buses_now[offset:offset + chunk_size]
                              for offset in range(0, len(buses_now), chunk_size)] or [[]]
                    for chunk_index, chunk in enumerate(chunks):
                        payload = {"type": "vehicles", "snapshot": first_snapshot,
                                   "snapshot_chunk_index": chunk_index,
                                   "snapshot_chunk_count": len(chunks),
                                   "buses": chunk, "simulated": True}
                        await websocket.send_text(json.dumps(payload, default=str))
                    first_snapshot = False
            import asyncio
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return


if WEB.exists():
    app.mount("/assets", StaticFiles(directory=WEB), name="assets")


@app.get("/")
def index(request: Request):
    cache_headers = {"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"}
    user = session_user(request)
    if not user:
        return FileResponse(WEB / "login.html", headers=cache_headers)
    if not WEB.joinpath("index.html").exists():
        return {"name": "Bus Pulse", "status": "web client is being assembled"}
    return FileResponse(WEB / "index.html", headers=cache_headers)
