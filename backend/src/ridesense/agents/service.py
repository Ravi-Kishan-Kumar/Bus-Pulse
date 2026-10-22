"""Tool-driven transit assistant with read-only tools and auditable evidence."""

from __future__ import annotations

import re
import json
import os
import urllib.request
import urllib.error
import uuid
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from ridesense.common.journey import find_journey_options, search_stops
from ridesense.common.regions import REGIONS


def _live_ttl() -> int:
    return int(os.getenv("BUS_LIVE_TTL_SECONDS", "75"))


def _journey_query(question: str) -> tuple[str, list[str]] | None:
    match = re.search(r"\b(?:from|for)\s+(.+?)\s+to\s+(.+?)(?:\s+direction)?[?.!]*$", question, re.I)
    if not match:
        return None
    start = re.sub(r"\([^)]*\)", "", match.group(1)).strip(" ,.;")
    destinations = [part.strip(" ,.;") for part in re.split(r"\s+or\s+", match.group(2), flags=re.I)]
    destinations = [re.sub(r"\s+(?:bus|buses|route|direction)$", "", part, flags=re.I).strip() for part in destinations]
    start = re.sub(r"^(?:bus|buses)\s+", "", start, flags=re.I)
    if len(start) < 2 or not destinations:
        return None
    return start, [value for value in destinations if len(value) >= 2]


def _trace(cur, session_id: str, tool: str, args: dict, result: dict):
    now = datetime.now(timezone.utc)
    agent = "insight_agent" if tool == "get_forecast_insight" else "commuter_assistant"
    cur.execute(
        """INSERT INTO agent_traces(call_id,trace_id,session_id,agent,tool,args,result,as_of_ts,created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (uuid.uuid4().hex, session_id, session_id, agent, tool, Jsonb(args),
         Jsonb(result, dumps=lambda value: json.dumps(value, default=str)), now, now),
    )


def answer_query(conn, question: str, session_id: str | None = None, *, _allow_groq: bool = True,
                 context: dict | None = None) -> dict:
    """Choose bounded database tools, gather evidence, then return a transparent reply."""
    session_id = session_id or uuid.uuid4().hex
    context = context or {}
    region_id = context.get("region_id")
    selected_route_id = context.get("selected_route_id")
    lowered = question.lower()
    route_match = re.search(r"(?:route\s*)?([a-z]{0,3}\d{2,4}(?:[-/][a-z0-9]+)?)", question, re.I)
    route_token = route_match.group(1).upper() if route_match else None
    with conn.cursor() as cur:
        vehicle_match = re.search(r"\b(V\d{3,})\b", question, re.I)
        if vehicle_match and any(word in lowered for word in ("forecast", "predict", "why", "explain")):
            vehicle_id = vehicle_match.group(1).upper()
            cur.execute("SELECT * FROM vehicle_latest WHERE vehicle_id=%s AND updated_at>now()-(%s * interval '1 second')", (vehicle_id, _live_ttl()))
            vehicle = cur.fetchone()
            cur.execute("SELECT made_at,pred_pct,model_version,contributions FROM predictions WHERE vehicle_id=%s ORDER BY made_at DESC LIMIT 1", (vehicle_id,))
            prediction = cur.fetchone()
            result = {"vehicle": vehicle, "prediction": prediction}
            cur.execute(
                """INSERT INTO agent_traces(call_id,trace_id,session_id,agent,tool,args,result,as_of_ts)
                   VALUES (%s,%s,%s,'insight_agent','get_forecast_insight',%s,%s,%s)""",
                (uuid.uuid4().hex, session_id, session_id, Jsonb({"vehicle_id": vehicle_id}),
                 Jsonb(result, dumps=lambda value: json.dumps(value, default=str)), datetime.now(timezone.utc)),
            )
            if vehicle and prediction:
                answer = (f"{vehicle_id} on route {vehicle['route_id']} is at {vehicle['occ_est_pct']:.0f}% current estimated occupancy. "
                          f"Its +5 minute forecast is {prediction['pred_pct']:.0f}% from {prediction['model_version']}. "
                          "Feature contributions are model attributions, not proof that a factor caused the change.")
            else:
                answer = f"I don't have a recent live snapshot and forecast for {vehicle_id}."
            return {"answer": answer, "evidence": result, "tools": ["get_forecast_insight"], "session_id": session_id}
        if "status" in lowered and any(word in lowered for word in ("bus", "buses", "route")):
            vehicle_match = re.search(r"\b(V\d{3,})\b", question, re.I)
            if vehicle_match:
                bus_id = vehicle_match.group(1).upper()
                cur.execute("""SELECT vehicle_id,route_id,region_id,lat,lon,speed_mps,occ_est_pct,
                              seated_count,standing_count,supported_standing_count,onboard_count,capacity,event_ts
                              FROM vehicle_latest WHERE vehicle_id=%s AND updated_at>now()-(%s * interval '1 second')""",
                            (bus_id, _live_ttl()))
                vehicle = cur.fetchone()
                if vehicle:
                    moving = float(vehicle["speed_mps"] or 0) > 0.5
                    answer = (f"{bus_id} on route {vehicle['route_id']} is {'moving at %.0f km/h' % (float(vehicle['speed_mps'])*3.6) if moving else 'stopped'} "
                              f"with about {vehicle['occ_est_pct']:.0f}% estimated capacity and {vehicle['onboard_count']} of {vehicle['capacity']} riders. "
                              "This is simulated status, not a live BMTC vehicle feed.")
                    result = {"vehicle": vehicle, "simulation_only": True}
                    _trace(cur, session_id, "get_bus_live_status", {"vehicle_id": bus_id}, result)
                    return {"answer": answer, "evidence": result, "tools": ["get_bus_live_status"], "session_id": session_id}
            if not route_token:
                cur.execute("SELECT region_id,running,paused,rush_hour FROM simulation_status WHERE singleton_id=1")
                sim = cur.fetchone() or {"region_id": region_id or "whole_city", "running": False, "paused": True}
                active_region = region_id or sim["region_id"]
                active_route = selected_route_id
                cur.execute(
                    """SELECT count(*) AS buses,count(*) FILTER (WHERE speed_mps>0.5) AS moving,
                              count(*) FILTER (WHERE speed_mps<=0.5) AS stopped,avg(occ_est_pct) AS mean_pct
                       FROM vehicle_latest WHERE updated_at>now()-(%s * interval '1 second')
                         AND region_id=%s AND (%s::text IS NULL OR route_id=%s)""",
                    (_live_ttl(), active_region, active_route, active_route),
                )
                summary = cur.fetchone()
                result = {"simulation": sim, "summary": summary, "region_id": active_region}
                _trace(cur, session_id, "get_region_live_status", {"region_id": active_region,
                       "route_id": active_route}, result)
                if summary["buses"]:
                    region_name = REGIONS.get(active_region, {}).get("name", active_region)
                    run_note = "Simulation is paused. " if not sim.get("running") else ""
                    answer = (f"{run_note}{region_name} simulation status: {summary['buses']} buses have recent snapshots; "
                              f"{summary['moving']} moving and {summary['stopped']} stopped. Average estimated occupancy is "
                              f"{float(summary['mean_pct'] or 0):.0f}%. This is simulated data. For one bus, ask for its ID or route number.")
                else:
                    region_name = REGIONS.get(active_region, {}).get("name", active_region)
                    answer = (f"No buses are reporting in {region_name}. "
                              "Ask the administrator to start a simulation region, or choose a route number.")
                return {"answer": answer, "evidence": result, "tools": ["get_region_live_status"], "session_id": session_id}

        if any(word in lowered for word in ("least crowded", "less crowded", "least busy", "less busy", "emptiest", "fewest people", "lower crowd")):
            cur.execute(
                """SELECT v.vehicle_id,v.route_id,r.short_name,v.occ_est_pct,v.seated_count,v.standing_count,
                          v.supported_standing_count,v.event_ts,v.capacity,v.vehicle_type FROM vehicle_latest v
                   JOIN routes r USING(route_id) WHERE v.updated_at > now() - (%s * interval '1 second')
                     AND (%s::text IS NULL OR v.region_id=%s)
                     AND (%s::text IS NULL OR v.route_id=%s)
                   ORDER BY v.occ_est_pct ASC,v.event_ts DESC LIMIT 5""",
                (_live_ttl(), region_id, region_id, selected_route_id, selected_route_id),
            )
            rows = cur.fetchall()
            result = {"vehicles": rows}
            _trace(cur, session_id, "find_least_crowded_buses", {"limit": 5}, result)
            if rows:
                lead = rows[0]
                answer = (f"The least crowded live simulated bus right now is {lead['vehicle_id']} on BMTC route "
                          f"{lead['short_name'] or lead['route_id']} at {lead['occ_est_pct']:.0f}% estimated capacity. "
                          f"It has {lead['seated_count']} seated and {lead['standing_count']} standing riders, "
                          f"including {lead['supported_standing_count']} using handholds.")
            else:
                answer = "No live simulated buses are reporting yet."
            return {"answer": answer, "evidence": result, "tools": ["find_least_crowded_buses"], "session_id": session_id}

        if any(term in lowered for term in ("should i wait", "wait for the next", "next bus", "worth waiting")):
            from_stop, to_stop = context.get("from_stop_id"), context.get("to_stop_id")
            if from_stop and to_stop:
                if not context.get("running"):
                    return {"answer": "The active simulation is paused. Ask the administrator to start this region, then I can compare arrival and crowding estimates.",
                            "evidence": {"region_id": region_id, "simulation_running": False},
                            "tools": ["get_simulation_status"], "session_id": session_id}
                options = find_journey_options(cur, from_stop, to_stop, 20, region_id=region_id)
                candidates = []
                for option in options:
                    cur.execute(
                        """SELECT v.vehicle_id,v.route_id,v.direction_id,v.progress_m,v.speed_mps,v.occ_est_pct,
                                  s.progress_m AS start_progress_m,d.length_m,v.event_ts
                           FROM vehicle_latest v JOIN route_stops s ON s.route_id=v.route_id
                                AND s.direction_id=%s AND s.seq=%s
                           JOIN route_directions d ON d.route_id=v.route_id AND d.direction_id=%s
                           WHERE v.route_id=%s AND v.updated_at>now()-(%s * interval '1 second')
                             AND (%s::text IS NULL OR v.region_id=%s)""",
                        (option["direction_id"], option["start_seq"], option["direction_id"], option["route_id"],
                         _live_ttl(), region_id, region_id),
                    )
                    for bus in cur.fetchall():
                        length, start, progress = float(bus["length_m"]), float(bus["start_progress_m"]), float(bus["progress_m"])
                        if bus["direction_id"] == option["direction_id"]:
                            distance = start-progress if progress <= start else max(0, 2*length-progress+start)
                        else:
                            distance = max(0, length-progress)+start
                        speed = float(bus["speed_mps"] or 0)
                        candidates.append({**bus, "wait_min": distance/max(speed, 3.5)/60+(0 if speed>0.5 else 0.25),
                                           "crowd_pct": float(bus["occ_est_pct"] or 0), "route_name": option["short_name"]})
                candidates.sort(key=lambda bus: (bus["wait_min"]+bus["crowd_pct"]/100*3, bus["crowd_pct"]))
                if candidates:
                    best = candidates[0]
                    suggestion = "Waiting looks reasonable" if best["wait_min"] <= 8 and best["crowd_pct"] < 85 else "Consider another route"
                    answer = (f"{suggestion}: bus {best['vehicle_id']} on {best['route_name'] or best['route_id']} is about "
                              f"{best['wait_min']:.0f} min from your start and at {best['crowd_pct']:.0f}% estimated capacity. "
                              "Arrival and crowd figures are simulated estimates, not BMTC live predictions.")
                    result = {"nearest_bus": best, "alternatives": candidates[1:4], "simulation_only": True}
                else:
                    answer = "I found the journey, but no bus has a fresh position in the active simulation. Ask the administrator to start this region."
                    result = {"routes": options, "simulation_only": True}
                _trace(cur, session_id, "compare_waiting_options", {"from_stop_id": from_stop, "to_stop_id": to_stop}, result)
                return {"answer": answer, "evidence": result, "tools": ["compare_waiting_options"], "session_id": session_id}
            if selected_route_id:
                cur.execute("""SELECT count(*) AS buses,avg(occ_est_pct) AS mean_pct,min(occ_est_pct) AS low_pct
                               FROM vehicle_latest WHERE route_id=%s AND updated_at>now()-(%s * interval '1 second')
                               AND (%s::text IS NULL OR region_id=%s)""",
                            (selected_route_id, _live_ttl(), region_id, region_id))
                summary = cur.fetchone()
                if summary["buses"]:
                    answer = (f"Route {selected_route_id} has {summary['buses']} current simulated buses and "
                              f"{float(summary['mean_pct'] or 0):.0f}% average estimated crowding. Select a start and destination "
                              "to compare each bus's approach time before deciding to wait.")
                    result = {"route_id": selected_route_id, "summary": summary, "simulation_only": True}
                    _trace(cur, session_id, "get_route_wait_context", {"route_id": selected_route_id}, result)
                    return {"answer": answer, "evidence": result, "tools": ["get_route_wait_context"], "session_id": session_id}
            return {"answer": "Choose a start and destination, or view a route first. I can then compare the nearest simulated bus, estimated wait, and crowding.",
                    "evidence": {"needs_journey_context": True}, "tools": ["request_journey_context"], "session_id": session_id}

        journey = _journey_query(question)
        if journey:
            start_query, destination_queries = journey
            starts = search_stops(cur, start_query, 8)
            branches = []
            for destination_query in destination_queries:
                destinations = search_stops(cur, destination_query, 8)
                branch_options = []
                for start in starts[:5]:
                    for destination in destinations[:5]:
                        if start["stop_id"] == destination["stop_id"]:
                            continue
                        branch_options.extend(find_journey_options(cur, start["stop_id"], destination["stop_id"], 20,
                                                                   region_id=region_id))
                unique = {}
                for option in branch_options:
                    unique.setdefault((option["route_id"], option["direction_id"]), option)
                routes = sorted(unique.values(), key=lambda item: (not item["simulated"], -int(item["live_buses"]),
                                                                    float(item["least_crowded_pct"] or 0), item["short_name"] or ""))[:4]
                branches.append({"destination_query": destination_query, "routes": routes})
            options = [route for branch in branches for route in branch["routes"]]
            result = {"origin_query": start_query, "destination_queries": destination_queries,
                      "destination_options": branches, "routes": options, "simulation_only": True}
            _trace(cur, session_id, "find_journey_routes", {"from": start_query, "to": destination_queries}, result)
            if options:
                descriptions = []
                for branch in branches:
                    if not branch["routes"]:
                        descriptions.append(f"{branch['destination_query']}: no single-route match")
                        continue
                    choices = []
                    for item in branch["routes"]:
                        bus = item.get("least_crowded_bus")
                        pct = item.get("least_crowded_pct")
                        live = f", bus {bus} at {float(pct):.0f}% simulated occupancy" if bus and pct is not None else ", no bus snapshot yet"
                        choices.append(f"{item['short_name'] or item['route_id']}{live}, alight at {item['matched_destination_stop']}")
                    descriptions.append(f"{branch['destination_query']}: " + "; ".join(choices))
                first = next(route for branch in branches for route in branch["routes"])
                stop_names = " → ".join(item["name"] for item in first["journey_stops"])
                answer = (f"From {start_query}, route options are " + " | ".join(descriptions) +
                          f". Stops on the first option: {stop_names}. Occupancy and bus locations are simulated, not live BMTC data.")
            else:
                answer = f"I couldn't match a single route from {start_query} to {' / '.join(destination_queries)} in the GTFS stop sequence. Try the stop picker or search a route number."
            return {"answer": answer, "evidence": result, "tools": ["find_journey_routes"], "session_id": session_id}

        if route_token:
            cur.execute(
                """SELECT route_id,short_name,long_name FROM routes
                   WHERE (upper(route_id)=upper(%s) OR upper(short_name)=upper(%s)
                      OR regexp_replace(upper(short_name),'[^A-Z0-9]','','g')=%s)
                     AND (%s::text IS NULL OR EXISTS (SELECT 1 FROM route_regions rr
                          WHERE rr.route_id=routes.route_id AND rr.region_id=%s))
                   ORDER BY simulated DESC LIMIT 1""", (route_token, route_token,
                    re.sub(r"[^A-Z0-9]", "", route_token), region_id, region_id),
            )
            route = cur.fetchone()
            if route:
                cur.execute(
                    """SELECT vehicle_id,occ_est_pct,seated_count,standing_count,supported_standing_count,
                              speed_mps,event_ts FROM vehicle_latest WHERE route_id=%s
                       AND updated_at > now() - (%s * interval '1 second')
                       AND (%s::text IS NULL OR region_id=%s) ORDER BY occ_est_pct ASC LIMIT 10""",
                    (route["route_id"], _live_ttl(), region_id, region_id),
                )
                buses = cur.fetchall()
                result = {"route": route, "buses": buses}
                _trace(cur, session_id, "get_route_live_status", {"route_id": route["route_id"]}, result)
                if buses:
                    mean = sum(float(b["occ_est_pct"]) for b in buses) / len(buses)
                    answer = (f"Route {route['short_name'] or route['route_id']} has {len(buses)} live simulated buses "
                              f"in the latest sample. Average estimated occupancy is {mean:.0f}%. The least crowded is "
                              f"{buses[0]['vehicle_id']} at {buses[0]['occ_est_pct']:.0f}%. This is simulation data, "
                              "not a BMTC live feed or timetable-based arrival estimate.")
                else:
                    answer = f"Route {route['short_name'] or route['route_id']} is in the BMTC route dataset, but no buses are currently assigned to its simulation."
                return {"answer": answer, "evidence": result, "tools": ["get_route_live_status"], "session_id": session_id}

        cur.execute(
            """SELECT r.route_id,r.short_name,r.long_name,r.usable,r.simulated,count(v.vehicle_id) AS live_buses
               FROM routes r LEFT JOIN vehicle_latest v ON v.route_id=r.route_id AND v.updated_at > now() - (%s * interval '1 second')
               WHERE r.route_id ILIKE %s OR r.short_name ILIKE %s OR r.long_name ILIKE %s
               GROUP BY r.route_id ORDER BY r.simulated DESC,r.short_name LIMIT 5""",
            (_live_ttl(), f"%{question.strip()}%", f"%{question.strip()}%", f"%{question.strip()}%"),
        )
        routes = cur.fetchall()
        result = {"routes": routes}
        _trace(cur, session_id, "search_bmtc_routes", {"query": question[:100]}, result)
        if routes:
            names = ", ".join(f"{r['short_name'] or r['route_id']} ({r['live_buses']} simulated buses)" for r in routes)
            answer = f"I found these matching routes: {names}. Crowd and bus counts shown here are synthetic simulation values."
        else:
            answer = "I can look up route crowding, find the least crowded simulated bus, or search BMTC route names. Try asking “Which bus is least crowded?” or include a route number."
        local = {"answer": answer, "evidence": result, "tools": ["search_bmtc_routes"], "session_id": session_id}
    if _allow_groq and os.getenv("LLM_PROVIDER", "mock").lower() == "groq" and os.getenv("GROQ_API_KEY"):
        return _groq_agent(conn, question, session_id)
    return local


GROQ_TOOLS = [
    {"type": "function", "function": {"name": "find_least_crowded_buses", "description": "Find the five least-crowded live simulated buses and their seat, standee, and handhold counts.", "parameters": {"type": "object", "properties": {}, "additionalProperties": False}}},
    {"type": "function", "function": {"name": "get_route_live_status", "description": "Get current simulated buses and occupancy for a BMTC route by route ID or public route name.", "parameters": {"type": "object", "properties": {"route": {"type": "string"}}, "required": ["route"], "additionalProperties": False}}},
    {"type": "function", "function": {"name": "search_bmtc_routes", "description": "Search static BMTC route identifiers and route names.", "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"], "additionalProperties": False}}},
    {"type": "function", "function": {"name": "find_journey_routes", "description": "Find GTFS routes and ordered stops between Bengaluru stops or hubs such as Majestic and Whitefield. Bus positions are simulated.", "parameters": {"type": "object", "properties": {"from_stop": {"type": "string"}, "to_stop": {"type": "string"}}, "required": ["from_stop", "to_stop"], "additionalProperties": False}}},
    {"type": "function", "function": {"name": "get_forecast_insight", "description": "Read a live bus's current occupancy and latest five-minute prediction with feature attributions.", "parameters": {"type": "object", "properties": {"vehicle_id": {"type": "string"}}, "required": ["vehicle_id"], "additionalProperties": False}}},
]


def _execute_tool(conn, session_id: str, name: str, args: dict) -> dict:
    with conn.cursor() as cur:
        if name == "find_least_crowded_buses":
            cur.execute(
                """SELECT v.vehicle_id,v.route_id,r.short_name,v.occ_est_pct,v.seated_count,v.standing_count,
                          v.supported_standing_count,v.event_ts FROM vehicle_latest v JOIN routes r USING(route_id)
                   WHERE v.updated_at > now() - (%s * interval '1 second')
                   ORDER BY v.occ_est_pct ASC,v.event_ts DESC LIMIT 5""", (_live_ttl(),)
            )
            result = {"vehicles": cur.fetchall()}
        elif name == "get_route_live_status":
            route = str(args.get("route", "")).strip()[:80]
            cur.execute("""SELECT route_id,short_name,long_name FROM routes
                           WHERE upper(route_id)=upper(%s) OR upper(short_name)=upper(%s)
                              OR regexp_replace(upper(short_name),'[^A-Z0-9]','','g')=%s
                           ORDER BY simulated DESC LIMIT 1""", (route, route, re.sub(r"[^A-Z0-9]", "", route.upper())))
            row = cur.fetchone()
            if row:
                cur.execute("""SELECT vehicle_id,occ_est_pct,seated_count,standing_count,supported_standing_count,
                                      speed_mps,event_ts FROM vehicle_latest WHERE route_id=%s
                               AND updated_at > now() - (%s * interval '1 second') ORDER BY occ_est_pct LIMIT 10""",
                            (row["route_id"], _live_ttl()))
                result = {"route": row, "buses": cur.fetchall()}
            else:
                result = {"route": None, "buses": []}
        elif name == "search_bmtc_routes":
            query = str(args.get("query", "")).strip()[:80]
            cur.execute("""SELECT route_id,short_name,long_name,usable,simulated FROM routes
                           WHERE route_id ILIKE %s OR short_name ILIKE %s OR long_name ILIKE %s
                           ORDER BY simulated DESC,short_name LIMIT 5""",
                        (f"%{query}%", f"%{query}%", f"%{query}%"))
            result = {"routes": cur.fetchall()}
        elif name == "find_journey_routes":
            from_query = str(args.get("from_stop", "")).strip()[:100]
            to_query = str(args.get("to_stop", "")).strip()[:100]
            starts = search_stops(cur, from_query, 5)
            destinations = search_stops(cur, to_query, 5)
            options = []
            for start in starts:
                for destination in destinations:
                    if start["stop_id"] != destination["stop_id"]:
                        options.extend(find_journey_options(cur, start["stop_id"], destination["stop_id"], 10))
            unique = {}
            for option in options:
                unique.setdefault((option["route_id"], option["direction_id"]), option)
            routes = sorted(unique.values(), key=lambda item: (not item["simulated"], -int(item["live_buses"])))[:8]
            result = {"from": from_query, "to": to_query, "routes": routes, "simulation_only": True}
        elif name == "get_forecast_insight":
            vehicle_id = str(args.get("vehicle_id", "")).strip().upper()[:32]
            cur.execute("SELECT vehicle_id,route_id,occ_est_pct,seated_count,standing_count,supported_standing_count,event_ts FROM vehicle_latest WHERE vehicle_id=%s AND updated_at>now()-(%s * interval '1 second')", (vehicle_id, _live_ttl()))
            vehicle = cur.fetchone()
            cur.execute("SELECT made_at,pred_pct,model_version,contributions FROM predictions WHERE vehicle_id=%s ORDER BY made_at DESC LIMIT 1", (vehicle_id,))
            prediction = cur.fetchone()
            result = {"vehicle": vehicle, "prediction": prediction,
                      "interpretation_note": "Contributions are model attributions, not causal evidence."}
        else:
            return {"error": "Tool is not available."}
        _trace(cur, session_id, name, args, result)
        return result


def _groq_agent(conn, question: str, session_id: str) -> dict:
    """Run a small tool-calling loop; the model receives no write/control tools."""
    model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    messages = [
        {"role": "system", "content": "You are Bus Pulse's Bengaluru transit assistant. Use the available read-only tools for current facts. Never claim simulated positions, passenger counts, or forecasts are BMTC live data. Distinguish static route data from synthetic simulation. If tools provide no data, say so. Keep replies concise."},
        {"role": "user", "content": question},
    ]
    tools_used = []
    evidence = []
    for _ in range(4):
        body = json.dumps({"model": model, "messages": messages, "tools": GROQ_TOOLS,
                           "tool_choice": "auto", "temperature": 0.1}).encode()
        request = urllib.request.Request(
            "https://api.groq.com/openai/v1/chat/completions", data=body,
            headers={"Authorization": f"Bearer {os.environ['GROQ_API_KEY']}",
                     "Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=float(os.getenv("LLM_TIMEOUT_S", "15"))) as response:
                result = json.loads(response.read())
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            # Preserve service availability; deterministic tools still answer when the LLM provider fails.
            fallback = answer_query(conn, question, session_id, _allow_groq=False)
            fallback["provider"] = "mock-fallback"
            fallback["provider_note"] = f"Groq request unavailable ({type(exc).__name__}); answered with local tools."
            return fallback
        message = result["choices"][0]["message"]
        calls = message.get("tool_calls") or []
        if not calls:
            return {"answer": message.get("content") or "I couldn't form a grounded answer from the available route data.",
                    "evidence": evidence, "tools": tools_used, "session_id": session_id, "provider": "groq"}
        messages.append(message)
        for call in calls:
            name = call["function"]["name"]
            try:
                args = json.loads(call["function"].get("arguments") or "{}")
            except ValueError:
                args = {}
            tool_result = _execute_tool(conn, session_id, name, args)
            tools_used.append(name)
            evidence.append({"tool": name, "result": tool_result})
            messages.append({"role": "tool", "tool_call_id": call["id"],
                             "content": json.dumps(tool_result, default=str)[:12000]})
    return {"answer": "I reached the assistant's tool-call limit. Try asking about one route or the least crowded bus.",
            "evidence": evidence, "tools": tools_used, "session_id": session_id, "provider": "groq"}




def refresh_advisories(conn):
    """Create a human-review suggestion from live alert evidence; never dispatch control."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT a.event_id,a.alert_type,a.severity,a.route_id,a.vehicle_id,a.occ_pct,a.pred_pct,a.detected_at,
                      r.short_name,(SELECT count(*) FROM vehicle_latest v WHERE v.route_id=a.route_id
                                    AND v.updated_at > now() - (%s * interval '1 second')) AS buses
               FROM alerts a JOIN routes r USING(route_id)
               WHERE a.acknowledged_at IS NULL AND a.alert_type IN ('OVERCROWD','PREDICTED_OVERCROWD')
               ORDER BY a.detected_at DESC LIMIT 30""", (_live_ttl(),)
        )
        alerts = cur.fetchall()
        for alert in alerts:
            occupied = alert["pred_pct"] if alert["pred_pct"] is not None else alert["occ_pct"]
            action = "Review adding 1 simulated bus to route " + str(alert["short_name"] or alert["route_id"])
            rationale = (f"{alert['alert_type']} at {float(occupied or 0):.0f}% capacity; "
                         f"{alert['buses']} buses are currently assigned in the simulation.")
            cur.execute(
                """INSERT INTO advisories(advisory_id,event_id,route_id,action,rationale,evidence_ids,source)
                   VALUES (%s,%s,%s,%s,%s,%s,'capacity_advisor') ON CONFLICT (event_id) DO NOTHING""",
                (uuid.uuid5(uuid.NAMESPACE_URL, f"bus-pulse-advisory:{alert['event_id']}").hex,
                 alert["event_id"], alert["route_id"], action, rationale,
                 Jsonb([alert["event_id"]])),
            )
