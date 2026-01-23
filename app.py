import os
import math
import random
import logging
from functools import wraps
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from flask import Flask, request, redirect, url_for, session, render_template, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

AWS_REGION = "ap-south-2"
DYNAMODB_TABLE_NAME = "BusRoute"
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-2:251198450365:Route_42_alerts"
APP_SECRET = os.environ.get("APP_SECRET", "super_secret_key_change_this_in_production_2025")

app = Flask(__name__, template_folder="templates")
app.secret_key = APP_SECRET
app.permanent_session_lifetime = timedelta(days=7)

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
sns_client = boto3.client("sns", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

buses = {
    "10A": {"route": "Secunderabad → Mehdipatnam", "stops": [{"name": "Secunderabad", "lat": 17.4399, "lon": 78.4983},
                                                             {"name": "Begumpet", "lat": 17.4444, "lon": 78.4611},
                                                             {"name": "Ameerpet", "lat": 17.4375, "lon": 78.4483},
                                                             {"name": "Punjagutta", "lat": 17.4294, "lon": 78.4506},
                                                             {"name": "Mehdipatnam", "lat": 17.3956, "lon": 78.4403}],
            "frequency": "Every 10 mins", "type": "AC"},
    "100M": {"route": "JBS → MGBS", "stops": [{"name": "JBS", "lat": 17.4565, "lon": 78.4998},
                                              {"name": "Paradise", "lat": 17.4432, "lon": 78.4841},
                                              {"name": "Charminar", "lat": 17.3616, "lon": 78.4747},
                                              {"name": "Moazzam Jahi Market", "lat": 17.3832, "lon": 78.4735},
                                              {"name": "MGBS", "lat": 17.3840, "lon": 78.4804}],
             "frequency": "Every 8 mins", "type": "Metro Express"},
    "65G": {"route": "Miyapur → Shamshabad", "stops": [{"name": "Miyapur", "lat": 17.4947, "lon": 78.3569},
                                                       {"name": "Lingampally", "lat": 17.4948, "lon": 78.3246},
                                                       {"name": "Gachibowli", "lat": 17.4401, "lon": 78.3489},
                                                       {"name": "Rajiv Gandhi Airport", "lat": 17.2402, "lon": 78.4294},
                                                       {"name": "Shamshabad", "lat": 17.2289, "lon": 78.4297}],
            "frequency": "Every 30 mins", "type": "Airport Express"}
}

users = {}
user_travel_history = {}


def now_ts(): return int(datetime.now(timezone.utc).timestamp())


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please login first.", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapper


def decimalize(x):
    if x is None or isinstance(x, Decimal): return x
    if isinstance(x, int): return x
    if isinstance(x, float): return Decimal(str(x))
    return x


def safe_put_item(item: dict, op="put_item"):
    if "BusID" not in item or not str(item["BusID"]).strip(): raise ValueError("Missing BusID")
    if "TimeStamp" not in item or item["TimeStamp"] in (None, ""): item["TimeStamp"] = now_ts()
    if isinstance(item["TimeStamp"], datetime):
        item["TimeStamp"] = int(item["TimeStamp"].timestamp())
    elif isinstance(item["TimeStamp"], str):
        item["TimeStamp"] = int(datetime.fromisoformat(item["TimeStamp"].replace("Z", "+00:00")).timestamp())
    else:
        item["TimeStamp"] = int(item["TimeStamp"])
    item["BusID"] = str(item["BusID"]).upper()
    for k in ("Latitude", "Longitude", "SpeedKmph", "DistanceToStopKm"):
        if k in item and item[k] is not None: item[k] = decimalize(item[k])
    table.put_item(Item=item)
    logging.info(f"✅ DynamoDB {op}: BusID={item['BusID']} TimeStamp={item['TimeStamp']}")


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def sns_subscribe_email(email): return sns_client.subscribe(TopicArn=SNS_TOPIC_ARN, Protocol="email", Endpoint=email)


def sns_publish(subject, message): return sns_client.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)


def format_ts(ts): return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def add_to_travel_history(username, bus_id):
    if username not in user_travel_history: user_travel_history[username] = []
    if bus_id not in user_travel_history[username]:
        user_travel_history[username].append(bus_id)
        logging.info(f"Added {bus_id} to {username}'s travel history")


def get_travel_history(username):
    bus_ids = user_travel_history.get(username, [])
    history_data = []
    for bus_id in bus_ids:
        if bus_id in buses:
            try:
                resp = table.query(KeyConditionExpression=Key("BusID").eq(bus_id), ScanIndexForward=False, Limit=5)
                trips = resp.get("Items", [])
                history_data.append({"bus_id": bus_id, "bus_info": buses[bus_id], "trip_count": len(trips),
                                     "last_trip": trips[0] if trips else None})
            except:
                history_data.append({"bus_id": bus_id, "bus_info": buses[bus_id], "trip_count": 0, "last_trip": None})
    return history_data


@app.post("/api/v1/track")
def api_track():
    try:
        data = request.get_json(force=True)
        bus_id = str(data.get("bus_id", "")).upper().strip()
        lat, lon = float(data["lat"]), float(data["lon"])
        speed, status, ts = float(data.get("speed_kmph", random.uniform(10, 35))), str(
            data.get("status", "ACTIVE")).upper(), data.get("timestamp", now_ts())
        if bus_id not in buses: return jsonify({"ok": False, "error": f"Unknown bus_id {bus_id}"}), 400
        stops = buses[bus_id]["stops"]
        dists = [(s["name"], haversine_km(lat, lon, s["lat"], s["lon"])) for s in stops]
        next_stop, dist_km = min(dists, key=lambda x: x[1])
        item = {"BusID": bus_id, "TimeStamp": ts, "Latitude": lat, "Longitude": lon, "SpeedKmph": speed,
                "Status": status, "Route": buses[bus_id]["route"], "NextStop": next_stop, "DistanceToStopKm": dist_km}
        safe_put_item(item, op="api_track")
        alert_radius_km = float(data.get("alert_radius_km", 0.5))
        if dist_km <= alert_radius_km:
            sns_publish(subject=f"Bus {bus_id} nearing {next_stop}",
                        message=f"RouteWise Alert\nBus: {bus_id}\nNext Stop: {next_stop}\nDistance: {dist_km:.2f} km\nTime: {format_ts(item['TimeStamp'])}\n")
        return jsonify({"ok": True, "stored": item}), 200
    except Exception as e:
        logging.exception("api_track failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/")
def index():
    if "user_id" not in session: return redirect(url_for("login"))
    return redirect(url_for("home"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username, password, email = request.form.get("username", "").strip(), request.form.get("password",
                                                                                               ""), request.form.get(
            "email", "").strip()
        if not username or not password: flash("Username and password are required.", "error"); return render_template(
            "register.html", logged_in=False)
        if username in users: flash("Username already exists.", "error"); return render_template("register.html",
                                                                                                 logged_in=False)
        users[username] = {"pw": generate_password_hash(password), "email": email if email else ""}
        flash("Registration successful. Please login.", "success")
        return redirect(url_for("login"))
    return render_template("register.html", logged_in=False)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username, password = request.form.get("username", "").strip(), request.form.get("password", "")
        u = users.get(username)
        if not u or not check_password_hash(u["pw"], password): flash("Invalid username or password.",
                                                                      "error"); return render_template("login.html",
                                                                                                       logged_in=False)
        session["user_id"], session.permanent = username, True
        flash("Logged in successfully.", "success")
        return redirect(url_for("home"))
    return render_template("login.html", logged_in=False)


@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "info")
    return redirect(url_for("login"))


@app.route("/home")
@login_required
def home():
    return render_template("home.html", logged_in=True, username=session["user_id"], buses=buses,
                           travel_history=get_travel_history(session["user_id"]))


@app.route("/buses")
@login_required
def buses_page():
    return render_template("buses.html", logged_in=True, buses=buses)


@app.route("/bus/<bus_id>")
@login_required
def bus_details(bus_id):
    bus_id = bus_id.upper().strip()
    if bus_id not in buses: flash(f"Bus {bus_id} not found.", "error"); return redirect(url_for("home"))
    add_to_travel_history(session["user_id"], bus_id)
    latest = None
    try:
        resp = table.query(KeyConditionExpression=Key("BusID").eq(bus_id), ScanIndexForward=False, Limit=1)
        items = resp.get("Items", [])
        latest = items[0] if items else None
        if latest and "TimeStamp" in latest:
            latest["FormattedTime"] = format_ts(latest["TimeStamp"])
            for key in ['Latitude', 'Longitude', 'SpeedKmph', 'DistanceToStopKm']:
                if key in latest and isinstance(latest[key], Decimal): latest[key] = float(latest[key])
    except Exception as e:
        logging.error(f"Latest fetch failed: {e}")
    return render_template("bus_details.html", logged_in=True, bus_id=bus_id, bus=buses[bus_id], latest=latest)


@app.route("/bus/<bus_id>/history")
@login_required
def bus_history(bus_id):
    bus_id = bus_id.upper().strip()
    if bus_id not in buses: flash(f"Bus {bus_id} not found.", "error"); return redirect(url_for("home"))
    items = []
    try:
        resp = table.query(KeyConditionExpression=Key("BusID").eq(bus_id), ScanIndexForward=False, Limit=25)
        items = resp.get("Items", [])
        for r in items:
            if "TimeStamp" in r: r["FormattedTime"] = format_ts(r["TimeStamp"])
            for key in ['Latitude', 'Longitude', 'SpeedKmph', 'DistanceToStopKm']:
                if key in r and isinstance(r[key], Decimal): r[key] = float(r[key])
    except Exception as e:
        logging.error(f"History fetch failed: {e}")
    return render_template("bus_history.html", logged_in=True, bus_id=bus_id, items=items)


@app.route("/subscribe", methods=["GET", "POST"])
@login_required
def subscribe():
    user_email = users.get(session["user_id"], {}).get("email", "")
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        if not email: flash("Please provide an email address.", "error"); return render_template("subscribe.html",
                                                                                                 logged_in=True,
                                                                                                 user_email=user_email)
        try:
            sns_subscribe_email(email); flash("Subscription request sent!", "success")
        except ClientError as e:
            flash(f"SNS error: {e.response['Error']['Message']}", "error")
    return render_template("subscribe.html", logged_in=True, user_email=user_email)


@app.route("/admin/simulate/<bus_id>")
@login_required
def simulate(bus_id):
    bus_id = bus_id.upper().strip()
    if bus_id not in buses: flash(f"Unknown bus {bus_id}", "error"); return redirect(url_for("home"))
    stop = random.choice(buses[bus_id]["stops"])
    payload = {"bus_id": bus_id, "lat": stop["lat"] + random.uniform(-0.005, 0.005),
               "lon": stop["lon"] + random.uniform(-0.005, 0.005), "speed_kmph": random.uniform(10, 35),
               "status": "ACTIVE", "alert_radius_km": 0.5}
    with app.test_request_context(json=payload): api_track()
    flash("✅ GPS detected - Bus tracking updated successfully!", "success")
    return redirect(url_for("bus_details", bus_id=bus_id))


@app.route("/admin/generate-test-data")
@login_required
def generate_test_data():
    count = 0
    for bus_id in buses.keys():
        for i in range(5):
            stop = random.choice(buses[bus_id]["stops"])
            safe_put_item({"BusID": bus_id, "TimeStamp": now_ts() - (i * 300),
                           "Latitude": stop["lat"] + random.uniform(-0.01, 0.01),
                           "Longitude": stop["lon"] + random.uniform(-0.01, 0.01), "SpeedKmph": random.uniform(15, 45),
                           "Status": "ACTIVE", "Route": buses[bus_id]["route"], "NextStop": stop["name"],
                           "DistanceToStopKm": random.uniform(0.1, 2.0)})
            count += 1
    flash(f"✅ GPS detected - Generated {count} test records successfully!", "success")
    return redirect(url_for("home"))


def create_templates():
    os.makedirs("templates", exist_ok=True)
    templates = {
        "base.html": '''<!doctype html><html><head><meta charset="utf-8"><title>{% block title %}RouteWise{% endblock %}</title><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:system-ui;background:linear-gradient(135deg,#667eea,#764ba2);min-height:100vh;color:#1e293b}nav{background:rgba(255,255,255,.95);padding:16px 0;box-shadow:0 2px 20px rgba(0,0,0,.1)}.nav-content{max-width:1200px;margin:0 auto;padding:0 20px;display:flex;justify-content:space-between;align-items:center}.nav-brand{font-size:24px;font-weight:700;color:#667eea}.nav-links a{color:#475569;text-decoration:none;margin-left:24px;font-weight:500}.wrap{max-width:1200px;margin:0 auto;padding:40px 20px}.card{background:rgba(255,255,255,.95);border-radius:16px;padding:28px;margin:20px 0;box-shadow:0 8px 32px rgba(0,0,0,.1)}.btn{display:inline-block;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;padding:12px 24px;border-radius:10px;text-decoration:none;border:none;cursor:pointer;font-weight:600;font-size:14px}.btn:hover{transform:translateY(-2px)}.btn.secondary{background:linear-gradient(135deg,#10b981,#059669)}input{width:100%;padding:12px 16px;border:2px solid #e2e8f0;border-radius:10px;margin:8px 0 16px;font-size:15px}label{display:block;font-weight:600;color:#334155;margin-bottom:4px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:24px;margin-top:24px}.flash{padding:16px 20px;border-radius:12px;margin:16px 0;font-weight:500}.flash.error{background:#fee2e2;color:#991b1b}.flash.success{background:#dcfce7;color:#166534}.flash.info{background:#dbeafe;color:#1e40af}.hero{text-align:center;padding:40px 0;color:#fff}.hero h1{font-size:48px;font-weight:700;margin-bottom:16px}.section-title{font-size:28px;font-weight:700;color:#fff;margin:40px 0 20px}.travel-history{background:rgba(255,255,255,.1);border-radius:16px;padding:24px;margin-top:30px}.history-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;margin-top:16px}.history-item{background:rgba(255,255,255,.95);padding:16px;border-radius:12px;text-align:center;transition:transform .3s}.history-item:hover{transform:scale(1.05)}.history-bus{font-size:20px;font-weight:700;color:#667eea}</style></head><body><nav><div class="nav-content"><div class="nav-brand">🚌 RouteWise</div><div class="nav-links">{% if logged_in %}<a href="{{ url_for('home') }}">Home</a><a href="{{ url_for('buses_page') }}">Buses</a><a href="{{ url_for('subscribe') }}">Subscribe</a><a href="{{ url_for('logout') }}">Logout</a>{% else %}<a href="{{ url_for('register') }}">Register</a><a href="{{ url_for('login') }}">Login</a>{% endif %}</div></div></nav><div class="wrap">{% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for cat,msg in messages %}<div class="flash {{cat}}">{{ msg|safe }}</div>{% endfor %}{% endif %}{% endwith %}{% block content %}{% endblock %}</div></body></html>''',

        "register.html": '''{% extends "base.html" %}{% block content %}<div style="max-width:450px;margin:60px auto"><div class="card"><h2 style="text-align:center">Create Account</h2><form method="POST"><label>Username</label><input type="text" name="username" required><label>Password</label><input type="password" name="password" required><label>Email (Optional)</label><input type="email" name="email" placeholder="Optional - for bus alerts"><button class="btn" type="submit" style="width:100%">Register</button></form><p style="margin-top:20px;text-align:center">Already have an account? <a href="{{ url_for('login') }}">Login</a></p></div></div>{% endblock %}''',

        "login.html": '''{% extends "base.html" %}{% block content %}<div style="max-width:450px;margin:60px auto"><div class="card"><h2 style="text-align:center">Welcome Back</h2><form method="POST"><label>Username</label><input type="text" name="username" required><label>Password</label><input type="password" name="password" required><button class="btn" type="submit" style="width:100%">Login</button></form><p style="margin-top:20px;text-align:center">Don't have an account? <a href="{{ url_for('register') }}">Register</a></p></div></div>{% endblock %}''',

        "home.html": '''{% extends "base.html" %}{% block content %}<div class="hero"><h1>Welcome, {{ username }}! 👋</h1><p>Track city buses in real-time</p></div><h2 class="section-title">Available Routes</h2><div class="grid">{% for bus_id, bus in buses.items() %}<div class="card"><h3>🚌 {{ bus_id }}</h3><p>{{ bus.route }}</p><p style="color:#64748b">⏱️ {{ bus.frequency }}</p><div style="display:flex;gap:8px;margin-top:16px"><a class="btn" href="{{ url_for('bus_details', bus_id=bus_id) }}" style="flex:1;text-align:center">View</a><a class="btn secondary" href="{{ url_for('simulate', bus_id=bus_id) }}" style="flex:1;text-align:center">🛰️ GPS</a></div></div>{% endfor %}</div>{% if travel_history %}<div class="travel-history"><h2 style="color:#fff;margin-bottom:16px">🕒 Your Travel History</h2><div class="history-grid">{% for item in travel_history %}<a href="{{ url_for('bus_history', bus_id=item.bus_id) }}" style="text-decoration:none"><div class="history-item"><div class="history-bus">🚌 {{ item.bus_id }}</div><div style="font-size:13px;color:#64748b;margin-top:4px">{{ item.bus_info.route }}</div><div style="margin-top:8px;font-size:12px;color:#667eea">{{ item.trip_count }} trips</div></div></a>{% endfor %}</div></div>{% endif %}{% endblock %}''',

        "buses.html": '''{% extends "base.html" %}{% block content %}<div class="hero"><h1>All Bus Routes</h1></div><div class="grid">{% for bus_id, bus in buses.items() %}<div class="card"><h3>🚌 {{ bus_id }}</h3><p>{{ bus.route }}</p><p style="color:#64748b">⏱️ {{ bus.frequency }}</p><a class="btn" href="{{ url_for('bus_details', bus_id=bus_id) }}" style="width:100%;text-align:center;margin-top:16px">View Details</a></div>{% endfor %}</div>{% endblock %}''',

        "bus_details.html": '''{% extends "base.html" %}{% block content %}<div class="hero"><h1>🚌 Bus {{ bus_id }}</h1><p>{{ bus.route }}</p></div><div class="card"><h3>Route Information</h3><p><strong>Type:</strong> {{ bus.type }}</p><p><strong>Frequency:</strong> {{ bus.frequency }}</p><h4 style="margin-top:20px">Bus Stops</h4><ol style="padding-left:20px">{% for stop in bus.stops %}<li style="margin:8px 0">{{ stop.name }}</li>{% endfor %}</ol></div>{% if latest %}<div class="card"><h3>📍 Latest Position</h3><p><strong>Time:</strong> {{ latest.FormattedTime }}</p><p><strong>Speed:</strong> {{ "%.1f"|format(latest.SpeedKmph) }} km/h</p><p><strong>Next Stop:</strong> {{ latest.NextStop }}</p><p><strong>Distance:</strong> {{ "%.2f"|format(latest.DistanceToStopKm) }} km</p></div>{% endif %}<div style="display:flex;gap:12px;margin-top:20px"><a class="btn" href="{{ url_for('simulate', bus_id=bus_id) }}">🛰️ Simulate GPS</a><a class="btn secondary" href="{{ url_for('home') }}">Back to Home</a></div>{% endblock %}''',

        "bus_history.html": '''{% extends "base.html" %}{% block content %}<div class="hero"><h1>📜 Travel History</h1><p>Bus {{ bus_id }}</p></div>{% if items %}<div class="card"><table style="width:100%;border-collapse:collapse"><thead><tr style="border-bottom:2px solid #e2e8f0"><th style="padding:12px;text-align:left">Time</th><th style="padding:12px;text-align:left">Location</th><th style="padding:12px;text-align:left">Speed</th><th style="padding:12px;text-align:left">Next Stop</th></tr></thead><tbody>{% for item in items %}<tr style="border-bottom:1px solid #f1f5f9"><td style="padding:12px">{{ item['FormattedTime'] }}</td><td style="padding:12px">{% if 'Latitude' in item and item['Latitude'] is not none %}{{ "%.4f"|format(item['Latitude']) }}, {{ "%.4f"|format(item['Longitude']) }}{% else %}N/A{% endif %}</td><td style="padding:12px">{% if 'SpeedKmph' in item %}{{ "%.1f"|format(item['SpeedKmph']) }} km/h{% endif %}</td><td style="padding:12px">{{ item['NextStop'] if 'NextStop' in item else 'N/A' }}</td></tr>{% endfor %}</tbody></table></div>{% else %}<div class="card" style="text-align:center;padding:60px 20px"><p>No history available</p></div>{% endif %}<div style="margin-top:20px"><a class="btn" href="{{ url_for('bus_details', bus_id=bus_id) }}">Back to Details</a></div>{% endblock %}''',

        "subscribe.html": '''{% extends "base.html" %}{% block content %}<div class="hero"><h1>📧 Subscribe to Alerts</h1></div><div style="max-width:600px;margin:0 auto"><div class="card"><h3>Real-time Notifications</h3><p style="color:#64748b;margin-bottom:24px">Get email alerts when buses approach your stop</p><form method="POST"><label>Email Address</label><input type="email" name="email" value="{{ user_email }}" required><button class="btn" type="submit" style="width:100%">Subscribe</button></form></div></div>{% endblock %}'''
    }

    for name, content in templates.items():
        with open(f"templates/{name}", "w", encoding="utf-8") as f:
            f.write(content)
    logging.info("✅ Templates created")


if __name__ == "__main__":
    create_templates()
    logging.info("=" * 70)
    logging.info("🚌 RouteWise - City Bus Route Tracking System")
    logging.info("=" * 70)
    logging.info("1. Register at http://localhost:8080/register")
    logging.info("2. Login and click 🛰️ GPS buttons to simulate tracking")
    logging.info("3. View buses to add them to your travel history!")
    logging.info("=" * 70)
    app.run(host="0.0.0.0", port=8080, debug=True)
