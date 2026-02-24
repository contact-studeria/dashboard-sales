"""
STUDERIA Dashboard — Backend API (Vercel Serverless)
Fetches data from HubSpot API and serves aggregated KPIs to the React frontend.
Deployed as a Python serverless function on Vercel.
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, jsonify, request

app = Flask(__name__)


# ── CDN Cache Decorator (replaces in-memory cache for Vercel) ──
def cached_response(ttl=300):
    """Decorator that adds Vercel CDN cache headers to Flask responses."""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            result = f(*args, **kwargs)
            if isinstance(result, tuple):
                resp_obj, status = result
            else:
                resp_obj = result
                status = 200
            if hasattr(resp_obj, 'headers'):
                resp_obj.headers['Cache-Control'] = f'public, s-maxage={ttl}, stale-while-revalidate=60'
            return resp_obj, status
        return wrapped
    return decorator

# ── HubSpot Configuration ──
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
HUBSPOT_API = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

# ── Pipeline IDs ──
PIPELINE_B2C_PROSPECTION = "685822342"
PIPELINE_B2B_PROSPECTION = "678417416"
PIPELINE_CLOSING_DAILY = "861971502"
PIPELINE_B2C_GESTION = "678483715"
PIPELINE_B2B_GESTION = "698625070"
PIPELINE_CSM = "864278827"
PIPELINE_SETTING_DAILY = "863863401"

# ── Stage IDs ──
STAGE_SIGNE_B2C = "1004887918"       # Signé (B2C prospection)
STAGE_PERDU_B2C = "1004887919"       # Perdu (B2C prospection)
STAGE_RDV_PLANIFIE = "1004235133"    # RDV planifié
STAGE_NO_SHOW = "1008269824"         # No show
STAGE_PROPOSITION = "1013271142"     # Proposition envoyée
STAGE_R2 = "1188070754"             # R2
STAGE_NON_TRAITE = "1095528793"     # Non-traité
STAGE_CLOSING_SIGNE = "1290057291"  # Signé (closing daily)

# ── Setting Daily Stage IDs ──
SETTING_STAGES = {
    "1294068081": "Lead Funnel",
    "1300671024": "Lead prioritaire",
    "1291941662": "Nouveaux leads",
    "1291941663": "NRP 1",
    "1291941664": "NRP 2",
    "1291941665": "NRP 3",
    "1292501787": "Nurturing",
    "1292501786": "Qualifie",
    "1291941668": "Disqualifie",
}

# ── Closing Daily Stage IDs ──
CLOSING_STAGES = {
    "1290057281": "R1 booke",
    "1290057283": "R1 No Show / Annule",
    "1290057284": "R2 booke",
    "1290057285": "R2 No Show / Annule",
    "1290057286": "Follow Up Court",
    "1290057292": "Follow Up Long",
    "1290057293": "Deal Perdu",
    "1290057288": "Contrat signe",
    "1290057289": "Acompte percu",
    "1290057290": "Cash percu",
    "1290057291": "Deal Gagne",
}

# Ordered stage lists for conversion funnel
SETTING_STAGE_ORDER = [
    "1294068081",   # Lead Funnel
    "1300671024",   # Lead prioritaire
    "1291941662",   # Nouveaux leads
    "1291941663",   # NRP 1
    "1291941664",   # NRP 2
    "1291941665",   # NRP 3
    "1292501787",   # Nurturing
    "1292501786",   # Qualifie
    "1291941668",   # Disqualifie
]

CLOSING_STAGE_ORDER = [
    "1290057281",   # R1 booke
    "1290057283",   # R1 No Show / Annule
    "1290057284",   # R2 booke
    "1290057285",   # R2 No Show / Annule
    "1290057286",   # Follow Up Court
    "1290057292",   # Follow Up Long
    "1290057293",   # Deal Perdu
    "1290057288",   # Contrat signe
    "1290057289",   # Acompte percu
    "1290057290",   # Cash percu
    "1290057291",   # Deal Gagne
]

# ── Offer mapping (offre_choisie values → our offer IDs) ──
OFFRE_MAPPING = {
    "Consultant IA 3K": "incubateur",
    "Consultant IA 6K": "consultant",
    "Incubateur dirigeant IA 6K": "accelerateur",
    "Incubateur dirigeant IA 9K": "accelerateur_premium",
    "Contrat Élite IA - 25K": "elite",
}

OFFRE_PRICES = {
    "incubateur": 3000,
    "consultant": 6000,
    "accelerateur": 6000,
    "accelerateur_premium": 10000,
    "elite": 25000,
}

OFFRE_COST_PCT = {
    "incubateur": 0.1887,
    "consultant": 0.131,
    "accelerateur": 0.1892,
    "accelerateur_premium": 0.1915,
    "elite": 0.254,
}

# ── Source mapping (hs_analytics_source → dashboard sources) ──
SOURCE_MAPPING = {
    "PAID_SEARCH": "Google",
    "PAID_SOCIAL": "Meta",
    "SOCIAL_MEDIA": "Meta",
    "OFFLINE": "Maformation",
    "OTHER_CAMPAIGNS": "Maformation",
    "DIRECT_TRAFFIC": "Google",
    "REFERRALS": "Maformation",
    "ORGANIC_SEARCH": "Google",
    "EMAIL_MARKETING": "Maformation",
}

# ── Setter Owner IDs ──
SETTER_OWNER_IDS = {
    "87794914": "Benoit",
    "87991143": "Andria",
}

# ── Call disposition: connected values ──
# HubSpot hs_call_disposition GUIDs for "connected" calls
# Discovered via /api/debug/call-dispositions endpoint
CONNECTED_DISPOSITIONS = {
    "f240bbac-87c9-4f6e-bf70-924b57d47db7",  # Connected (1729 calls)
    # "73a0d17f-1163-4015-bdd5-ec830791da20" = No answer (231 calls)
    # "b2cf5968-551e-4856-9783-52b3da59a7d0" = Unknown (34 calls)
    # "17b47fee-58de-441e-a44c-c6300d46f273" = Unknown (6 calls)
}

# ── Meeting type inference keywords ──
MEETING_TYPE_KEYWORDS_R2 = ["r2", "R2", "deuxième", "2ème", "second", "deuxieme"]
MEETING_TYPE_KEYWORDS_FU = ["follow", "FU", "suivi", "follow-up", "follow up", "rappel", "relance"]

# ── Simple in-memory cache ──
_cache = {}
CACHE_TTL = 300  # 5 minutes


def cached(key, ttl=CACHE_TTL):
    """Decorator-like cache check."""
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < ttl:
            return data
    return None


def set_cache(key, data):
    _cache[key] = (data, time.time())


# ── HubSpot API helpers ──

def hubspot_search(object_type, filters=None, properties=None, sorts=None, limit=100):
    """Search HubSpot CRM objects with pagination."""
    url = f"{HUBSPOT_API}/crm/v3/objects/{object_type}/search"
    all_results = []
    after = 0

    while True:
        body = {"limit": min(limit, 100)}
        if filters:
            body["filterGroups"] = filters
        if properties:
            body["properties"] = properties
        if sorts:
            body["sorts"] = sorts
        if after:
            body["after"] = after

        try:
            resp = requests.post(url, headers=HEADERS, json=body, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"HubSpot API error: {e}")
            break

        results = data.get("results", [])
        all_results.extend(results)

        paging = data.get("paging", {})
        next_page = paging.get("next", {})
        if next_page.get("after") and len(all_results) < limit:
            after = next_page["after"]
        else:
            break

    return all_results


def hubspot_search_leads(filters=None, properties=None, limit=500):
    """Search HubSpot Leads objects with pagination (separate from deals)."""
    url = f"{HUBSPOT_API}/crm/v3/objects/leads/search"
    all_results = []
    after = 0

    while True:
        body = {"limit": min(limit, 100)}
        if filters:
            body["filterGroups"] = filters
        if properties:
            body["properties"] = properties
        if after:
            body["after"] = after

        try:
            resp = requests.post(url, headers=HEADERS, json=body, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"HubSpot Leads API error: {e}")
            break

        results = data.get("results", [])
        all_results.extend(results)

        paging = data.get("paging", {})
        next_page = paging.get("next", {})
        if next_page.get("after") and len(all_results) < limit:
            after = next_page["after"]
        else:
            break

    return all_results


def get_deals_for_period(pipeline_id, start_date, end_date, extra_properties=None):
    """Get all deals in a pipeline created within a date range."""
    props = [
        "dealname", "amount", "dealstage", "pipeline", "closedate",
        "createdate", "hs_analytics_source", "hubspot_owner_id",
        "offre_choisie", "offre_daily", "setter", "nom_setter",
        "montant_cpf", "montant_carte", "montant_acompte_virement",
        "montant_btob", "reste_a_charge", "hs_is_closed_won", "hs_is_closed_lost",
    ]
    if extra_properties:
        props.extend(extra_properties)

    filters = [{
        "filters": [
            {"propertyName": "pipeline", "operator": "EQ", "value": pipeline_id},
            {"propertyName": "createdate", "operator": "GTE", "value": start_date},
            {"propertyName": "createdate", "operator": "LTE", "value": end_date},
        ]
    }]

    return hubspot_search("deals", filters=filters, properties=props, limit=200)


def get_won_deals_for_period(pipeline_id, start_date, end_date, extra_properties=None):
    """Get won deals in a pipeline closed within a date range."""
    props = [
        "dealname", "amount", "dealstage", "pipeline", "closedate",
        "createdate", "hs_analytics_source", "hubspot_owner_id",
        "offre_choisie", "offre_daily", "setter", "nom_setter",
        "montant_cpf", "montant_carte", "montant_acompte_virement",
        "montant_btob", "reste_a_charge", "hs_is_closed_won",
    ]
    if extra_properties:
        props.extend(extra_properties)

    filters = [{
        "filters": [
            {"propertyName": "pipeline", "operator": "EQ", "value": pipeline_id},
            {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"},
            {"propertyName": "closedate", "operator": "GTE", "value": start_date},
            {"propertyName": "closedate", "operator": "LTE", "value": end_date},
        ]
    }]

    return hubspot_search("deals", filters=filters, properties=props, limit=500)


def get_contacts_for_period(start_date, end_date):
    """Get contacts created in a period."""
    props = [
        "firstname", "lastname", "email", "createdate",
        "hs_analytics_source", "b2b_b2c", "source_lead_rdv",
        "origine_du_lead", "hubspot_owner_id", "lifecyclestage",
    ]

    filters = [{
        "filters": [
            {"propertyName": "createdate", "operator": "GTE", "value": start_date},
            {"propertyName": "createdate", "operator": "LTE", "value": end_date},
        ]
    }]

    return hubspot_search("contacts", filters=filters, properties=props, limit=500)


def get_setting_leads_for_period(start_date, end_date):
    """Get leads in the Setting Daily pipeline created within a date range."""
    props = [
        "hs_lead_name", "hs_pipeline", "hs_pipeline_stage",
        "createdate", "hs_lead_status", "hubspot_owner_id",
        "hs_contact_analytics_source", "hs_contact_analytics_source_data_1",
    ]

    filters = [{
        "filters": [
            {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SETTING_DAILY},
            {"propertyName": "createdate", "operator": "GTE", "value": start_date},
            {"propertyName": "createdate", "operator": "LTE", "value": end_date},
        ]
    }]

    return hubspot_search_leads(filters=filters, properties=props, limit=500)


def get_all_setting_leads():
    """Get ALL leads currently in the Setting Daily pipeline (current snapshot, no date filter)."""
    props = [
        "hs_lead_name", "hs_pipeline", "hs_pipeline_stage",
        "createdate", "hs_lead_status", "hubspot_owner_id",
        "hs_contact_analytics_source", "hs_contact_analytics_source_data_1",
    ]

    filters = [{
        "filters": [
            {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SETTING_DAILY},
        ]
    }]

    return hubspot_search_leads(filters=filters, properties=props, limit=2000)


def get_closing_deals_for_period(start_date, end_date):
    """Get deals in the Closing Daily pipeline created within a date range."""
    props = [
        "dealname", "dealstage", "amount", "offre_choisie", "offre_daily",
        "closedate", "createdate", "hubspot_owner_id", "nom_setter",
        "hs_analytics_source", "montant_cpf", "montant_carte",
        "montant_acompte_virement", "montant_btob", "modalite_de_paiement_daily",
        "hs_is_closed_won", "hs_is_closed_lost", "pipeline",
    ]

    filters = [{
        "filters": [
            {"propertyName": "pipeline", "operator": "EQ", "value": PIPELINE_CLOSING_DAILY},
            {"propertyName": "createdate", "operator": "GTE", "value": start_date},
            {"propertyName": "createdate", "operator": "LTE", "value": end_date},
        ]
    }]

    return hubspot_search("deals", filters=filters, properties=props, limit=500)


def get_all_closing_deals():
    """Get ALL deals currently in the Closing Daily pipeline (current snapshot, no date filter)."""
    props = [
        "dealname", "dealstage", "amount", "offre_choisie", "offre_daily",
        "closedate", "createdate", "hubspot_owner_id", "nom_setter",
        "hs_analytics_source", "montant_cpf", "montant_carte",
        "montant_acompte_virement", "montant_btob", "modalite_de_paiement_daily",
        "hs_is_closed_won", "hs_is_closed_lost", "pipeline",
    ]

    filters = [{
        "filters": [
            {"propertyName": "pipeline", "operator": "EQ", "value": PIPELINE_CLOSING_DAILY},
        ]
    }]

    return hubspot_search("deals", filters=filters, properties=props, limit=2000)


# ── New HubSpot fetch functions (calls, meetings, contacts) ──

def get_calls_for_period(start_date, end_date, owner_ids=None):
    """Fetch calls within a date range, optionally filtered by owner IDs.
    start_date / end_date: ms timestamps as strings.
    owner_ids: list of owner ID strings, or None for all.
    """
    props = [
        "hs_call_duration", "hs_call_disposition", "hs_call_status",
        "hs_timestamp", "hubspot_owner_id",
    ]

    base_filters = [
        {"propertyName": "hs_timestamp", "operator": "GTE", "value": start_date},
        {"propertyName": "hs_timestamp", "operator": "LTE", "value": end_date},
    ]

    if owner_ids:
        # One filterGroup per owner (OR logic between owners)
        filter_groups = []
        for oid in owner_ids:
            filter_groups.append({
                "filters": base_filters + [
                    {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": oid},
                ]
            })
    else:
        filter_groups = [{"filters": base_filters}]

    return hubspot_search("calls", filters=filter_groups, properties=props, limit=2000)


def get_meetings_for_period(start_date, end_date):
    """Fetch meetings within a date range.
    start_date / end_date: ms timestamps as strings.
    """
    props = [
        "hs_meeting_title", "hs_meeting_start_time", "hs_meeting_end_time",
        "hs_meeting_outcome", "hubspot_owner_id",
    ]

    filters = [{
        "filters": [
            {"propertyName": "hs_meeting_start_time", "operator": "GTE", "value": start_date},
            {"propertyName": "hs_meeting_start_time", "operator": "LTE", "value": end_date},
        ]
    }]

    return hubspot_search("meetings", filters=filters, properties=props, limit=2000)


def get_contacts_with_reaction_time(start_date, end_date, owner_ids=None):
    """Fetch contacts created in date range with reaction time data.
    start_date / end_date: ms timestamps as strings.
    owner_ids: list of owner ID strings, or None for all.
    """
    props = [
        "firstname", "lastname", "createdate",
        "hubspot_owner_id", "hs_time_to_first_engagement",
    ]

    base_filters = [
        {"propertyName": "createdate", "operator": "GTE", "value": start_date},
        {"propertyName": "createdate", "operator": "LTE", "value": end_date},
        {"propertyName": "hs_time_to_first_engagement", "operator": "HAS_PROPERTY"},
    ]

    if owner_ids:
        filter_groups = []
        for oid in owner_ids:
            filter_groups.append({
                "filters": base_filters + [
                    {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": oid},
                ]
            })
    else:
        filter_groups = [{"filters": base_filters}]

    return hubspot_search("contacts", filters=filter_groups, properties=props, limit=2000)


# ── Date helpers ──

def get_month_range(year, month):
    """Return (start_ms, end_ms) for a given month."""
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1) - timedelta(milliseconds=1)
    else:
        end = datetime(year, month + 1, 1) - timedelta(milliseconds=1)
    return (
        str(int(start.timestamp() * 1000)),
        str(int(end.timestamp() * 1000)),
    )


def get_year_range(year):
    """Return (start_ms, end_ms) for a given year."""
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1) - timedelta(milliseconds=1)
    return (
        str(int(start.timestamp() * 1000)),
        str(int(end.timestamp() * 1000)),
    )


def parse_hubspot_datetime(ts_str):
    """Parse a HubSpot date value (ms timestamp OR ISO 8601 string) to a datetime object.
    Returns None on failure."""
    if not ts_str:
        return None
    # Try ms timestamp first (e.g., "1735689600000")
    try:
        ts_ms = int(ts_str)
        return datetime.fromtimestamp(ts_ms / 1000)
    except (ValueError, TypeError, OSError):
        pass
    # Try ISO 8601 (e.g., "2025-02-08T13:30:56.230Z")
    ts_clean = str(ts_str).replace("Z", "+00:00")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f+00:00", "%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%d"):
        try:
            return datetime.strptime(ts_clean, fmt)
        except ValueError:
            continue
    return None


def get_month_index_from_timestamp(ts_str, year=2026):
    """Convert a HubSpot date (ms or ISO) to a month index (0-11) for the given year.
    Returns -1 if the date is not in the given year."""
    dt = parse_hubspot_datetime(ts_str)
    if dt is None or dt.year != year:
        return -1
    return dt.month - 1


def get_date_string_from_timestamp(ts_str):
    """Convert a HubSpot date (ms or ISO) to an ISO date string 'YYYY-MM-DD'."""
    dt = parse_hubspot_datetime(ts_str)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def infer_meeting_type(title):
    """Infer R1/R2/FU from meeting title keywords."""
    if not title:
        return "R1"
    title_lower = title.lower()
    for kw in MEETING_TYPE_KEYWORDS_R2:
        if kw.lower() in title_lower:
            return "R2"
    for kw in MEETING_TYPE_KEYWORDS_FU:
        if kw.lower() in title_lower:
            return "FU"
    return "R1"


# ── New aggregation functions (daily breakdown, source×stage, calls, reaction, meetings) ──

def build_daily_breakdown(deals, year, month):
    """Group deals by day × closer for a specific month.
    Returns: { "YYYY-MM-DD": { owner_id: {newDeals, r1, noshow, r2, won, lost, ca, fu}, "_total": {...} } }
    """
    result = {}
    for deal in deals:
        props = deal.get("properties", {})
        createdate = props.get("createdate", "")
        if not createdate:
            continue
        date_str = get_date_string_from_timestamp(createdate)
        if not date_str:
            continue
        # Filter to the requested month
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            if dt.year != year or dt.month != month:
                continue
        except ValueError:
            continue

        owner_id = props.get("hubspot_owner_id", "unknown") or "unknown"
        stage = props.get("dealstage", "")
        amount = float(props.get("amount") or 0)
        is_won = props.get("hs_is_closed_won") == "true"
        is_lost = props.get("hs_is_closed_lost") == "true"

        if date_str not in result:
            result[date_str] = {}

        for key in [owner_id, "_total"]:
            if key not in result[date_str]:
                result[date_str][key] = {
                    "newDeals": 0, "r1": 0, "noshow": 0, "r2": 0,
                    "won": 0, "lost": 0, "ca": 0, "fu": 0,
                }
            bucket = result[date_str][key]
            bucket["newDeals"] += 1
            if is_won:
                bucket["won"] += 1
                bucket["ca"] += amount
            if is_lost:
                bucket["lost"] += 1

            # Stage-specific counters
            if stage == "1290057281":      # R1 booke
                bucket["r1"] += 1
            elif stage in ("1290057283", "1290057285"):  # R1/R2 No Show
                bucket["noshow"] += 1
            elif stage == "1290057284":    # R2 booke
                bucket["r2"] += 1
            elif stage in ("1290057286", "1290057292"):  # FU Court/Long
                bucket["fu"] += 1

    return result


def build_source_by_stage(deals):
    """Cross source × stage for deals.
    Returns: { source: {totalDeals, r1NoShow, r1Effectues, r2Bookes, r2NoShow,
                         fuCourt, fuLong, dealsSigne, caSigne, dealsPerdu} }
    """
    result = {}
    for deal in deals:
        props = deal.get("properties", {})
        source_raw = props.get("hs_analytics_source", "")
        source = SOURCE_MAPPING.get(source_raw, "Autre")
        stage = props.get("dealstage", "")
        amount = float(props.get("amount") or 0)
        is_won = props.get("hs_is_closed_won") == "true"
        is_lost = props.get("hs_is_closed_lost") == "true"

        if source not in result:
            result[source] = {
                "totalDeals": 0, "r1NoShow": 0, "r1Effectues": 0,
                "r2Bookes": 0, "r2NoShow": 0, "fuCourt": 0, "fuLong": 0,
                "dealsSigne": 0, "caSigne": 0, "dealsPerdu": 0,
            }
        s = result[source]
        s["totalDeals"] += 1

        if is_won:
            s["dealsSigne"] += 1
            s["caSigne"] += amount
        if is_lost:
            s["dealsPerdu"] += 1

        # Stage-specific
        if stage == "1290057283":         # R1 No Show
            s["r1NoShow"] += 1
        elif stage == "1290057281":       # R1 booke (effectué = passé en R1)
            s["r1Effectues"] += 1
        elif stage == "1290057284":       # R2 booke
            s["r2Bookes"] += 1
        elif stage == "1290057285":       # R2 No Show
            s["r2NoShow"] += 1
        elif stage == "1290057286":       # Follow Up Court
            s["fuCourt"] += 1
        elif stage == "1290057292":       # Follow Up Long
            s["fuLong"] += 1

    return result


def aggregate_calls(calls, year):
    """Aggregate call data per owner.
    Returns: { owner_id: {appelsTotaux, appelsDecroches, connectionRate, byMonth: [12]} }
    """
    result = {}
    for call in calls:
        props = call.get("properties", {})
        owner_id = props.get("hubspot_owner_id", "unknown") or "unknown"
        disposition = props.get("hs_call_disposition", "")
        ts = props.get("hs_timestamp", "")

        if owner_id not in result:
            result[owner_id] = {
                "appelsTotaux": 0,
                "appelsDecroches": 0,
                "connectionRate": 0,
                "totalDuration": 0,
                "byMonth": [{"total": 0, "connected": 0} for _ in range(12)],
            }

        result[owner_id]["appelsTotaux"] += 1
        is_connected = disposition in CONNECTED_DISPOSITIONS
        if is_connected:
            result[owner_id]["appelsDecroches"] += 1

        duration = int(props.get("hs_call_duration") or 0)
        result[owner_id]["totalDuration"] += duration

        if ts:
            month_idx = get_month_index_from_timestamp(ts, year)
            if 0 <= month_idx < 12:
                result[owner_id]["byMonth"][month_idx]["total"] += 1
                if is_connected:
                    result[owner_id]["byMonth"][month_idx]["connected"] += 1

    # Compute connection rates
    for owner_id in result:
        stats = result[owner_id]
        total = stats["appelsTotaux"]
        if total > 0:
            stats["connectionRate"] = round(stats["appelsDecroches"] / total * 100, 2)
        stats["avgDuration"] = round(stats["totalDuration"] / total, 1) if total > 0 else 0

    return result


def aggregate_reaction_times(contacts, year):
    """Compute median/avg reaction time per owner from contacts.
    Returns: { owner_id: {median_minutes, avg_minutes, count, byMonth: [12]} }
    """
    import statistics

    owner_times = {}  # owner_id -> list of ms values
    owner_monthly = {}  # owner_id -> [12 lists of ms values]

    for contact in contacts:
        props = contact.get("properties", {})
        owner_id = props.get("hubspot_owner_id", "unknown") or "unknown"
        reaction_ms = props.get("hs_time_to_first_engagement")
        createdate = props.get("createdate", "")

        if not reaction_ms:
            continue
        try:
            reaction_val = float(reaction_ms)
        except (ValueError, TypeError):
            continue

        if owner_id not in owner_times:
            owner_times[owner_id] = []
            owner_monthly[owner_id] = [[] for _ in range(12)]

        owner_times[owner_id].append(reaction_val)

        if createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                owner_monthly[owner_id][month_idx].append(reaction_val)

    result = {}
    for owner_id, times in owner_times.items():
        if not times:
            continue

        median_ms = statistics.median(times)
        avg_ms = statistics.mean(times)

        monthly_stats = []
        for m in range(12):
            m_times = owner_monthly[owner_id][m]
            if m_times:
                monthly_stats.append({
                    "median_minutes": round(statistics.median(m_times) / 60000, 1),
                    "avg_minutes": round(statistics.mean(m_times) / 60000, 1),
                    "count": len(m_times),
                })
            else:
                monthly_stats.append({"median_minutes": 0, "avg_minutes": 0, "count": 0})

        result[owner_id] = {
            "median_minutes": round(median_ms / 60000, 1),
            "avg_minutes": round(avg_ms / 60000, 1),
            "count": len(times),
            "byMonth": monthly_stats,
        }

    return result


def aggregate_meetings(meetings):
    """Aggregate meetings by date.
    Returns: { "YYYY-MM-DD": [{ owner, title, start, end, outcome, type }] }
    """
    result = {}
    summary = {"total": 0, "completed": 0, "noShow": 0, "cancelled": 0}

    for meeting in meetings:
        props = meeting.get("properties", {})
        title = props.get("hs_meeting_title", "")
        start_time = props.get("hs_meeting_start_time", "")
        end_time = props.get("hs_meeting_end_time", "")
        outcome = props.get("hs_meeting_outcome", "")
        owner_id = props.get("hubspot_owner_id", "")

        if not start_time:
            continue

        date_str = get_date_string_from_timestamp(start_time)
        if not date_str:
            continue

        # Parse times for display
        try:
            start_dt = datetime.fromtimestamp(int(start_time) / 1000)
            start_hm = start_dt.strftime("%H:%M")
        except (ValueError, TypeError, OSError):
            start_hm = ""

        try:
            end_dt = datetime.fromtimestamp(int(end_time) / 1000)
            end_hm = end_dt.strftime("%H:%M")
        except (ValueError, TypeError, OSError):
            end_hm = ""

        meeting_type = infer_meeting_type(title)

        entry = {
            "owner": owner_id,
            "title": title,
            "start": start_hm,
            "end": end_hm,
            "outcome": outcome,
            "type": meeting_type,
        }

        if date_str not in result:
            result[date_str] = []
        result[date_str].append(entry)

        summary["total"] += 1
        outcome_lower = (outcome or "").lower()
        if "completed" in outcome_lower or "scheduled" in outcome_lower:
            summary["completed"] += 1
        elif "no show" in outcome_lower or "noshow" in outcome_lower:
            summary["noShow"] += 1
        elif "cancel" in outcome_lower or "rescheduled" in outcome_lower:
            summary["cancelled"] += 1

    # Sort meetings per day by start time
    for date_str in result:
        result[date_str].sort(key=lambda m: m["start"])

    total = summary["total"]
    summary["showUpRate"] = round(
        (summary["completed"] / total * 100) if total > 0 else 0, 2
    )

    return {"byDate": result, "summary": summary}


# ── Data aggregation ──

def aggregate_monthly_data(year=2026):
    """Build the full dashboard data structure from HubSpot."""
    cache_key = f"dashboard_{year}"
    cached_data = cached(cache_key)
    if cached_data:
        return cached_data

    result = {
        "dashboard": {
            "caEncaisse": [None] * 12,
            "nbClients": [None] * 12,
            "marketingSpend": [None] * 12,
            "agenceMarketing": [None] * 12,
            "equipeCommerciale": [None] * 12,
            "coutsProduits": [None] * 12,
            "coutsFixes": [70000] * 12,
        },
        "suiviOffre": {},
        "offerDetails": {},
        "sales": {
            "b2c": {"setting": {}, "closing": {}},
            "b2b": {"setting": {}, "closing": {}},
        },
        "closers": {},
        "setters": {},
        "lastUpdated": datetime.now().isoformat(),
    }

    # Init suivi offre
    for offer_id in ["incubateur", "consultant", "accelerateur", "accelerateur_premium", "elite"]:
        result["suiviOffre"][offer_id] = {
            "volume": [0] * 12,
            "ca": [0] * 12,
            "coutPct": [OFFRE_COST_PCT.get(offer_id, 0)] * 12,
            "margeBrute": [0] * 12,
        }

    # Init offer details
    for offer_id in ["incubateur", "consultant", "accelerateur", "elite"]:
        result["offerDetails"][offer_id] = {
            "acquisition": {},
            "closing": {},
            "revenue": {},
        }
        for source in ["Maformation", "Google", "Meta"]:
            result["offerDetails"][offer_id]["acquisition"][source] = {
                "leads": [0] * 12, "rdvBooked": [0] * 12,
            }
            result["offerDetails"][offer_id]["closing"][source] = {
                "rdvRealise": [0] * 12, "dealsSigne": [0] * 12,
            }
            result["offerDetails"][offer_id]["revenue"][source] = {
                "revenue": [0] * 12, "commission": [0] * 12,
            }

    # Process each month
    for month_idx in range(12):
        month_num = month_idx + 1
        start_ms, end_ms = get_month_range(year, month_num)

        # Only fetch data for past/current months
        now = datetime.now()
        if year > now.year or (year == now.year and month_num > now.month):
            continue

        # ── Fetch all pipelines for this month ──
        b2c_deals = get_deals_for_period(PIPELINE_B2C_PROSPECTION, start_ms, end_ms)
        b2b_deals = get_deals_for_period(PIPELINE_B2B_PROSPECTION, start_ms, end_ms)
        closing_deals = get_deals_for_period(PIPELINE_CLOSING_DAILY, start_ms, end_ms)

        # Won deals (by close date for revenue)
        b2c_won = get_won_deals_for_period(PIPELINE_B2C_PROSPECTION, start_ms, end_ms)
        closing_won = get_won_deals_for_period(PIPELINE_CLOSING_DAILY, start_ms, end_ms)

        # Contacts (leads)
        contacts = get_contacts_for_period(start_ms, end_ms)

        # ── Dashboard P&L ──
        all_won = b2c_won + closing_won
        total_ca = sum(float(d["properties"].get("amount") or 0) for d in all_won)
        nb_clients = len(all_won)

        result["dashboard"]["caEncaisse"][month_idx] = total_ca
        result["dashboard"]["nbClients"][month_idx] = nb_clients

        # Compute costs from won deals by offer
        total_product_cost = 0
        for deal in all_won:
            props = deal["properties"]
            offre_raw = props.get("offre_choisie", "")
            offer_id = OFFRE_MAPPING.get(offre_raw, "incubateur")
            amount = float(props.get("amount") or 0)
            cost_pct = OFFRE_COST_PCT.get(offer_id, 0.15)
            total_product_cost += amount * cost_pct

        result["dashboard"]["coutsProduits"][month_idx] = round(total_product_cost, 2)

        # ── Suivi par offre ──
        for deal in all_won:
            props = deal["properties"]
            offre_raw = props.get("offre_choisie", "")
            offer_id = OFFRE_MAPPING.get(offre_raw)

            if not offer_id:
                # Try to infer from amount
                amount = float(props.get("amount") or 0)
                if amount <= 3500:
                    offer_id = "incubateur"
                elif amount <= 6500:
                    offer_id = "consultant"
                elif amount <= 10500:
                    offer_id = "accelerateur_premium"
                else:
                    offer_id = "elite"

            amount = float(props.get("amount") or 0)
            cost_pct = OFFRE_COST_PCT.get(offer_id, 0.15)

            if offer_id in result["suiviOffre"]:
                result["suiviOffre"][offer_id]["volume"][month_idx] += 1
                result["suiviOffre"][offer_id]["ca"][month_idx] += amount
                marge = amount * (1 - cost_pct)
                result["suiviOffre"][offer_id]["margeBrute"][month_idx] += round(marge, 2)

        # ── Offer details by source ──
        for deal in all_won:
            props = deal["properties"]
            offre_raw = props.get("offre_choisie", "")
            offer_id = OFFRE_MAPPING.get(offre_raw)
            if not offer_id:
                amount = float(props.get("amount") or 0)
                if amount <= 3500:
                    offer_id = "incubateur"
                elif amount <= 6500:
                    offer_id = "consultant"
                elif amount <= 10500:
                    offer_id = "accelerateur"
                else:
                    offer_id = "elite"

            source_raw = props.get("hs_analytics_source", "")
            source = SOURCE_MAPPING.get(source_raw, "Maformation")
            amount = float(props.get("amount") or 0)

            if offer_id in result["offerDetails"] and source in result["offerDetails"][offer_id]["closing"]:
                result["offerDetails"][offer_id]["closing"][source]["dealsSigne"][month_idx] += 1
                result["offerDetails"][offer_id]["revenue"][source]["revenue"][month_idx] += amount

        # ── Count leads by source for offer details ──
        for contact in contacts:
            props = contact["properties"]
            source_raw = props.get("hs_analytics_source", "")
            source = SOURCE_MAPPING.get(source_raw, "Maformation")

            for offer_id in ["incubateur", "consultant", "accelerateur", "elite"]:
                if source in result["offerDetails"][offer_id]["acquisition"]:
                    result["offerDetails"][offer_id]["acquisition"][source]["leads"][month_idx] += 1

        # Distribute leads evenly across offers (rough approximation)
        for offer_id in ["incubateur", "consultant", "accelerateur", "elite"]:
            for source in ["Maformation", "Google", "Meta"]:
                total_leads = result["offerDetails"][offer_id]["acquisition"][source]["leads"][month_idx]
                result["offerDetails"][offer_id]["acquisition"][source]["leads"][month_idx] = total_leads // 4

        # ── Sales: Closers performance ──
        closer_stats = {}
        for deal in closing_deals + b2c_deals:
            props = deal["properties"]
            owner_id = props.get("hubspot_owner_id", "unknown")
            is_won = props.get("hs_is_closed_won") == "true"
            amount = float(props.get("amount") or 0)

            if owner_id not in closer_stats:
                closer_stats[owner_id] = {"total": 0, "won": 0, "revenue": 0}
            closer_stats[owner_id]["total"] += 1
            if is_won:
                closer_stats[owner_id]["won"] += 1
                closer_stats[owner_id]["revenue"] += amount

        result["closers"][str(month_idx)] = closer_stats

        # ── Sales: Setters performance ──
        setter_stats = {}
        for deal in closing_deals + b2c_deals:
            props = deal["properties"]
            setter_id = props.get("setter", "")
            if setter_id:
                if setter_id not in setter_stats:
                    setter_stats[setter_id] = {"rdvBooked": 0}
                setter_stats[setter_id]["rdvBooked"] += 1

        result["setters"][str(month_idx)] = setter_stats

    set_cache(cache_key, result)
    return result


def get_live_summary():
    """Get a quick live summary for the current month only."""
    cache_key = "live_summary"
    cached_data = cached(cache_key, ttl=120)  # 2 min cache
    if cached_data:
        return cached_data

    now = datetime.now()
    start_ms, end_ms = get_month_range(now.year, now.month)

    # Current month deals
    closing_deals = get_deals_for_period(PIPELINE_CLOSING_DAILY, start_ms, end_ms)
    b2c_deals = get_deals_for_period(PIPELINE_B2C_PROSPECTION, start_ms, end_ms)
    b2b_deals = get_deals_for_period(PIPELINE_B2B_PROSPECTION, start_ms, end_ms)

    # Won deals
    closing_won = [d for d in closing_deals if d["properties"].get("hs_is_closed_won") == "true"]
    b2c_won = [d for d in b2c_deals if d["properties"].get("hs_is_closed_won") == "true"]

    all_won = closing_won + b2c_won
    total_ca = sum(float(d["properties"].get("amount") or 0) for d in all_won)
    nb_clients = len(all_won)

    # Deals by offer
    offers_summary = {}
    for deal in all_won:
        props = deal["properties"]
        offre_raw = props.get("offre_choisie", "")
        offer_id = OFFRE_MAPPING.get(offre_raw, "incubateur")
        if offer_id not in offers_summary:
            offers_summary[offer_id] = {"count": 0, "revenue": 0}
        offers_summary[offer_id]["count"] += 1
        offers_summary[offer_id]["revenue"] += float(props.get("amount") or 0)

    # Deals by source
    source_summary = {}
    for deal in all_won:
        props = deal["properties"]
        source_raw = props.get("hs_analytics_source", "")
        source = SOURCE_MAPPING.get(source_raw, "Autre")
        if source not in source_summary:
            source_summary[source] = {"count": 0, "revenue": 0}
        source_summary[source]["count"] += 1
        source_summary[source]["revenue"] += float(props.get("amount") or 0)

    # Pipeline counts
    pipeline_counts = {
        "b2c_total": len(b2c_deals),
        "b2c_won": len(b2c_won),
        "b2b_total": len(b2b_deals),
        "closing_total": len(closing_deals),
        "closing_won": len(closing_won),
    }

    # Closer leaderboard
    closer_board = {}
    for deal in all_won:
        owner_id = deal["properties"].get("hubspot_owner_id", "")
        if owner_id:
            if owner_id not in closer_board:
                closer_board[owner_id] = {"deals": 0, "revenue": 0}
            closer_board[owner_id]["deals"] += 1
            closer_board[owner_id]["revenue"] += float(deal["properties"].get("amount") or 0)

    summary = {
        "month": now.strftime("%B %Y"),
        "monthIndex": now.month - 1,
        "totalCA": total_ca,
        "nbClients": nb_clients,
        "offersSummary": offers_summary,
        "sourceSummary": source_summary,
        "pipelineCounts": pipeline_counts,
        "closerLeaderboard": closer_board,
        "totalDealsInPipeline": len(closing_deals) + len(b2c_deals),
        "lastUpdated": now.isoformat(),
    }

    set_cache(cache_key, summary)
    return summary


# ── Setting Daily aggregation ──

def aggregate_setting_daily(year=2026):
    """Build the Setting Daily pipeline data from HubSpot Leads."""
    cache_key = f"setting_daily_{year}"
    cached_data = cached(cache_key)
    if cached_data:
        return cached_data

    year_start_ms, year_end_ms = get_year_range(year)

    # Fetch all leads in the setting daily pipeline (snapshot)
    all_leads = get_all_setting_leads()

    # Fetch leads created this year for monthly breakdown
    year_leads = get_setting_leads_for_period(year_start_ms, year_end_ms)

    # ── Current snapshot: leads per stage ──
    stage_counts = {}
    for stage_id in SETTING_STAGES:
        stage_counts[stage_id] = {
            "name": SETTING_STAGES[stage_id],
            "count": 0,
        }

    for lead in all_leads:
        props = lead.get("properties", {})
        stage = props.get("hs_pipeline_stage", "")
        if stage in stage_counts:
            stage_counts[stage]["count"] += 1

    # ── Monthly breakdown: leads created per month per stage ──
    monthly_by_stage = {}
    for stage_id in SETTING_STAGES:
        monthly_by_stage[stage_id] = {
            "name": SETTING_STAGES[stage_id],
            "months": [0] * 12,
        }

    for lead in year_leads:
        props = lead.get("properties", {})
        stage = props.get("hs_pipeline_stage", "")
        createdate = props.get("createdate", "")
        if stage in monthly_by_stage and createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                monthly_by_stage[stage]["months"][month_idx] += 1

    # ── Per-owner breakdown ──
    owner_stats = {}
    for lead in year_leads:
        props = lead.get("properties", {})
        owner_id = props.get("hubspot_owner_id", "unknown") or "unknown"
        stage = props.get("hs_pipeline_stage", "")

        if owner_id not in owner_stats:
            owner_stats[owner_id] = {
                "total": 0,
                "qualified": 0,
                "disqualified": 0,
                "nurturing": 0,
                "nrp": 0,
                "by_stage": {},
            }

        owner_stats[owner_id]["total"] += 1

        if stage == "1292501786":  # Qualifie
            owner_stats[owner_id]["qualified"] += 1
        elif stage == "1291941668":  # Disqualifie
            owner_stats[owner_id]["disqualified"] += 1
        elif stage == "1292501787":  # Nurturing
            owner_stats[owner_id]["nurturing"] += 1
        elif stage in ("1291941663", "1291941664", "1291941665"):  # NRP 1/2/3
            owner_stats[owner_id]["nrp"] += 1

        stage_name = SETTING_STAGES.get(stage, "Unknown")
        if stage_name not in owner_stats[owner_id]["by_stage"]:
            owner_stats[owner_id]["by_stage"][stage_name] = 0
        owner_stats[owner_id]["by_stage"][stage_name] += 1

    # ── Conversion rates ──
    total_all = len(year_leads)
    total_qualified = sum(1 for l in year_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1292501786")
    total_disqualified = sum(1 for l in year_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1291941668")
    total_nurturing = sum(1 for l in year_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1292501787")
    total_nouveaux = sum(1 for l in year_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1291941662")

    conversion_rates = {
        "lead_to_qualified": round((total_qualified / total_all * 100) if total_all > 0 else 0, 2),
        "lead_to_disqualified": round((total_disqualified / total_all * 100) if total_all > 0 else 0, 2),
        "lead_to_nurturing": round((total_nurturing / total_all * 100) if total_all > 0 else 0, 2),
        "nouveaux_to_qualified": round((total_qualified / total_nouveaux * 100) if total_nouveaux > 0 else 0, 2),
    }

    # ── Per-owner conversion rates ──
    for owner_id in owner_stats:
        stats = owner_stats[owner_id]
        total = stats["total"]
        if total > 0:
            stats["conversion_rate"] = round(stats["qualified"] / total * 100, 2)
            stats["disqualification_rate"] = round(stats["disqualified"] / total * 100, 2)
        else:
            stats["conversion_rate"] = 0
            stats["disqualification_rate"] = 0

    result = {
        "pipelineId": PIPELINE_SETTING_DAILY,
        "pipelineName": "SETTING STUDERIA 2026 - DAILY",
        "year": year,
        "snapshot": {
            "totalLeads": len(all_leads),
            "byStage": stage_counts,
            "stageOrder": SETTING_STAGE_ORDER,
        },
        "monthly": {
            "byStage": monthly_by_stage,
            "totalByMonth": [0] * 12,
        },
        "owners": owner_stats,
        "conversionRates": conversion_rates,
        "lastUpdated": datetime.now().isoformat(),
    }

    # Compute total leads created per month
    for lead in year_leads:
        props = lead.get("properties", {})
        createdate = props.get("createdate", "")
        if createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                result["monthly"]["totalByMonth"][month_idx] += 1

    # ── NEW: Setter call metrics ──
    try:
        setter_ids = list(SETTER_OWNER_IDS.keys())
        calls = get_calls_for_period(year_start_ms, year_end_ms, owner_ids=setter_ids)
        result["callMetrics"] = aggregate_calls(calls, year)
        # Map owner IDs to setter names
        for oid, name in SETTER_OWNER_IDS.items():
            if oid in result["callMetrics"]:
                result["callMetrics"][oid]["name"] = name
    except Exception as e:
        print(f"Error fetching call metrics: {e}")
        result["callMetrics"] = {}

    # ── NEW: Setter reaction times ──
    try:
        contacts = get_contacts_with_reaction_time(year_start_ms, year_end_ms, owner_ids=setter_ids)
        result["reactionTimes"] = aggregate_reaction_times(contacts, year)
        for oid, name in SETTER_OWNER_IDS.items():
            if oid in result["reactionTimes"]:
                result["reactionTimes"][oid]["name"] = name
    except Exception as e:
        print(f"Error fetching reaction times: {e}")
        result["reactionTimes"] = {}

    set_cache(cache_key, result)
    return result


# ── Closing Daily aggregation ──

def aggregate_closing_daily(year=2026):
    """Build the Closing Daily pipeline data from HubSpot Deals."""
    cache_key = f"closing_daily_{year}"
    cached_data = cached(cache_key)
    if cached_data:
        return cached_data

    year_start_ms, year_end_ms = get_year_range(year)

    # Fetch all deals in the closing daily pipeline (snapshot)
    all_deals = get_all_closing_deals()

    # Fetch deals created this year for monthly breakdown
    year_deals = get_closing_deals_for_period(year_start_ms, year_end_ms)

    # ── Current snapshot: deals per stage ──
    stage_counts = {}
    for stage_id in CLOSING_STAGES:
        stage_counts[stage_id] = {
            "name": CLOSING_STAGES[stage_id],
            "count": 0,
            "revenue": 0,
        }

    for deal in all_deals:
        props = deal.get("properties", {})
        stage = props.get("dealstage", "")
        amount = float(props.get("amount") or 0)
        if stage in stage_counts:
            stage_counts[stage]["count"] += 1
            stage_counts[stage]["revenue"] += amount

    # ── Monthly breakdown: deals per month per stage ──
    monthly_by_stage = {}
    for stage_id in CLOSING_STAGES:
        monthly_by_stage[stage_id] = {
            "name": CLOSING_STAGES[stage_id],
            "counts": [0] * 12,
            "revenue": [0] * 12,
        }

    for deal in year_deals:
        props = deal.get("properties", {})
        stage = props.get("dealstage", "")
        createdate = props.get("createdate", "")
        amount = float(props.get("amount") or 0)
        if stage in monthly_by_stage and createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                monthly_by_stage[stage]["counts"][month_idx] += 1
                monthly_by_stage[stage]["revenue"][month_idx] += amount

    # ── Per-owner (closer) breakdown ──
    owner_stats = {}
    for deal in year_deals:
        props = deal.get("properties", {})
        owner_id = props.get("hubspot_owner_id", "unknown") or "unknown"
        stage = props.get("dealstage", "")
        amount = float(props.get("amount") or 0)
        is_won = props.get("hs_is_closed_won") == "true"
        is_lost = props.get("hs_is_closed_lost") == "true"

        if owner_id not in owner_stats:
            owner_stats[owner_id] = {
                "total": 0,
                "won": 0,
                "lost": 0,
                "revenue_won": 0,
                "r1_booked": 0,
                "r1_no_show": 0,
                "r2_booked": 0,
                "r2_no_show": 0,
                "contrat_signe": 0,
                "by_stage": {},
            }

        owner_stats[owner_id]["total"] += 1

        if is_won:
            owner_stats[owner_id]["won"] += 1
            owner_stats[owner_id]["revenue_won"] += amount
        if is_lost:
            owner_stats[owner_id]["lost"] += 1

        if stage == "1290057281":
            owner_stats[owner_id]["r1_booked"] += 1
        elif stage == "1290057283":
            owner_stats[owner_id]["r1_no_show"] += 1
        elif stage == "1290057284":
            owner_stats[owner_id]["r2_booked"] += 1
        elif stage == "1290057285":
            owner_stats[owner_id]["r2_no_show"] += 1
        elif stage == "1290057288":
            owner_stats[owner_id]["contrat_signe"] += 1

        stage_name = CLOSING_STAGES.get(stage, "Unknown")
        if stage_name not in owner_stats[owner_id]["by_stage"]:
            owner_stats[owner_id]["by_stage"][stage_name] = 0
        owner_stats[owner_id]["by_stage"][stage_name] += 1

    # Per-owner conversion rates
    for owner_id in owner_stats:
        stats = owner_stats[owner_id]
        total = stats["total"]
        if total > 0:
            stats["win_rate"] = round(stats["won"] / total * 100, 2)
            stats["loss_rate"] = round(stats["lost"] / total * 100, 2)
            stats["no_show_rate"] = round((stats["r1_no_show"] + stats["r2_no_show"]) / total * 100, 2)
        else:
            stats["win_rate"] = 0
            stats["loss_rate"] = 0
            stats["no_show_rate"] = 0

    # ── Source attribution ──
    source_breakdown = {}
    for deal in year_deals:
        props = deal.get("properties", {})
        source_raw = props.get("hs_analytics_source", "")
        source = SOURCE_MAPPING.get(source_raw, "Autre")
        amount = float(props.get("amount") or 0)
        is_won = props.get("hs_is_closed_won") == "true"

        if source not in source_breakdown:
            source_breakdown[source] = {
                "total": 0,
                "won": 0,
                "revenue_won": 0,
                "by_month": [0] * 12,
                "revenue_by_month": [0] * 12,
            }

        source_breakdown[source]["total"] += 1
        if is_won:
            source_breakdown[source]["won"] += 1
            source_breakdown[source]["revenue_won"] += amount

        createdate = props.get("createdate", "")
        if createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                source_breakdown[source]["by_month"][month_idx] += 1
                if is_won:
                    source_breakdown[source]["revenue_by_month"][month_idx] += amount

    # ── Offer breakdown ──
    offer_breakdown = {}
    for deal in year_deals:
        props = deal.get("properties", {})
        offre_raw = props.get("offre_choisie", "") or props.get("offre_daily", "") or ""
        offer_id = OFFRE_MAPPING.get(offre_raw, "autre")
        amount = float(props.get("amount") or 0)
        is_won = props.get("hs_is_closed_won") == "true"

        if offer_id not in offer_breakdown:
            offer_breakdown[offer_id] = {
                "total": 0,
                "won": 0,
                "revenue_won": 0,
                "by_month": [0] * 12,
            }

        offer_breakdown[offer_id]["total"] += 1
        if is_won:
            offer_breakdown[offer_id]["won"] += 1
            offer_breakdown[offer_id]["revenue_won"] += amount

        createdate = props.get("createdate", "")
        if createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                offer_breakdown[offer_id]["by_month"][month_idx] += 1

    # ── Payment details aggregated ──
    payment_totals = {
        "montant_cpf": 0,
        "montant_carte": 0,
        "montant_acompte_virement": 0,
        "montant_btob": 0,
        "by_month": {
            "montant_cpf": [0] * 12,
            "montant_carte": [0] * 12,
            "montant_acompte_virement": [0] * 12,
            "montant_btob": [0] * 12,
        },
    }

    won_deals = [d for d in year_deals if d.get("properties", {}).get("hs_is_closed_won") == "true"]
    for deal in won_deals:
        props = deal.get("properties", {})
        cpf = float(props.get("montant_cpf") or 0)
        carte = float(props.get("montant_carte") or 0)
        virement = float(props.get("montant_acompte_virement") or 0)
        btob = float(props.get("montant_btob") or 0)

        payment_totals["montant_cpf"] += cpf
        payment_totals["montant_carte"] += carte
        payment_totals["montant_acompte_virement"] += virement
        payment_totals["montant_btob"] += btob

        createdate = props.get("createdate", "")
        if createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                payment_totals["by_month"]["montant_cpf"][month_idx] += cpf
                payment_totals["by_month"]["montant_carte"][month_idx] += carte
                payment_totals["by_month"]["montant_acompte_virement"][month_idx] += virement
                payment_totals["by_month"]["montant_btob"][month_idx] += btob

    # ── Payment modality breakdown ──
    modality_breakdown = {}
    for deal in won_deals:
        props = deal.get("properties", {})
        modality = props.get("modalite_de_paiement_daily", "") or "Non renseigne"
        amount = float(props.get("amount") or 0)

        if modality not in modality_breakdown:
            modality_breakdown[modality] = {"count": 0, "revenue": 0}
        modality_breakdown[modality]["count"] += 1
        modality_breakdown[modality]["revenue"] += amount

    # ── Global conversion rates ──
    total_deals = len(year_deals)
    total_won = len(won_deals)
    total_lost = sum(1 for d in year_deals if d.get("properties", {}).get("hs_is_closed_lost") == "true")
    total_r1 = sum(1 for d in year_deals if d.get("properties", {}).get("dealstage") == "1290057281")
    total_r2 = sum(1 for d in year_deals if d.get("properties", {}).get("dealstage") == "1290057284")
    total_r1_no_show = sum(1 for d in year_deals if d.get("properties", {}).get("dealstage") == "1290057283")
    total_r2_no_show = sum(1 for d in year_deals if d.get("properties", {}).get("dealstage") == "1290057285")
    total_contrat = sum(1 for d in year_deals if d.get("properties", {}).get("dealstage") == "1290057288")

    conversion_rates = {
        "overall_win_rate": round((total_won / total_deals * 100) if total_deals > 0 else 0, 2),
        "overall_loss_rate": round((total_lost / total_deals * 100) if total_deals > 0 else 0, 2),
        "r1_to_r2": round((total_r2 / total_r1 * 100) if total_r1 > 0 else 0, 2),
        "r1_no_show_rate": round((total_r1_no_show / (total_r1 + total_r1_no_show) * 100) if (total_r1 + total_r1_no_show) > 0 else 0, 2),
        "r2_no_show_rate": round((total_r2_no_show / (total_r2 + total_r2_no_show) * 100) if (total_r2 + total_r2_no_show) > 0 else 0, 2),
        "contrat_to_won": round((total_won / total_contrat * 100) if total_contrat > 0 else 0, 2),
    }

    # ── Revenue summary ──
    total_revenue_won = sum(float(d.get("properties", {}).get("amount") or 0) for d in won_deals)
    revenue_by_month = [0] * 12
    for deal in won_deals:
        props = deal.get("properties", {})
        amount = float(props.get("amount") or 0)
        createdate = props.get("createdate", "")
        if createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                revenue_by_month[month_idx] += amount

    result = {
        "pipelineId": PIPELINE_CLOSING_DAILY,
        "pipelineName": "CLOSING STUDERIA 2026 - DAILY",
        "year": year,
        "snapshot": {
            "totalDeals": len(all_deals),
            "byStage": stage_counts,
            "stageOrder": CLOSING_STAGE_ORDER,
        },
        "monthly": {
            "byStage": monthly_by_stage,
            "totalByMonth": [0] * 12,
            "revenueByMonth": revenue_by_month,
        },
        "owners": owner_stats,
        "conversionRates": conversion_rates,
        "sourceBreakdown": source_breakdown,
        "offerBreakdown": offer_breakdown,
        "paymentDetails": payment_totals,
        "modalityBreakdown": modality_breakdown,
        "revenueSummary": {
            "totalWon": total_revenue_won,
            "byMonth": revenue_by_month,
            "avgDealSize": round(total_revenue_won / total_won, 2) if total_won > 0 else 0,
        },
        "lastUpdated": datetime.now().isoformat(),
    }

    # Compute total deals created per month
    for deal in year_deals:
        props = deal.get("properties", {})
        createdate = props.get("createdate", "")
        if createdate:
            month_idx = get_month_index_from_timestamp(createdate, year)
            if 0 <= month_idx < 12:
                result["monthly"]["totalByMonth"][month_idx] += 1

    # ── NEW: Daily breakdown per closer (for current month) ──
    current_month = datetime.now().month
    result["dailyBreakdown"] = build_daily_breakdown(year_deals, year, current_month)

    # ── NEW: Source × Stage cross-table ──
    result["sourceByStage"] = build_source_by_stage(year_deals)

    set_cache(cache_key, result)
    return result


# ── Pipeline Summary (combined) ──

def get_pipeline_summary(year=2026):
    """Get a combined quick summary of both Setting and Closing daily pipelines."""
    cache_key = f"pipeline_summary_{year}"
    cached_data = cached(cache_key)
    if cached_data:
        return cached_data

    now = datetime.now()
    current_month_start, current_month_end = get_month_range(now.year, now.month)

    # ── Setting pipeline snapshot ──
    all_setting_leads = get_all_setting_leads()
    setting_total = len(all_setting_leads)
    setting_qualified = sum(1 for l in all_setting_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1292501786")
    setting_disqualified = sum(1 for l in all_setting_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1291941668")
    setting_nurturing = sum(1 for l in all_setting_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1292501787")
    setting_nouveaux = sum(1 for l in all_setting_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1291941662")
    setting_nrp = sum(1 for l in all_setting_leads if l.get("properties", {}).get("hs_pipeline_stage") in ("1291941663", "1291941664", "1291941665"))

    # Current month setting leads
    setting_month_leads = get_setting_leads_for_period(current_month_start, current_month_end)
    setting_month_total = len(setting_month_leads)
    setting_month_qualified = sum(1 for l in setting_month_leads if l.get("properties", {}).get("hs_pipeline_stage") == "1292501786")

    # ── Closing pipeline snapshot ──
    all_closing_deals = get_all_closing_deals()
    closing_total = len(all_closing_deals)
    closing_won = sum(1 for d in all_closing_deals if d.get("properties", {}).get("hs_is_closed_won") == "true")
    closing_lost = sum(1 for d in all_closing_deals if d.get("properties", {}).get("hs_is_closed_lost") == "true")
    closing_revenue_won = sum(
        float(d.get("properties", {}).get("amount") or 0)
        for d in all_closing_deals
        if d.get("properties", {}).get("hs_is_closed_won") == "true"
    )
    closing_in_progress = closing_total - closing_won - closing_lost

    # Current month closing deals
    closing_month_deals = get_closing_deals_for_period(current_month_start, current_month_end)
    closing_month_total = len(closing_month_deals)
    closing_month_won = sum(1 for d in closing_month_deals if d.get("properties", {}).get("hs_is_closed_won") == "true")
    closing_month_revenue = sum(
        float(d.get("properties", {}).get("amount") or 0)
        for d in closing_month_deals
        if d.get("properties", {}).get("hs_is_closed_won") == "true"
    )

    # Stage distribution for closing
    closing_stage_dist = {}
    for stage_id in CLOSING_STAGES:
        count = sum(1 for d in all_closing_deals if d.get("properties", {}).get("dealstage") == stage_id)
        closing_stage_dist[stage_id] = {
            "name": CLOSING_STAGES[stage_id],
            "count": count,
        }

    summary = {
        "year": year,
        "setting": {
            "pipelineName": "SETTING STUDERIA 2026 - DAILY",
            "total": setting_total,
            "qualified": setting_qualified,
            "disqualified": setting_disqualified,
            "nurturing": setting_nurturing,
            "nouveaux": setting_nouveaux,
            "nrp": setting_nrp,
            "qualificationRate": round((setting_qualified / setting_total * 100) if setting_total > 0 else 0, 2),
            "currentMonth": {
                "total": setting_month_total,
                "qualified": setting_month_qualified,
            },
        },
        "closing": {
            "pipelineName": "CLOSING STUDERIA 2026 - DAILY",
            "total": closing_total,
            "won": closing_won,
            "lost": closing_lost,
            "inProgress": closing_in_progress,
            "revenueWon": closing_revenue_won,
            "winRate": round((closing_won / closing_total * 100) if closing_total > 0 else 0, 2),
            "avgDealSize": round(closing_revenue_won / closing_won, 2) if closing_won > 0 else 0,
            "stageDistribution": closing_stage_dist,
            "currentMonth": {
                "total": closing_month_total,
                "won": closing_month_won,
                "revenue": closing_month_revenue,
            },
        },
        "funnel": {
            "settingLeads": setting_total,
            "qualified": setting_qualified,
            "closingDeals": closing_total,
            "won": closing_won,
            "settingToClosing": round((closing_total / setting_qualified * 100) if setting_qualified > 0 else 0, 2),
            "closingToWon": round((closing_won / closing_total * 100) if closing_total > 0 else 0, 2),
            "overallConversion": round((closing_won / setting_total * 100) if setting_total > 0 else 0, 2),
        },
        "lastUpdated": datetime.now().isoformat(),
    }

    set_cache(cache_key, summary)
    return summary


# ── API Routes ──

@app.route("/api/dashboard")
@cached_response(300)
def api_dashboard():
    """Full dashboard data for all months."""
    year = int(datetime.now().year)
    try:
        data = aggregate_monthly_data(year)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/live")
@cached_response(120)
def api_live():
    """Live summary for current month (fast, cached 2 min)."""
    try:
        data = get_live_summary()
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/deals/<pipeline_id>")
@cached_response(300)
def api_deals(pipeline_id):
    """Get deals for a specific pipeline (current month)."""
    now = datetime.now()
    start_ms, end_ms = get_month_range(now.year, now.month)
    try:
        deals = get_deals_for_period(pipeline_id, start_ms, end_ms)
        return jsonify({
            "success": True,
            "data": [d["properties"] for d in deals],
            "total": len(deals),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/owners")
@cached_response(300)
def api_owners():
    """Get all HubSpot owners."""
    cache_key = "owners"
    cached_data = cached(cache_key, ttl=3600)
    if cached_data:
        return jsonify({"success": True, "data": cached_data})

    url = f"{HUBSPOT_API}/crm/v3/owners"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        owners = resp.json().get("results", [])
        owner_map = {}
        for o in owners:
            owner_map[str(o["id"])] = {
                "name": f"{o.get('firstName', '')} {o.get('lastName', '')}".strip(),
                "email": o.get("email", ""),
            }
        set_cache(cache_key, owner_map)
        return jsonify({"success": True, "data": owner_map})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/setting-daily")
@cached_response(300)
def api_setting_daily():
    """Setting Daily pipeline data (Leads object)."""
    year = int(datetime.now().year)
    try:
        data = aggregate_setting_daily(year)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/closing-daily")
@cached_response(300)
def api_closing_daily():
    """Closing Daily pipeline data (Deals object)."""
    year = int(datetime.now().year)
    try:
        data = aggregate_closing_daily(year)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/pipeline-summary")
@cached_response(300)
def api_pipeline_summary():
    """Combined overview of Setting and Closing daily pipelines."""
    year = int(datetime.now().year)
    try:
        data = get_pipeline_summary(year)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/meetings")
@cached_response(300)
def api_meetings():
    """Meetings for a given month/year."""
    year = int(request.args.get("year", datetime.now().year))
    month = int(request.args.get("month", datetime.now().month))

    cache_key = f"meetings_{year}_{month}"
    cached_data = cached(cache_key)
    if cached_data:
        return jsonify({"success": True, "data": cached_data})

    try:
        start_ms, end_ms = get_month_range(year, month)
        meetings = get_meetings_for_period(start_ms, end_ms)
        data = aggregate_meetings(meetings)
        set_cache(cache_key, data)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/setter-metrics")
@cached_response(300)
def api_setter_metrics():
    """Setter metrics: calls + reaction times for a given year."""
    year = int(request.args.get("year", datetime.now().year))

    cache_key = f"setter_metrics_{year}"
    cached_data = cached(cache_key)
    if cached_data:
        return jsonify({"success": True, "data": cached_data})

    try:
        year_start_ms, year_end_ms = get_year_range(year)
        setter_ids = list(SETTER_OWNER_IDS.keys())

        # Fetch calls
        calls = get_calls_for_period(year_start_ms, year_end_ms, owner_ids=setter_ids)
        call_metrics = aggregate_calls(calls, year)

        # Fetch reaction times
        contacts = get_contacts_with_reaction_time(year_start_ms, year_end_ms, owner_ids=setter_ids)
        reaction_data = aggregate_reaction_times(contacts, year)

        # Build per-setter result
        data = {}
        for oid, name in SETTER_OWNER_IDS.items():
            data[oid] = {
                "name": name,
                "calls": call_metrics.get(oid, {
                    "appelsTotaux": 0, "appelsDecroches": 0,
                    "connectionRate": 0, "avgDuration": 0,
                    "byMonth": [{"total": 0, "connected": 0} for _ in range(12)],
                }),
                "reactionTime": reaction_data.get(oid, {
                    "median_minutes": 0, "avg_minutes": 0, "count": 0,
                    "byMonth": [{"median_minutes": 0, "avg_minutes": 0, "count": 0} for _ in range(12)],
                }),
            }

        set_cache(cache_key, data)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/debug/call-dispositions")
@cached_response(300)
def api_debug_call_dispositions():
    """Debug: list unique hs_call_disposition values from recent calls."""
    try:
        now = datetime.now()
        start_ms, _ = get_month_range(now.year, max(1, now.month - 1))  # last 2 months
        _, end_ms = get_month_range(now.year, now.month)
        calls = get_calls_for_period(start_ms, end_ms)

        dispositions = {}
        for call in calls:
            d = call.get("properties", {}).get("hs_call_disposition", "")
            status = call.get("properties", {}).get("hs_call_status", "")
            if d:
                if d not in dispositions:
                    dispositions[d] = {"count": 0, "statuses": set()}
                dispositions[d]["count"] += 1
                if status:
                    dispositions[d]["statuses"].add(status)

        # Convert sets to lists for JSON
        for d in dispositions:
            dispositions[d]["statuses"] = list(dispositions[d]["statuses"])
            dispositions[d]["isConnected"] = d in CONNECTED_DISPOSITIONS

        return jsonify({"success": True, "data": dispositions, "totalCalls": len(calls)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/cache/clear")
def api_clear_cache():
    """Clear all caches to force fresh data."""
    _cache.clear()
    return jsonify({"success": True, "message": "Cache cleared"})


@app.route("/api/health")
@cached_response(10)
def api_health():
    """Health check."""
    has_token = bool(HUBSPOT_TOKEN)
    return jsonify({
        "status": "ok" if has_token else "missing_token",
        "hasToken": has_token,
        "timestamp": datetime.now().isoformat(),
    })


# ── Vercel serverless entry point ──
# Vercel auto-discovers the `app` Flask instance
