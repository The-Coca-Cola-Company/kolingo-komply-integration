import logging
import json
import requests
import os
from datetime import datetime, timedelta
import azure.functions as func
from azure.identity import ClientSecretCredential
from azure.functions import HttpRequest, HttpResponse

# Suppress verbose Azure HTTP logs
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Read from environment
TENANT_ID     = os.getenv("EV__TENANT_ID")
CLIENT_ID     = os.getenv("EV__CLIENT_ID")
CLIENT_SECRET = os.getenv("EV__CLIENT_SECRET")
DATAVERSE_URL = os.getenv("EV__DATAVERSE_URL")

# Tables & keys
SPECIAL_KEYS            = ["IngredientList", "ShortIngredientList", "LegalName"]
WORD_LEVEL_TABLE        = "cococola_tccc_wordleveltranslation"
CASE_HISTORY_TABLE      = "cococola_tccc_casehistorie"
LANGUAGE_COUNTRY_TABLE  = "cococola_tccc_languagecountryselection"
LEGAL_TABLE             = "cococola_tccc_transaltion_legalname"
OTHER_INFO_TABLE        = "cococola_tccc_otherinfo"
NUTRITION_INFO_TABLE    = "cococola_tccc_nutritioninformation"
NUTRITION_VALUES_TABLE  = "cococola_tccc_nutritionvaluese"
FRONT_OF_PACK_TABLE     = "cococola_tccc_frontofpacklabeling"

_cached_token = None


def get_dataverse_token():
    """Fetch a fresh token if none cached or within 60s of expiry, else return cached."""
    global _cached_token, _token_expires_at

    now = datetime.utcnow()
    if not _cached_token or not _token_expires_at or now + timedelta(seconds=60) >= _token_expires_at:
        logging.info("Fetching new token for Dataverse...")
        credential = ClientSecretCredential(
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        token = credential.get_token(DATAVERSE_URL + "/.default")
        _cached_token = token.token
        # expires_on is epoch seconds (UTC)
        _token_expires_at = datetime.utcfromtimestamp(token.expires_on)

    return _cached_token
# def get_dataverse_token():
#     global _cached_token
#     if _cached_token:
#         return _cached_token

#     logging.info("Requesting new Dataverse token…")
#     cred = ClientSecretCredential(
#         tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
#     )
#     token = cred.get_token(f"{DATAVERSE_URL}/.default")
#     _cached_token = token.token
#     logging.info("Dataverse token acquired.")
#     return _cached_token

def fetch_records(table_name, filter_clause=None, select_clause=None, orderby=None):
    token = get_dataverse_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"{DATAVERSE_URL}/api/data/v9.2/{table_name}s"
    params = {}
    if filter_clause: params['$filter']  = filter_clause
    if select_clause: params['$select']  = select_clause
    if orderby:       params['$orderby'] = orderby

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    items = resp.json().get('value', [])
    logging.info(f"Fetched {len(items)} records from {table_name}.")
    return items

def build_response(case_id):
    logging.info(f"Building response for case {case_id}…")
    response = {
        "requestId": None,
        "assessmentId": None,
        "targetLanguages": [],
        "Legal Name & Ingredient Information": [],
        "Other Information": [],
        "Nutrition Information": [],
        "Front of Pack Labeling": [],
        "Nutrition Values": []
    }

    # 1. Top-level metadata
    case = fetch_records(CASE_HISTORY_TABLE,
                         filter_clause=f"cococola_name eq '{case_id}'")[0]
    response["requestId"]    = case.get('cococola_requestid')
    response["assessmentId"] = case.get('cococola_assessmentid')

    # 2. Target languages
    langs = fetch_records(
        LANGUAGE_COUNTRY_TABLE,
        filter_clause=f"cococola_caseid eq '{case_id}'",
        select_clause="cococola_language",
        orderby="cococola_order asc"
    )
    target_languages = [r['cococola_language'] for r in langs]
    response['targetLanguages'] = target_languages
    logging.info(f"Target languages: {target_languages}")

    def make_translations(rec, prefix, src_prefix):
        out = []
        for i, lang in enumerate(target_languages, start=1):
            key = f"{prefix}{i}"
            src = src_prefix if i == 1 else f"{src_prefix}{i}"
            out.append({
                "language": lang,
                "values": [{
                    "value": rec.get(key),
                    "source": rec.get(src)
                }]
            })
        return out

    # 3. Sections
    sections = [
        ("Legal Name & Ingredient Information", LEGAL_TABLE),
        ("Other Information", OTHER_INFO_TABLE),
        ("Nutrition Information", NUTRITION_INFO_TABLE),
        ("Front of Pack Labeling", FRONT_OF_PACK_TABLE)
    ]
    base_fields = (
        ["cococola_itemid","cococola_label","cococola_key","cococola_value"] +
        [f"cococola_lang{i}" for i in range(1,7)] +
        ["cococola_translationsource"] +
        [f"cococola_translationsource{i}" for i in range(2,7)]
    )

    for section_name, table in sections:
        rows = fetch_records(table,
                             filter_clause=f"cococola_caseid eq '{case_id}'",
                             select_clause=','.join(base_fields))
        items = []
        for rec in rows:
            key = rec['cococola_key']
            if key in SPECIAL_KEYS:
                special = []
                for i, lang in enumerate(target_languages, start=1):
                    wl = fetch_records(
                        WORD_LEVEL_TABLE,
                        filter_clause=(
                            f"cococola_caseid eq '{case_id}' and "
                            f"cococola_key eq '{key}' and "
                            f"cococola_language eq '{lang}'"
                        ),
                        select_clause="cococola_order,cococola_source_word,cococola_translated_word,cococola_translatedby",
                        orderby="cococola_order asc"
                    )
                    full_trans = rec.get(f"cococola_lang{i}")
                    if wl:
                        vals = [
                            {
                                "english": w['cococola_source_word'],
                                "value":   w['cococola_translated_word'],
                                "source":  w['cococola_translatedby']
                            }
                            for w in wl
                        ]
                    else:
                        vals = [{
                            "english": rec['cococola_value'],
                            "value":   full_trans,
                            "source":  rec['cococola_translationsource'] 
                                      if i==1 
                                      else rec[f"cococola_translationsource{i}"]
                        }]
                    special.append({
                        "language":    lang,
                        "translation": full_trans,
                        "values":      vals
                    })
                item = {
                    "id":           rec['cococola_itemid'],
                    "label":        rec['cococola_label'],
                    "key":          key,
                    "value":        rec['cococola_value'],
                    "translations": special
                }
            else:
                item = {
                    "id":           rec['cococola_itemid'],
                    "label":        rec['cococola_label'],
                    "key":          key,
                    "value":        rec['cococola_value'],
                    "translations": make_translations(
                        rec, 'cococola_lang', 'cococola_translationsource'
                    )
                }
            items.append(item)
        response[section_name] = items

    # 4. Nutrition Values
    nv_fields = (
        ["cococola_nutrientcomposition","cococola_english","cococola_nutrientid","cococola_unit"] +
        [f"cococola_lang{i}" for i in range(1,7)] +
        ["cococola_translationsource"] +
        [f"cococola_translationsource{i}" for i in range(2,7)] +
        [f"cococola_unittranslationlang{i}" for i in range(1,7)] +
        [f"cococola_translationsourceunitlang{i}" for i in range(1,7)]
    )
    nv_rows = fetch_records(NUTRITION_VALUES_TABLE,
                            filter_clause=f"cococola_caseid eq '{case_id}'",
                            select_clause=','.join(nv_fields))
    nv_items = []
    for rec in nv_rows:
        units_trans = [{
            "language": lang,
            "values": [{
                "value":  rec[f"cococola_unittranslationlang{i}"],
                "source": rec[f"cococola_translationsourceunitlang{i}"]
            }]
        } for i, lang in enumerate(response['targetLanguages'], start=1)]

        nv_items.append({
            "nutrientComposition":         rec['cococola_nutrientcomposition'],
            "english":                     rec['cococola_english'],
            "picassoNutrientId":           rec['cococola_nutrientid'],
            "unit":                        rec['cococola_unit'],
            "nutrientCompositionTranslations": make_translations(
                                                 rec,
                                                 'cococola_lang',
                                                 'cococola_translationsource'
                                               ),
            "unitsTranslations": units_trans
        })
    response['Nutrition Values'] = nv_items

    logging.info("Response build complete.")
    return response

def main(req: HttpRequest) -> HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body.", status_code=400)

    case_id = body.get("caseId")
    if not case_id:
        return func.HttpResponse("Please include 'caseId' in the request body.", status_code=400)

    try:
        result = build_response(case_id)
        return func.HttpResponse(
            json.dumps(result, indent=2),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.exception("Failed to build response")
        return func.HttpResponse(f"Error: {e}", status_code=500)
