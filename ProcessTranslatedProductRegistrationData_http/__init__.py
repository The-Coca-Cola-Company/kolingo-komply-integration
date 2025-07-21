import logging
import json
import requests
import os

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

# Tables
WORD_LEVEL_TABLE = "cococola_tccc_wordleveltranslation"
CASE_HISTORY_TABLE = "cococola_tccc_casehistorie"
LANGUAGE_COUNTRY_TABLE = "cococola_tccc_languagecountryselection"
PRODUCT_REGISTRATION_TABLE = "cococola_tccc_productregistration"

# Special keys requiring word-level handling
SPECIAL_KEYS = ["IngredientList", "ShortIngredientList", "LegalName"]


def generate_dataverse_token():
    """Generate a fresh Dataverse access token (no caching)."""
    try:
        logging.info("Generating new Dataverse token...")
        cred = ClientSecretCredential(
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        token = cred.get_token(f"{DATAVERSE_URL}/.default")
        return token.token
    except Exception:
        logging.exception("Error generating Dataverse token")
        raise


def fetch_records(token, table_name, filter_clause=None, select_clause=None, orderby=None):
    """Fetch records from a Dataverse table using the provided token."""
    try:
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        url = f"{DATAVERSE_URL}/api/data/v9.2/{table_name}s"
        params = {}
        if filter_clause:
            params["$filter"] = filter_clause
        if select_clause:
            params["$select"] = select_clause
        if orderby:
            params["$orderby"] = orderby

        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        items = resp.json().get("value", [])
        logging.info(f"Fetched {len(items)} records from {table_name}.")
        return items
    except Exception:
        logging.exception(f"Error fetching records from {table_name}")
        raise


def build_response(token, case_id):
    """Construct the ProductRegistrationResponse JSON by querying Dataverse."""
    try:
        logging.info(f"Building ProductRegistrationResponse for case {case_id}…")
        response = {
            "requestId": None,
            "assessmentId": None,
            "sourceLanguage": "",
            "targetLanguage": "",
            "country": "",
            "translationItems": []
        }

        # 1. Top-level metadata
        cases = fetch_records(token, CASE_HISTORY_TABLE, filter_clause=f"cococola_name eq '{case_id}'")
        if not cases:
            raise ValueError(f"No case found for caseId {case_id}")
        case = cases[0]
        response["requestId"] = case.get("cococola_requestid")
        response["assessmentId"] = case.get("cococola_assessmentid")

        # 2. Target language & country
        lang_recs = fetch_records(
            token,
            LANGUAGE_COUNTRY_TABLE,
            filter_clause=f"cococola_caseid eq '{case_id}'",
            select_clause="cococola_language,cococola_country",
            orderby="cococola_order asc"
        )
        if lang_recs:
            rc = lang_recs[0]
            response["targetLanguage"] = rc.get("cococola_language", "")
            response["country"] = rc.get("cococola_country", "")

        # 3. Translation items
        select_fields = [
            "cococola_id", "cococola_group", "cococola_label", "cococola_key",
            "cococola_value", "cococola_language", "cococola_translatedtext", "cococola_translationsource"
        ]
        rows = fetch_records(
            token,
            PRODUCT_REGISTRATION_TABLE,
            filter_clause=f"cococola_caseid eq '{case_id}'",
            select_clause=",".join(select_fields),
            orderby="cococola_order asc"
        )
        if rows:
            response["sourceLanguage"] = rows[0].get("cococola_language", "")

        items = []
        for rec in rows:
            key = rec.get("cococola_key")
            base = {
                "id": rec.get("cococola_id"),
                "group": rec.get("cococola_group"),
                "label": rec.get("cococola_label"),
                "key": key,
                "value": rec.get("cococola_value")
            }

            if key in SPECIAL_KEYS:
                # Word-level translations
                wl = fetch_records(
                    token,
                    WORD_LEVEL_TABLE,
                    filter_clause=(
                        f"cococola_caseid eq '{case_id}' and "
                        f"cococola_key eq '{key}' and "
                        f"cococola_language eq '{response['targetLanguage']}'"
                    ),
                    select_clause="cococola_order,cococola_source_word,cococola_translated_word,cococola_translatedby",
                    orderby="cococola_order asc"
                )
                full_trans = rec.get("cococola_translatedtext")
                if wl:
                    vals = [
                        {
                            "english": w.get("cococola_source_word"),
                            "value": w.get("cococola_translated_word"),
                            "source": w.get("cococola_translatedby")
                        }
                        for w in wl
                    ]
                else:
                    vals = [{
                        "english": rec.get("cococola_value"),
                        "value": full_trans,
                        "source": rec.get("cococola_translationsource")
                    }]

                base["translations"] = {"translation": full_trans, "values": vals}

            else:
                # Simple translations
                base["translations"] = {
                    "value": rec.get("cococola_translatedtext"),
                    "source": rec.get("cococola_translationsource")
                }

            items.append(base)

        response["translationItems"] = items
        logging.info("ProductRegistrationResponse build complete.")
        return response

    except Exception:
        logging.exception(f"Error in build_response for case {case_id}")
        raise


def main(req: HttpRequest) -> HttpResponse:
    try:
        logging.info("HTTP trigger received a ProductRegistrationResponse request.")
        try:
            body = req.get_json()
        except ValueError:
            return HttpResponse("Invalid JSON body.", status_code=400)

        case_id = body.get("caseId")
        if not case_id:
            return HttpResponse("Please include 'caseId' in the request body.", status_code=400)

        try:
            token = generate_dataverse_token()
            result = build_response(token, case_id)
            return HttpResponse(json.dumps(result, indent=2), mimetype="application/json", status_code=200)
        except Exception as e:
            logging.exception("Failed to build ProductRegistrationResponse")
            return HttpResponse(f"Error: {e}", status_code=500)

    except Exception as e:
        logging.exception("Unhandled exception in main")
        return HttpResponse(f"Unhandled error: {e}", status_code=500)
 