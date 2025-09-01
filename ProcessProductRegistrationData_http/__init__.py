import logging
import os

from datetime import datetime
import requests
import json
from azure.functions import HttpRequest, HttpResponse
import azure.functions as func
from azure.identity import ClientSecretCredential

# Configure logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.basicConfig(level=logging.INFO)

# Read from environment
TENANT_ID = os.getenv("EV__TENANT_ID")
CLIENT_ID = os.getenv("EV__CLIENT_ID")
CLIENT_SECRET = os.getenv("EV__CLIENT_SECRET")
DATAVERSE_URL = os.getenv("EV__DATAVERSE_URL")

# Table names
DATAVERSE_TRANSLATION_FLOW_RUN_STATUS_TABLE = "cococola_tccc_translation_flowstatuses"
DATAVERSE_CASEHISTORY_TABLE = "cococola_tccc_casehistories"
DATAVERSE_LANGUAGE_ORDER_TABLE = "cococola_tccc_languageorders"
DATAVERSE_LANGUAGE_COUNTRY_SELECTION_TABLE = "cococola_tccc_languagecountryselections"
DATAVERSE_PRODUCT_REGISTRATION_TABLE = "cococola_tccc_productregistrations"


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Request received for processing translation data.")
        data = get_json_body(req)
        token = get_token()
        logging.info("Successfully retrieved token.")
        case_entity = parse_general_info(data, token)

        if not case_entity:
            return func.HttpResponse(
                "Failed to create case history record.", status_code=500
            )
        case_id = case_entity.get("cococola_name")
        if not case_id:
            return func.HttpResponse(
                "Case history created but case ID not found.", status_code=500
            )
        logging.info(f"Case history created with ID: {case_id}")

        create_flow_status_record(data, case_id, token)
        parse_language_countries_values(data, case_id, token)

        count = parse_translation_items(data, case_id, token)
        logging.info(f"Processed {count} translation items for {case_id}")
        return func.HttpResponse(
            f"Processed {count} translation items for {case_id}", status_code=200
        )
    except ValueError:
        logging.exception("JSON parsing failed.")
        return func.HttpResponse(
            "Invalid JSON. Ensure Content-Type is 'application/json' and body is properly formatted.",
            status_code=400,
        )
    except Exception:
        logging.exception("Unhandled exception in ProcessProductRegistrationData_http")
        return func.HttpResponse("An internal error occurred.", status_code=500)


def get_json_body(req: HttpRequest) -> dict:
    """Parse and return JSON body, or raise ValueError."""
    try:
        data = req.get_json()
        logging.debug(f"Parsed request body: {json.dumps(data, indent=2)}")
        return data
    except ValueError:
        logging.warning("Request body is not valid JSON or has wrong Content-Type.")
        raise
    except Exception:
        logging.exception("Error while parsing JSON body.")
        raise


def get_token() -> str:
    try:
        credential = ClientSecretCredential(
            tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
        )
        token = credential.get_token(f"{DATAVERSE_URL}/.default")
        return token.token
    except Exception:
        logging.exception("Error obtaining authentication token.")
        raise


def safe_get(value, default=""):
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return value
    except Exception:
        logging.exception("Error in safe_get utility.")
        return default


def styles_to_text(styles) -> str:
    """Serialize styles array to multiline JSON text for Dataverse multiline text column."""
    try:
        if styles is None:
            return ""
        return json.dumps(styles, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("Failed to serialize styles")
        return ""


def parse_general_info(data: dict, token: str) -> dict:
    try:
        payload = {
            "cococola_requestid": safe_get(data.get("requestId")),
            "cococola_assessmentid": safe_get(data.get("assessmentId")),
            "cococola_isproductregistration": "Yes",
            "cococola_casename": "PR - " + safe_get(data.get("requestId"))[-5:],
        }
        logging.info(f"Creating case history with payload: {payload}")
        return store_to_dataverse(DATAVERSE_CASEHISTORY_TABLE, payload, token)
    except Exception:
        logging.exception("Error parsing general case history info.")
        return {}


def parse_language_countries_values(data: dict, case_id: str, token: str):
    try:
        logging.info("Parsing Language & Country Combination")
        language = safe_get(data.get("targetLanguage"))
        country = safe_get(data.get("country"))
        combo = f"{country}$$##{language}"
        order = 1
        if language and country:
            p1 = {
                "cococola_caseid": case_id,
                "cococola_language": language,
                "cococola_country": country,
                "cococola_order": str(order),
            }
            p2 = {
                "cococola_country": country,
                "cococola_country_language": combo,
                "cococola_languageorder": f"lang{order}",
                "cococola_source": "KOMPLY",
                "cococola_destination": language,
                "cococola_caseid": case_id,
            }
            store_to_dataverse(DATAVERSE_LANGUAGE_COUNTRY_SELECTION_TABLE, p1, token)
            store_to_dataverse(DATAVERSE_LANGUAGE_ORDER_TABLE, p2, token)
        else:
            logging.warning("Missing target language or country in the input data")
    except Exception:
        logging.exception("Error parsing language/country values")


def create_flow_status_record(data: dict, case_id: str, token: str):
    try:
        logging.info("Creating Flow Status Record")
        payload = {
            "cococola_caseid": case_id,
            "cococola_numberofflows": 10,
        }
        store_to_dataverse(DATAVERSE_TRANSLATION_FLOW_RUN_STATUS_TABLE, payload, token)
    except Exception:
        logging.exception("Error creating flow status record")


def parse_translation_items(data: dict, case_id: str, token: str) -> int:
    try:
        items = data.get("translationItems", []) or []
        logging.info(f"Found {len(items)} translation items.")
        itemOrder = 1
        for item in items:
            payload = {
                "cococola_caseid": case_id,
                "cococola_productregistrationinfo": safe_get(item.get("label")),
                "cococola_english": safe_get(item.get("value")),
                "cococola_language": safe_get(data.get("sourceLanguage")),
                "cococola_order": str(itemOrder),
                "cococola_flowrunstatus": "init",
                "cococola_id": safe_get(item.get("id")),
                "cococola_group": safe_get(item.get("group")),
                "cococola_label": safe_get(item.get("label")),
                "cococola_key": safe_get(item.get("key")),
                "cococola_value": safe_get(item.get("value")),
                "cococola_sourcestyles": styles_to_text(item.get("styles"))
            }
            store_to_dataverse(DATAVERSE_PRODUCT_REGISTRATION_TABLE, payload, token)
            itemOrder += 1
        return len(items)
    except Exception:
        logging.exception("Error parsing translation items")
        return 0


def store_to_dataverse(table_name: str, payload: dict, token: str) -> dict:
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Accept": "application/json",
        }
        url = f"{DATAVERSE_URL}/api/data/v9.2/{table_name}"
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()

        logging.info(f"Inserted into {table_name}: Status {resp.status_code}")

        if table_name == DATAVERSE_CASEHISTORY_TABLE:
            entity_url = resp.headers.get("OData-EntityId")
            return get_dataverse_record_by_url(entity_url, token)

    except requests.HTTPError as http_err:
        logging.error(f"HTTP error while inserting into {table_name}: {http_err}")
        logging.error(f"Response content: {resp.text}")
    except Exception:
        logging.exception(f"Unexpected error inserting into {table_name}")
    return {}


def get_dataverse_record_by_url(entity_url: str, token: str) -> dict:
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }
        resp = requests.get(entity_url, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as http_err:
        logging.error(f"HTTP error fetching record: {http_err}")
        logging.error(f"Response content: {resp.text}")
    except Exception:
        logging.exception("Error fetching record by URL")
    return {}
