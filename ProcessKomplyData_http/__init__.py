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
DATAVERSE_CASEHISTORY_TABLE = "cococola_tccc_casehistorie"
DATAVERSE_LEGAL_TABLE_NAME = "cococola_tccc_transaltion_legalname"
DATAVERSE_TRANSLATION_OTHERINFO_TABLE = "cococola_tccc_otherinfo"
DATAVERSE_TRANSLATION_NUTRIINFO_TABLE = "cococola_tccc_nutritioninformation"
DATAVERSE_TRANSLATION_NUTRITIONVALUES_TABLE = "cococola_tccc_nutritionvaluese"
DATAVERSE_TRANSLATION_FRONTOFPACK_TABLE = "cococola_tccc_frontofpacklabeling"
DATAVERSE_TRANSLATION_LANGUAGEORDER_TABLE = "cococola_tccc_languageorder"
DATAVERSE_TRANSLATION_LANGUAGECOUNTRY_SELECTION_TABLE = (
    "cococola_tccc_languagecountryselection"
)
DATAVERSE_TRANSLATION_FLOW_RUN_STATUS_TABLE = "cococola_tccc_translation_flowstatuse"


def get_dataverse_token() -> str:
    """Fetch a fresh token once per function invocation."""
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )
    token = credential.get_token(DATAVERSE_URL + "/.default")
    return token.token


def safe_get(value, default="N/A"):
    """Return a default if the value is None or empty."""
    if value is None:
        return default
    if isinstance(value, str) and not value.strip():
        return default
    return value


def get_json_body(req: HttpRequest) -> dict:
    """Parse and return JSON body, or raise ValueError."""
    try:
        return req.get_json()
    except ValueError:
        logging.warning("Request body is not valid JSON or has wrong Content-Type.")
        raise


def styles_to_text(styles) -> str:
    """Serialize styles array to multiline JSON text for Dataverse multiline text column."""
    try:
        if styles is None:
            return ""
        return json.dumps(styles, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("Failed to serialize styles")
        return ""


def main(req: HttpRequest) -> HttpResponse:
    logging.info("Function triggered: ProcessKomplyData_http")
    try:
        data = get_json_body(req)
        token = get_dataverse_token()

        # 1) Insert case history and retrieve its cococola_name/caseid
        case_entity = parse_general_info(data, token)
        case_id = case_entity["cococola_name"]
        logging.info(f"Case created : {case_id}")

        # 2) Process all other sections
        create_flow_status_record(data, case_id, token)
        parse_language_countries_values(data, case_id, token)
        parse_legal_ingredients(data, case_id, token)
        parse_other_info(data, case_id, token)
        parse_nutrition_info(data, case_id, token)
        parse_nutrition_values(data, case_id, token)
        parse_front_of_pack(data, case_id, token)

        logging.info(f"All data sections processed successfully for {case_id}")
        return func.HttpResponse(
            f"Data processed successfully for {case_id}", status_code=200
        )

    except ValueError:
        return func.HttpResponse(
            "Invalid JSON. Ensure Content-Type is 'application/json' and body is properly formatted.",
            status_code=400,
        )
    except Exception:
        logging.exception("Unhandled exception in ProcessKomplyData_http")
        return func.HttpResponse("An internal error occurred.", status_code=500)


def parse_general_info(data: dict, token: str) -> dict:
    logging.info("Parsing General Information")
    now_str = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    payload = {
        "cococola_requestid": safe_get(data.get("requestId")),
        "cococola_assessmentid": safe_get(data.get("assessmentId")),
        "cococola_labelpacksizeid": safe_get(data.get("labelPackSizeId")),
        "cococola_labelpacksizetype": safe_get(data.get("labelPackSizeType")),
        "cococola_labelidentifier": safe_get(data.get("labelIdentifier")),
        "cococola_regulation": safe_get(data.get("regulation")),
        "cococola_status": safe_get(data.get("status")),
        "cococola_lastediteddate": safe_get(data.get("lastEditedDate")),
        "cococola_lastpublisheddate": safe_get(data.get("lastPublishedDate")),
        "cococola_sraassociatename": safe_get(data.get("sraAssociateName")),
        "cococola_sraassociateemail": safe_get(data.get("sraAssociateEmail")),
        "cococola_labelcode": safe_get(data.get("labelCode")),
        "cococola_bbn": safe_get(data.get("bbn")),
        "cococola_notes": safe_get(data.get("notes")),
        "cococola_countryofsalemarket": safe_get(data.get("countryOfSaleMarket")),
        "cococola_productname": safe_get(data.get("productName")),
        "cococola_regioncluster": safe_get(data.get("regionCluster")),
        "cococola_numberforbottler": safe_get(data.get("numberForBottler")),
        "cococola_producttype": safe_get(data.get("productType")),
        "cococola_productcategory": safe_get(data.get("productCategory")),
        "cococola_registrationinfonumber": safe_get(data.get("registrationInfoNumber")),
        "cococola_enduse": safe_get(data.get("endUse")),
        "cococola_brand": safe_get(data.get("brand")),
        "cococola_processtype": safe_get(data.get("processType")),
        "cococola_operatingunit": safe_get(data.get("operatingUnit")),
        "cococola_casetype": "Translation",
        "cococola_casename": safe_get(data.get("regulation")),
        "cococola_casestatus": "Translation in progress",
        "cococola_uniquelabelidentifier": safe_get(data.get("labelIdentifier")),
        "cococola_caseownername": safe_get(data.get("sraAssociateName")),
        "cococola_caseowneremailid": safe_get(data.get("sraAssociateEmail")),
        "cococola_timestamp": now_str,
    }
    return store_to_dataverse(DATAVERSE_CASEHISTORY_TABLE, payload, token)


def create_flow_status_record(data: dict, case_id: str, token: str):
    logging.info("Creating Flow Status Record")
    payload = {
        "cococola_caseid": case_id,
        "cococola_numberofflows": 5,
    }
    mapping = {
        "Legal Name & Ingredient Information": "cococola_legalnameflowstatus",
        "Other Information": "cococola_otherinfoflowstatus",
        "Nutrition Information": "cococola_nutritioninfoflowstatus",
        "Nutrition Values": "cococola_nutritionvalueflowstatus",
        "Front of Pack Labeling": "cococola_foplabelingsflowstatus",
    }
    for section, col in mapping.items():
        if isinstance(data.get(section), list) and not data.get(section):
            payload[col] = "Success"
    store_to_dataverse(DATAVERSE_TRANSLATION_FLOW_RUN_STATUS_TABLE, payload, token)


def parse_language_countries_values(data: dict, case_id: str, token: str):
    logging.info("Parsing Language & Country Combinations")
    languages = data.get("targetLanguages") or []
    country = safe_get(data.get("targetCountry"))
    order = 1

    for lang in languages:
        p1 = {
            "cococola_caseid": case_id,
            "cococola_language": lang,
            "cococola_country": country,
            "cococola_order": str(order),
        }
        combo = f"{country}$$##{lang}"
        p2 = {
            "cococola_country": country,
            "cococola_country_language": combo,
            "cococola_languageorder": f"lang{order}",
            "cococola_source": "KOMPLY",
            "cococola_destination": lang,
            "cococola_caseid": case_id,
        }
        store_to_dataverse(
            DATAVERSE_TRANSLATION_LANGUAGECOUNTRY_SELECTION_TABLE, p1, token
        )
        store_to_dataverse(DATAVERSE_TRANSLATION_LANGUAGEORDER_TABLE, p2, token)
        order += 1


def parse_legal_ingredients(data: dict, case_id: str, token: str):
    logging.info("Parsing Legal Name & Ingredient Information")
    itemOrder = 1
    for item in data.get("Legal Name & Ingredient Information", []):
        payload = {
            "cococola_caseid": case_id,
            "cococola_itemid": safe_get(item.get("id")),
            "cococola_label": safe_get(item.get("label")),
            "cococola_key": safe_get(item.get("key")),
            "cococola_value": safe_get(item.get("value")),
            "cococola_legalname_ingredientinformation": safe_get(item.get("label")),
            "cococola_english": safe_get(item.get("value")),
            "cococola_language": safe_get(data.get("sourceLanguage")),
            "cococola_packsize": safe_get(data.get("labelPackSizeType")),
            "cococola_flowrunstatus": "init",
            "cococola_order": itemOrder,
            "cococola_sourcestyles": styles_to_text(item.get("styles"))
        }
        store_to_dataverse(DATAVERSE_LEGAL_TABLE_NAME, payload, token)
        itemOrder += 1


def parse_other_info(data: dict, case_id: str, token: str):
    logging.info("Parsing Other Information")
    itemOrder = 1
    for item in data.get("Other Information", []):
        payload = {
            "cococola_caseid": case_id,
            "cococola_itemid": safe_get(item.get("id")),
            "cococola_label": safe_get(item.get("label")),
            "cococola_key": safe_get(item.get("key")),
            "cococola_value": safe_get(item.get("value")),
            "cococola_otherinfo": safe_get(item.get("label")),
            "cococola_english": safe_get(item.get("value")),
            "cococola_language": safe_get(data.get("sourceLanguage")),
            "cococola_packsize": safe_get(data.get("labelPackSizeType")),
            "cococola_flowrunstatus": "init",
            "cococola_order": itemOrder,
            "cococola_sourcestyles": styles_to_text(item.get("styles"))
        }
        store_to_dataverse(DATAVERSE_TRANSLATION_OTHERINFO_TABLE, payload, token)
        itemOrder += 1


def parse_nutrition_info(data: dict, case_id: str, token: str):
    logging.info("Parsing Nutrition Information")
    itemOrder = 1
    for item in data.get("Nutrition Information", []):
        payload = {
            "cococola_caseid": case_id,
            "cococola_itemid": safe_get(item.get("id")),
            "cococola_label": safe_get(item.get("label")),
            "cococola_key": safe_get(item.get("key")),
            "cococola_value": safe_get(item.get("value")),
            "cococola_nutritioninfo": safe_get(item.get("label")),
            "cococola_english": safe_get(item.get("value")),
            "cococola_language": safe_get(data.get("sourceLanguage")),
            "cococola_packsize": safe_get(data.get("labelPackSizeType")),
            "cococola_flowrunstatus": "init",
            "cococola_order": itemOrder,
            "cococola_sourcestyles": styles_to_text(item.get("styles"))
        }
        store_to_dataverse(DATAVERSE_TRANSLATION_NUTRIINFO_TABLE, payload, token)
        itemOrder += 1


def parse_nutrition_values(data: dict, case_id: str, token: str):
    logging.info("Parsing Nutrition Values")
    itemOrder = 1
    for nut in data.get("Nutrition Values", []):
        payload = {
            "cococola_caseid": case_id,
            "cococola_nutrientcomposition": safe_get(nut.get("nutrientComposition")),
            "cococola_english": safe_get(nut.get("english")),
            "cococola_nutrientid": safe_get(nut.get("picassoNutrientId")),
            "cococola_unit": safe_get(nut.get("unit")),
            "cococola_nutritioncomposition": safe_get(nut.get("nutrientComposition")),
            "cococola_language": safe_get(data.get("sourceLanguage")),
            "cococola_packsize": safe_get(data.get("labelPackSizeType")),
            "cococola_flowrunstatus": "init",
            "cococola_order": itemOrder,
        }
        store_to_dataverse(DATAVERSE_TRANSLATION_NUTRITIONVALUES_TABLE, payload, token)
        itemOrder += 1


def parse_front_of_pack(data: dict, case_id: str, token: str):
    logging.info("Parsing Front of Pack Labeling")
    itemOrder = 1
    for item in data.get("Front of Pack Labeling", []):
        payload = {
            "cococola_caseid": case_id,
            "cococola_itemid": safe_get(item.get("id")),
            "cococola_label": safe_get(item.get("label")),
            "cococola_key": safe_get(item.get("key")),
            "cococola_value": safe_get(item.get("value")),
            "cococola_frontofpacklabeling": safe_get(item.get("label")),
            "cococola_english": safe_get(item.get("value")),
            "cococola_language": safe_get(data.get("sourceLanguage")),
            "cococola_packsize": safe_get(data.get("labelPackSizeType")),
            "cococola_flowrunstatus": "init",
            "cococola_order": itemOrder,
            "cococola_sourcestyles": styles_to_text(item.get("styles"))
        }
        store_to_dataverse(DATAVERSE_TRANSLATION_FRONTOFPACK_TABLE, payload, token)
        itemOrder += 1


def store_to_dataverse(table_name: str, payload: dict, token: str) -> dict:
    """Insert payload into Dataverse and return new entity for CaseHistory."""
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Accept": "application/json",
        }
        resp = requests.post(
            f"{DATAVERSE_URL}/api/data/v9.2/{table_name}s",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        logging.info(f"Inserted into {table_name}: {resp.status_code}")

        if table_name == DATAVERSE_CASEHISTORY_TABLE:
            entity_url = resp.headers.get("OData-EntityId")
            return get_dataverse_record_by_url(entity_url, token)
    except Exception:
        logging.exception(f"Error inserting into {table_name}")
    return {}


def get_dataverse_record_by_url(entity_url: str, token: str) -> dict:
    """Fetch a single record by URL."""
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
    except Exception:
        logging.exception("Error fetching record by URL")
    return {}
