#!/usr/bin/env python3

import re
import os
import json
import logging
import requests
import azure.functions as func
from azure.identity import ClientSecretCredential

# Setup logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Azure Dataverse config
TENANT_ID = os.getenv("EV__TENANT_ID")
CLIENT_ID = os.getenv("EV__CLIENT_ID")
CLIENT_SECRET = os.getenv("EV__CLIENT_SECRET")
DATAVERSE_URL = os.getenv("EV__DATAVERSE_URL")


def generate_dataverse_token():
    """Generate access token for Dataverse (no caching)."""
    try:
        cred = ClientSecretCredential(
            tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
        )
        token = cred.get_token(f"{DATAVERSE_URL}/.default")
        logging.debug("Dataverse token generated.")
        return token.token
    except Exception as exc:
        logging.exception("Failed to generate Dataverse token.")
        raise RuntimeError("Authentication error") from exc


def fetch_records(
    token, table_name, filter_clause=None, select_clause=None, orderby=None
):
    """Fetch records from a Dataverse table."""
    url = f"{DATAVERSE_URL}/api/data/v9.2/{table_name}s"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {}
    if filter_clause:
        params["$filter"] = filter_clause
    if select_clause:
        params["$select"] = select_clause
    if orderby:
        params["$orderby"] = orderby
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        records = resp.json().get("value", [])
        logging.debug("Fetched %d records from %s.", len(records), table_name)
        return records
    except requests.RequestException as exc:
        logging.exception("HTTP error fetching %s: %s", table_name, exc)
        raise RuntimeError("Data fetch error") from exc


def get_delimiters_for_language(language):
    """Retrieve delimiter array for a given language or default to comma."""
    if not language:
        logging.warning("Language not provided, defaulting to ','.")
        return [","]
    try:
        filter_q = f"cococola_languagename eq '{language}'"
        select_q = "cococola_delimiterarray"
        token = generate_dataverse_token()
        records = fetch_records(
            token,
            "cococola_tccc_languagedelimitermapping",
            filter_clause=filter_q,
            select_clause=select_q,
        )
        if not records or "cococola_delimiterarray" not in records[0]:
            logging.warning("No delimiters found for language '%s'", language)
            return [","]
        arr = records[0]["cococola_delimiterarray"]
        if isinstance(arr, str):
            try:
                arr = json.loads(arr)
            except json.JSONDecodeError:
                logging.error("Invalid JSON delimiter array for '%s'", language)
                return [","]
        return arr if isinstance(arr, list) else [","]
    except Exception:
        logging.exception("Error retrieving delimiters for '%s'", language)
        return [","]


# def build_split_pattern(delimiters):
#     """Build a regex pattern to split on any of the provided delimiters."""
#     escaped = sorted((re.escape(d) for d in delimiters), key=len, reverse=True)
#     pattern_str = "|".join(escaped)
#     try:
#         return re.compile(pattern_str)
#     except re.error:
#         logging.exception("Invalid regex pattern: %s", pattern_str)
#         return re.compile(r",")

# def build_split_pattern(delimiters):
#     escaped = []
#     for d in delimiters:
#         if d == ",":
#             # match comma only if not between digits
#             escaped.append(r"(?<!\d),(?!\d)")
#         else:
#             escaped.append(re.escape(d))
#     pattern_str = "|".join(sorted(escaped, key=len, reverse=True))
#     return re.compile(pattern_str)

# def build_split_pattern(delimiters): 
#     escaped = []
#     for d in delimiters:
#         if d == ",":
#             # match comma only if not between digits
#             escaped.append(r"(?<!\d),(?!\d)")
#         elif d == ".":
#             # match period only if not between digits
#             escaped.append(r"(?<!\d)\.(?!\d)")
#         else:
#             escaped.append(re.escape(d))
#     pattern_str = "|".join(sorted(escaped, key=len, reverse=True))
#     return re.compile(pattern_str)

def build_split_pattern(delimiters):
    escaped = []
    for d in delimiters:
        if d == ",":
            # match comma unless both sides are digits
            escaped.append(r"(?<!\d),(?!\d)")
        elif d == ".":
            # match dot unless both sides are digits
            escaped.append(r"(?<!\d)\.(?!\d)")
        else:
            escaped.append(re.escape(d))
    pattern_str = "|".join(sorted(escaped, key=len, reverse=True))
    return re.compile(pattern_str)

def split_preserve_numbers(text, delimiters):
    """Split text on given delimiters, but preserve commas/periods between digits."""
    # Build a regex to match all delimiters
    pattern = "|".join(re.escape(d) for d in delimiters)
    result = []
    start = 0
    for m in re.finditer(pattern, text):
        sep = m.group()
        idx = m.start()

        # Preserve comma or period if it is between digits
        if sep in {",", "."}:
            if idx > 0 and idx < len(text) - 1:
                if text[idx - 1].isdigit() and text[idx + 1].isdigit():
                    continue  # skip this delimiter

        # Split here
        result.append(text[start:idx].strip())
        start = idx + 1

    result.append(text[start:].strip())
    return [r for r in result if r]


def split_ingredient_units(text, regional_delims):
    """Split ingredient list text using regional + static punctuation delimiters."""
    static = {",", ":", ";", "(", ")", "[", "]"}
    delimiters = regional_delims | static
    return split_preserve_numbers(text, delimiters)


def split_legalname_units(text, regional_delims):
    """Split legal name text using only period and comma (ignore numbers inside)."""
    static = {".", ","}
    return split_preserve_numbers(text, static)


# def split_ingredient_units(text, regional_delims):
#     """Split ingredient list text using regional + static punctuation delimiters."""
#     static = {",", ":", ";", "(", ")", "[", "]"}
#     delimiters = regional_delims | static
#     regex = build_split_pattern(delimiters)
#     return [seg.strip() for seg in regex.split(text) if seg.strip()]


# def split_legalname_units(text, regional_delims):
#     """Split legal name text using regional + space, period, comma; ignore hyphens."""
#     #static = {" ", ".", ","}
#     static = {".", ","}
#     #delimiters = (regional_delims | static) - {"-"}
#     regex = build_split_pattern(static)
#     return [seg.strip() for seg in regex.split(text) if seg.strip()]


def split_units(text, language, field_name):
    """Dispatch splitting logic based on the field_name."""
    if not text or not field_name:
        logging.error(
            "Text or field_name missing: text='%s', field_name='%s'", text, field_name
        )
        return []
    regional = set(get_delimiters_for_language(language))
    if field_name in ("IngredientList", "ShortIngredientList"):
        return split_ingredient_units(text, regional)
    if field_name == "LegalName":
        return split_legalname_units(text, regional)
    logging.warning("Unsupported field_name '%s'", field_name)
    return []


def split_and_merge_texts_for_languages(text1, lang1, text2, lang2, field_name):
    """Split two texts and merge them into paired segments."""
    parts1 = split_units(text1, lang1, field_name)
    parts2 = split_units(text2, lang2, field_name)
    if len(parts1) != len(parts2):
        logging.warning(
            "Length mismatch for '%s': %d vs %d", field_name, len(parts1), len(parts2)
        )
    return [f"{a}&&**{b}" for a, b in zip(parts1, parts2)]


def translate_string(main_str, source_arr, target_arr):
    """Perform precise word-boundary replacements from source to target."""
    if not main_str:
        logging.warning("Empty main_str provided to translate_string.")
        return ""
    pairs = sorted(zip(source_arr, target_arr), key=lambda x: len(x[0]), reverse=True)
    result = main_str
    for src, tgt in pairs:
        # result = re.sub(rf"{re.escape(src)}", tgt, result)
        pattern = rf"(?<!\w){re.escape(src)}(?!\w)"
        result = re.sub(pattern, tgt, result)
    return result


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Functions entry point."""
    logging.info("Received HTTP request for delimiter processing.")
    try:
        payload = req.get_json()
        logging.debug("Request payload: %s", payload)
    except ValueError:
        logging.error("Invalid JSON payload.")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON body."}),
            status_code=400,
            mimetype="application/json",
        )

    process = payload.get("process_type")
    field_name = payload.get("fieldName")

    try:
        if process == "TranslationStep1":
            text = payload.get("text")
            language = payload.get("language")
            if not text or not language or not field_name:
                logging.error("Missing params for TranslationStep1.")
                return func.HttpResponse(
                    json.dumps({"error": "Missing text, language, or fieldName."}),
                    status_code=400,
                    mimetype="application/json",
                )
            segments = split_units(text, language, field_name)
            result = {"segments": segments}

        elif process == "TranslationStep2":
            main_str = payload.get("main_string")
            src = payload.get("source_array")
            tgt = payload.get("target_array")
            if not main_str or not isinstance(src, list) or not isinstance(tgt, list):
                logging.error("Missing params for TranslationStep2.")
                return func.HttpResponse(
                    json.dumps({"error": "Missing main_string or arrays."}),
                    status_code=400,
                    mimetype="application/json",
                )
            result = {"translated": translate_string(main_str, src, tgt)}

        elif process == "Catalog":
            t1 = payload.get("text1")
            l1 = payload.get("lang1")
            t2 = payload.get("text2")
            l2 = payload.get("lang2")
            if not all([t1, l1, t2, l2, field_name]):
                logging.error("Missing params for Catalog.")
                return func.HttpResponse(
                    json.dumps({"error": "Missing catalog parameters."}),
                    status_code=400,
                    mimetype="application/json",
                )
            merged = split_and_merge_texts_for_languages(t1, l1, t2, l2, field_name)
            result = {"merged": merged}

        else:
            logging.error("Unsupported process_type '%s'", process)
            return func.HttpResponse(
                json.dumps({"error": "Unsupported process_type."}),
                status_code=400,
                mimetype="application/json",
            )

        logging.info("Processed %s successfully.", process)
        return func.HttpResponse(
            json.dumps(result), status_code=200, mimetype="application/json"
        )

    except Exception as exc:
        logging.exception("Unhandled error processing '%s': %s", process, exc)
        return func.HttpResponse(
            json.dumps({"error": "Internal server error."}),
            status_code=500,
            mimetype="application/json",
        )


# TrsnlationStep1 {body}
# {
# "process_type": "TranslationStep1",
# "fieldName": "IngredientList",
# "text": "This product contains Charcoal, Clay (Kaolin, Bentonite), Plant Extracts (Chamomile, Aloe), and Shea Oil for soothing.",
# "language": "Spanish"
# }

# TrsnlationStep2 {body}
# {
#   "process_type": "TranslationStep2",
#   "main_string": "Sugar, Acids (Citric Acid), Preservative (Sodium Benzoate), Modified Starch, Stabilisers (Glycerol Esters Of Wood Rosins, Sucrose Acetate Isobutyrate), Colours (E 110), Sugar, Antioxidant (Ascorbic Acid)",
#   "source_array": ["Sugar", "Acids", "Citric Acid", "Preservative", "Sodium Benzoate", "Modified Starch", "Stabilisers", "Glycerol Esters Of Wood Rosins", "Sucrose Acetate Isobutyrate", "Colours", "E 110", ",", "Sugar", "Antioxidant", "Acide Ascorbique"],
#   "target_array": ["Sucre", "Acides", "Acide Citrique", "Conservateur", "Benzoate de Sodium", "Amidon modifié", "Stabilisateurs", "Esters glycérols de résines de bois", "Saccharose acétate isobutyrate", "Couleurs", "E 110", ",", "Sucre", "Antioxydant", "Acide Ascorbique"]
# }

# Catalog {body}
# {
#   "process_type": "Catalog",
#   "fieldName": "IngredientList",
#   "text1": "SUGAR, WATER, CITRIC ACID, FLAVOURS, CONSERVATIVES",
#   "lang1": "English",
#   "text2": "SUCRE, EAU, ACIDE CITRIQUE, ARÔMES, CONSERVATEURS",
#   "lang2": "French"
# }
