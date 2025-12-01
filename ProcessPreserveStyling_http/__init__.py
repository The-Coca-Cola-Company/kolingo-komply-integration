import os
import re
import json
import time
import random
import logging
from typing import List, Dict, Any, Iterable, Optional, Tuple
import requests
from azure.functions import HttpRequest, HttpResponse
import azure.functions as func
from azure.identity import ClientSecretCredential

# =============================================================================
# ============================== LOGGING SETUP ================================
# =============================================================================
logging.basicConfig(
    level=getattr(logging, "INFO", logging.INFO),
    format="%(asctime)s %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# =============================================================================
# ============================== CONFIG / CONSTANTS ===========================
# =============================================================================

# -- System prompt for alignment API --
SYSTEM_PROMPT = (
    "You align styled phrases from a source text to contiguous spans in a translated text.\n\n"
    "Return EXACTLY this JSON object with DOUBLE-QUOTED strings and NO extra fields:\n"
    "{\n"
    '  "bold": ["sourceBoldA****targetBoldA", "sourceBoldB****targetBoldB"],\n'
    '  "italics": ["sourceItalicA****targetItalicA","sourceItalicB****targetItalicB"],\n'
    '  "underline": ["sourceUnderlineA****targetUnderlineA","sourceUnderlineB****targetUnderlineB"]\n'
    "}\n"
    "Rules:\n"
    "- For each source phrase in each list (bold, italics, underline), choose the single best contiguous substring from the translated text that represents the same semantic content.\n"
    "- Case/diacritic-insensitive search is OK, but OUTPUT must use the exact substring as it appears in the translated text.\n"
    "- Prefer left-to-right, non-overlapping placements within each style list. Overlaps across styles are allowed.\n"
    "- Include clitics/determiners/particles if essential in the target language.\n"
    "- When the source phrase is a number (e.g., 100), match it directly to the identical number in the target text if present.\n"
    '- If a phrase does not appear or you are not confident, output the empty string as the target (e.g., "source****").\n'
    "- DO NOT hallucinate content.\n"
    "- IMPORTANT: Output ONLY the raw JSON object as above example. No code fences, no prose, no trailing commas.\n"
)

# -- Special keys per request type --
SPECIAL_KEYS_LABEL = ["IngredientList", "WarningAdvisoryStatement"]
SPECIAL_KEYS_PR = ["#IngredientList#", "#WarningAdvisoryStatement#"]

# -- APIM (Alignment) --
APIM_URL: str = os.getenv("EV__APIM_URL")
APIM_KEY: str = os.getenv("EV__APIM_KEY")

# -- Dataverse OAuth (env-driven) --
DV_BASE_URL = os.getenv("EV__DATAVERSE_URL")
DV_TENANT_ID = os.getenv("EV__TENANT_ID")
DV_CLIENT_ID = os.getenv("EV__CLIENT_ID")
DV_CLIENT_SECRET = os.getenv("EV__CLIENT_SECRET")
DV_API_VERSION = "v9.2"

# -- Table / column names --
TABLE_LANGUAGE_ORDERS = "cococola_tccc_languageorders"
TABLE_LEGALNAMES = "cococola_tccc_transaltion_legalnames"
TABLE_PRODUCT_REGISTRATIONS = "cococola_tccc_productregistrations"

COL_CASE_ID = "cococola_caseid"

COL_LANG_ORDER = "cococola_languageorder"
COL_LANG_DEST = "cococola_destination"

COL_LEGAL_ID = "cococola_tccc_transaltion_legalnameid"
COL_KEY = "cococola_key"
COL_VALUE = "cococola_value"
COL_SOURCESTYLES = "cococola_sourcestyles"
COL_LANG_PREFIX = "cococola_lang"  # + index (1..N)
COL_TARGET_PREFIX = "cococola_targetstyles"  # + index (1..N)

COL_PR_ID = "cococola_tccc_productregistrationid"
COL_PR_TRANSLATED = "cococola_translatedtext"
COL_PR_TARGET = "cococola_targetstyles"


# =============================================================================
# ============================== DATAVERSE CLIENT =============================
# =============================================================================
class DataverseClient:
    """Readable Dataverse Web API client (client_credentials)."""

    def __init__(
        self,
        base_url: str,
        token: str,
        api_version: str = DV_API_VERSION,
        timeout: int = 30,
    ):
        try:
            self.base_url = base_url.rstrip("/")
            self.api_root = f"{self.base_url}/api/data/{api_version}"
            self.timeout = timeout
            self.session = requests.Session()
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "Content-Type": "application/json; charset=utf-8",
                    "OData-MaxVersion": "4.0",
                    "OData-Version": "4.0",
                }
            )
        except Exception:
            logging.exception("DataverseClient.__init__ failed")
            raise

    @staticmethod
    def get_token(
        tenant_id: str, client_id: str, client_secret: str, scope: str
    ) -> str:
        try:
            url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            data = {
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
                "scope": scope,
            }
            r = requests.post(url, data=data, timeout=30)
            r.raise_for_status()
            token = r.json().get("access_token")
            if not token:
                raise RuntimeError("No access_token in token response")
            return token
        except Exception:
            logging.exception("get_token failed")
            raise

    def list_rows(
        self,
        table: str,
        select: Iterable[str],
        odata_filter: Optional[str] = None,
        orderby: Optional[str] = None,
        top: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        try:
            params: Dict[str, str] = {}
            if select:
                params["$select"] = ",".join(select)
            if odata_filter:
                params["$filter"] = odata_filter
            if orderby:
                params["$orderby"] = orderby
            if top is not None:
                params["$top"] = str(top)

            url = f"{self.api_root}/{table}"
            resp = self.session.get(url, params=params, timeout=self.timeout)
            if not resp.ok:
                raise RuntimeError(f"GET {url} failed: {resp.status_code} {resp.text}")
            return resp.json().get("value", [])
        except Exception:
            logging.exception("list_rows failed")
            raise

    def patch_row(self, table: str, row_id: str, body: Dict[str, Any]) -> None:
        try:
            url = f"{self.api_root}/{table}({row_id})"
            resp = self.session.patch(
                url,
                data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                timeout=self.timeout,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"PATCH {url} failed: {resp.status_code} {resp.text}"
                )
        except Exception:
            logging.exception("patch_row failed")
            raise


def get_dataverse_client() -> DataverseClient:
    try:
        if not (DV_BASE_URL and DV_TENANT_ID and DV_CLIENT_ID and DV_CLIENT_SECRET):
            raise RuntimeError("Dataverse configuration is missing in app settings.")
        scope = f"{DV_BASE_URL.rstrip('/')}/.default"
        token = DataverseClient.get_token(
            DV_TENANT_ID, DV_CLIENT_ID, DV_CLIENT_SECRET, scope
        )
        return DataverseClient(DV_BASE_URL, token, DV_API_VERSION)
    except Exception:
        logging.exception("get_dataverse_client failed")
        raise


# =============================================================================
# ============================== STYLE UTILITIES ==============================
# =============================================================================
_WORD_CH = re.compile(
    r"[0-9A-Za-zÀ-ÿ\u00C0-\u024F\u0370-\u03FF\u0400-\u04FF\u0590-\u05FF\u0600-\u06FF\u0900-\u097F\u4E00-\u9FFF]"
)
# TRIM_EDGE = re.compile(r"^[^\w~]+|[^\w~]+$", flags=re.UNICODE)
TRIM_EDGE = re.compile(r"^[^\w~(]+|[^\w~)]+$", flags=re.UNICODE)


def _normalize_style_name(name: str) -> str:
    try:
        s = (name or "").upper()
        if s in ("BOLDED",):
            s = "BOLD"
        if s in ("ITALICS", "ITALICIZED"):
            s = "ITALIC"
        if s in ("UNDERLINED"):
            s = "UNDERLINE"
        return s
    except Exception:
        logging.exception("_normalize_style_name failed")
        raise


def _parse_styles(raw) -> List[Dict[str, Any]]:
    try:
        if not raw:
            return []
        try:
            styles = json.loads(raw) if isinstance(raw, str) else list(raw)
        except Exception:
            return []
        out: List[Dict[str, Any]] = []
        for s in styles:
            if not isinstance(s, dict):
                continue
            try:
                offset = int(s.get("offset", s.get("Offset", 0)) or 0)
                length = int(s.get("length", s.get("Length", 0)) or 0)
                style = _normalize_style_name(str(s.get("style", s.get("Style", ""))))
                if length > 0 and style in ("BOLD", "ITALIC", "UNDERLINE"):
                    out.append({"offset": offset, "length": length, "style": style})
            except Exception:
                continue
        return out
    except Exception:
        logging.exception("_parse_styles failed")
        raise


def _is_word_char(ch: str) -> bool:
    return bool(ch and (_WORD_CH.match(ch) or ch in "'-"))


def _coerce_brace_list_to_array(text: str) -> Optional[List[str]]:
    t = (text or "").strip()
    # Looks like: {"a","b","c"} with NO colons anywhere
    if t.startswith("{") and t.endswith("}") and ":" not in t:
        try:
            arr = json.loads("[" + t[1:-1] + "]")
            if isinstance(arr, list) and all(isinstance(x, str) for x in arr):
                return arr
        except Exception:
            return None
    return None


def _expand_to_word_boundaries(text: str, start: int, end: int) -> Tuple[int, int]:
    try:
        i = start
        while i > 0 and _is_word_char(text[i - 1]):
            i -= 1
        j = end
        while j < len(text) and _is_word_char(text[j]):
            j += 1
        return i, j
    except Exception:
        logging.exception("_expand_to_word_boundaries failed")
        raise


# def _trim_edges_keep_tilde(s: str) -> str:
#     try:
#         if s == "~":
#             return s
#         return TRIM_EDGE.sub("", s)
#     except Exception:
#         logging.exception("_trim_edges_keep_tilde failed")
#         raise


def extract_words_by_style(
    payload: Dict[str, Any], snap_to_word: bool = False, dedupe: bool = True
) -> Dict[str, List[str]]:
    try:
        src = payload.get("value") or ""
        spans = payload.get("styles") or []
        items = []
        for s in spans:
            try:
                start = int(s.get("offset"))
                length = int(s.get("length"))
                if length <= 0:
                    continue
                end = start + length
                style = _normalize_style_name(s.get("style", ""))
                if style not in ("BOLD", "ITALIC", "UNDERLINE"):
                    continue
                si, sj = (start, end)
                # if snap_to_word:
                #     si, sj = _expand_to_word_boundaries(src, start, end)
                # chunk = _trim_edges_keep_tilde(src[si:sj].strip())
                # No trimming — keep punctuation/commas/colons/etc.
                chunk = src[si:sj]
                if not chunk:
                    continue
                items.append((style, si, chunk))
            except Exception:
                continue
        items.sort(key=lambda t: t[1])
        out = {"bold": [], "italics": [], "underline": []}
        seen = {"bold": set(), "italics": set(), "underline": set()}
        for style, _pos, chunk in items:
            key = {"BOLD": "bold", "ITALIC": "italics", "UNDERLINE": "underline"}[style]
            if dedupe:
                norm = chunk.casefold()
                if norm in seen[key]:
                    continue
                seen[key].add(norm)
            out[key].append(chunk)
        return out
    except Exception:
        logging.exception("extract_words_by_style failed")
        raise


# =============================================================================
# ============================ ALIGNMENT (APIM) ===============================
# =============================================================================
def _strip_code_fences(text: str) -> str:
    try:
        t = text.strip()
        if t.startswith("```") and t.endswith("```"):
            t = t[3:]
            if "\n" in t:
                first_line, rest = t.split("\n", 1)
                if first_line.strip().lower() in ("json", "javascript", "js"):
                    t = rest
                else:
                    t = first_line + "\n" + rest
            if t.endswith("```"):
                t = t[:-3]
        return t.strip()
    except Exception:
        logging.exception("_strip_code_fences failed")
        raise


def _extract_first_json_object(text: str) -> Optional[str]:
    try:
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(text)):
            c = text[i]
            if in_str:
                if esc:
                    esc = False
                elif c == "\\":
                    esc = True
                elif c == '"':
                    in_str = False
            else:
                if c == '"':
                    in_str = True
                elif c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        return text[start : i + 1]
        return None
    except Exception:
        logging.exception("_extract_first_json_object failed")
        raise


def _http_post_json_with_retries(
    url: str,
    headers: Dict[str, str],
    body: Dict[str, Any],
    timeout_sec: int = 45,
    max_retries: int = 4,
) -> Dict[str, Any]:
    try:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        backoff = 1.0
        for attempt in range(max_retries + 1):
            try:
                resp = requests.post(
                    url, headers=headers, data=payload, timeout=timeout_sec
                )
                if (
                    resp.status_code in (408, 429, 500, 502, 503, 504)
                    and attempt < max_retries
                ):
                    time.sleep(backoff + random.uniform(0, 0.5))
                    backoff = min(backoff * 2, 8.0)
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception:
                if attempt >= max_retries:
                    raise
                logging.info(f"Retrying alignment call (attempt {attempt+1})")
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff = min(backoff * 2, 8.0)
        raise RuntimeError("HTTP retry loop exhausted")
    except Exception:
        logging.exception("_http_post_json_with_retries failed")
        raise


def _ensure_alignment_keys(obj: Dict[str, Any]) -> Dict[str, List[str]]:
    try:
        for k in ("bold", "italics", "underline"):
            if k not in obj:
                obj[k] = []
            obj[k] = [str(x) for x in obj[k]]
        return obj
    except Exception:
        logging.exception("_ensure_alignment_keys failed")
        raise


def call_alignment_api(
    *,
    api_url: str,
    api_key: Optional[str],
    translated_language: str,
    source_text: str,
    translated_text: str,
    bold_array: List[str],
    italics_array: List[str],
    underlined_array: List[str],
    timeout_sec: int = 45,
    max_retries: int = 4,
) -> Dict[str, List[str]]:
    try:
        if not api_url:
            raise RuntimeError("ALIGN_API_URL is not configured.")
        headers = {"Content-Type": "application/json"}
        if api_key:
            # headers["Ocp-Apim-Subscription-Key"] = api_key #this is updated after we moved from Azure APIM to AI Foundry
            headers["Authorization"] = api_key

        inputs = {
            "translated_language": translated_language,
            "source_text": source_text,
            "translated_Text": translated_text,
            "terms": {
                "bold": bold_array,
                "italics": italics_array,
                "underline": underlined_array,
            },
        }
        # "temperature": 0,
        body = {
            "temperature": 0,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": "Inputs:\n" + json.dumps(inputs, ensure_ascii=False),
                },
            ],
        }

        logging.info(
            f"APIM request → lang={translated_language} | bold={len(bold_array)}, "
            f"italics={len(italics_array)}, underline={len(underlined_array)}"
        )

        data = _http_post_json_with_retries(
            api_url, headers, body, timeout_sec, max_retries
        )

        content = data
        try:
            content = data["choices"][0]["message"]["content"]
            if isinstance(content, str):
                logging.info("APIM response (raw content):" + content)
        except Exception:
            pass

        if isinstance(content, str):
            raw = _strip_code_fences(content)

            # 1) Try normal JSON parse
            parsed = None
            try:
                parsed = json.loads(raw)
            except Exception:
                # 2) Try to extract the first JSON object from prose
                maybe = _extract_first_json_object(raw)
                if maybe:
                    try:
                        parsed = json.loads(maybe)
                    except Exception:
                        parsed = None

            # 3) SPECIAL FALLBACK: brace-wrapped list → array → split into buckets
            if parsed is None:
                brace_list = _coerce_brace_list_to_array(raw)
                if brace_list is not None:
                    bN = len(bold_array)
                    iN = len(italics_array)
                    uN = len(underlined_array)
                    total = bN + iN + uN
                    # Only trust if lengths match
                    if len(brace_list) == total:
                        obj = {
                            "bold": brace_list[0:bN],
                            "italics": brace_list[bN : bN + iN],
                            "underline": brace_list[bN + iN : bN + iN + uN],
                        }
                        logging.info(
                            "Applied brace-list fallback to reconstruct alignment object."
                        )
                        return _ensure_alignment_keys(obj)

            if parsed is None:
                raise ValueError(
                    f"Alignment API did not return valid JSON content: {content!r}"
                )

            if isinstance(parsed, list):
                # Flat list returned; best-effort split by counts
                bN = len(bold_array)
                iN = len(italics_array)
                uN = len(underlined_array)
                obj = {
                    "bold": parsed[0:bN],
                    "italics": parsed[bN : bN + iN],
                    "underline": parsed[bN + iN : bN + iN + uN],
                }
            elif isinstance(parsed, dict):
                obj = parsed
            else:
                raise ValueError(
                    "Unexpected alignment API response structure (string mode)."
                )

            obj = _ensure_alignment_keys(obj)

        # if isinstance(content, str):
        #     raw = _strip_code_fences(content)
        #     parsed = None
        #     try:
        #         parsed = json.loads(raw)
        #     except Exception:
        #         maybe = _extract_first_json_object(raw)
        #         if maybe:
        #             parsed = json.loads(maybe)
        #     if parsed is None:
        #         raise ValueError(
        #             f"Alignment API did not return valid JSON content: {content!r}"
        #         )
        #     obj = _ensure_alignment_keys(parsed)
        # elif isinstance(content, dict):
        #     obj = _ensure_alignment_keys(content)
        # else:
        #     raise ValueError("Unexpected alignment API response structure")

        logging.info(
            f"APIM response: bold={len(obj['bold'])}, italics={len(obj['italics'])}, underline={len(obj['underline'])}"
        )
        return obj
    except Exception:
        logging.exception("call_alignment_api failed")
        raise


def _split_pair(s: str) -> Tuple[str, str]:
    try:
        return s.split("****", 1) if "****" in s else (s, "")
    except Exception:
        logging.exception("_split_pair failed")
        raise


def _find_from(text: str, needle: str, start: int) -> int:
    """Find needle in text (case-insensitive fallback), prefer forward search from 'start'."""
    try:
        pos = text.find(needle, start)
        if pos != -1:
            return pos
        pos = text.lower().find(needle.lower(), start)
        if pos != -1:
            return pos
        return text.find(needle)
    except Exception:
        logging.exception("_find_from failed")
        raise


def _pair_positionally_if_needed(
    alignment: Dict[str, List[str]], source_terms: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    try:
        fixed: Dict[str, List[str]] = {"bold": [], "italics": [], "underline": []}
        applied = False
        for key in ("bold", "italics", "underline"):
            items = alignment.get(key, []) or []
            has_pairs = any("****" in x for x in items)
            if not items:
                fixed[key] = []
                continue
            if not has_pairs:
                srcs = source_terms.get(key, [])
                n = min(len(items), len(srcs))
                paired = [f"{srcs[i]}****{items[i]}" for i in range(n) if items[i]]
                fixed[key] = paired
                applied = True
            else:
                fixed[key] = items
        if applied:
            logging.info(
                "Applied positional pairing fallback (converted targets → 'source****target')."
            )
        return fixed
    except Exception:
        logging.exception("_pair_positionally_if_needed failed")
        raise


def transform_alignment_to_styles(
    *,
    alignment: Dict[str, List[str]],
    translated_text: str,
    keep_unmatched: bool = False,
) -> List[Dict[str, Any]]:
    try:
        out: List[Dict[str, Any]] = []
        cursors = {"bold": 0, "italics": 0, "underline": 0}

        for key, style_name in (
            ("bold", "BOLD"),
            ("italics", "ITALIC"),
            ("underline", "UNDERLINE"),
        ):
            arr = alignment.get(key, []) or []
            cursor = cursors[key]
            for item in arr:
                src, tgt = _split_pair(item)
                if tgt:
                    pos = _find_from(translated_text, tgt, cursor)
                    if pos != -1:
                        out.append(
                            {
                                "style": style_name,
                                "source": src,
                                "translated_text": translated_text[
                                    pos : pos + len(tgt)
                                ],
                                "offset": pos,
                                "length": len(tgt),
                            }
                        )
                        cursor = pos + len(tgt)
                    elif keep_unmatched:
                        out.append(
                            {
                                "style": style_name,
                                "source": src,
                                "translated_text": "",
                                "offset": -1,
                                "length": 0,
                            }
                        )
                elif keep_unmatched:
                    out.append(
                        {
                            "style": style_name,
                            "source": src,
                            "translated_text": "",
                            "offset": -1,
                            "length": 0,
                        }
                    )
            cursors[key] = cursor

        logging.info(f"Located spans → count={len(out)}")
        return out
    except Exception:
        logging.exception("transform_alignment_to_styles failed")
        raise


def align_styled_terms_with_translation(
    *,
    payload: Dict[str, Any],
    translated_language: str,
    translated_text: str,
    api_url: str,
    api_key: Optional[str] = None,
    source_text_fallback: Optional[str] = None,
) -> Dict[str, List[str]]:
    try:
        source_text = payload.get("value") or source_text_fallback or ""
        arrays = extract_words_by_style(payload, snap_to_word=True, dedupe=True)

        logging.info(
            "Extracted Styles → "
            f"bold={arrays.get('bold',[])}, "
            f"italics={arrays.get('italics',[])}, "
            f"underline={arrays.get('underline',[])}"
        )

        raw_alignment = call_alignment_api(
            api_url=api_url,
            api_key=api_key,
            translated_language=translated_language,
            source_text=source_text,
            translated_text=translated_text,
            bold_array=arrays.get("bold", []),
            italics_array=arrays.get("italics", []),
            underlined_array=arrays.get("underline", []),
        )

        fixed_alignment = _pair_positionally_if_needed(raw_alignment, arrays)
        return fixed_alignment
    except Exception:
        logging.exception("align_styled_terms_with_translation failed")
        raise


# =============================================================================
# ============================== CASE PROCESSORS ==============================
# =============================================================================
def _lang_cols(index_1_based: int) -> Tuple[str, str]:
    try:
        return (
            f"{COL_LANG_PREFIX}{index_1_based}",
            f"{COL_TARGET_PREFIX}{index_1_based}",
        )
    except Exception:
        logging.exception("_lang_cols failed")
        raise


def process_label_translation(
    *,
    case_id: str,
    dv: DataverseClient,
    keep_unmatched: bool = False,
) -> Dict[str, Any]:
    try:
        summary = {
            "case_id": case_id,
            "table": TABLE_LEGALNAMES,
            "languages_processed": [],
            "records_updated": 0,
            "skipped_no_translation": 0,
        }

        # 1) Languages by order
        lang_rows = dv.list_rows(
            table=TABLE_LANGUAGE_ORDERS,
            select=[COL_LANG_ORDER, COL_LANG_DEST],
            odata_filter=f"{COL_CASE_ID} eq '{case_id}'",
            orderby=f"{COL_LANG_ORDER} asc",
        )
        langs = [(i + 1, r.get(COL_LANG_DEST, "")) for i, r in enumerate(lang_rows)]
        summary["languages_processed"] = [lang for _, lang in langs]
        logging.info(f"Languages fetched: {summary['languages_processed']}")
        if not langs:
            logging.info("No language order rows found; nothing to do.")
            return summary

        # 2) Select language columns up-front
        lang_cols = [f"{COL_LANG_PREFIX}{idx}" for idx, _ in langs]
        logging.info(f"Selecting language columns: {lang_cols}")

        # 3) Legalname rows for SPECIAL_KEYS_LABEL (include lang columns)
        key_filter = " or ".join([f"{COL_KEY} eq '{k}'" for k in SPECIAL_KEYS_LABEL])
        legal_rows = dv.list_rows(
            table=TABLE_LEGALNAMES,
            select=[COL_LEGAL_ID, COL_KEY, COL_VALUE, COL_SOURCESTYLES] + lang_cols,
            odata_filter=f"{COL_CASE_ID} eq '{case_id}' and ({key_filter})",
        )
        logging.info(f"Fetched legalname rows: {len(legal_rows)}")

        for row in legal_rows:
            row_id = row[COL_LEGAL_ID]
            row_key = row.get(COL_KEY)
            source_text = row.get(COL_VALUE) or ""
            styles = _parse_styles(row.get(COL_SOURCESTYLES))
            logging.info(
                f"Row id={row_id} key={row_key} src_len={len(source_text)} styles={len(styles)}"
            )

            for idx, translated_language in langs:
                lang_col, target_col = _lang_cols(idx)
                translated_text = (row.get(lang_col) or "").strip()

                t = (translated_text or "").replace("\n", " ")
                t = t if len(t) <= 160 else (t[:160] + "…")
                logging.info(
                    f"  {translated_language:<10s} {lang_col:<20s} len={len(translated_text)} sample='{t}'"
                )

                if translated_text:
                    payload = {"value": source_text, "styles": styles}
                    alignment_pairs = align_styled_terms_with_translation(
                        payload=payload,
                        translated_language=translated_language,
                        translated_text=translated_text,
                        api_url=APIM_URL,
                        api_key=APIM_KEY,
                    )
                    result_styles = transform_alignment_to_styles(
                        alignment=alignment_pairs,
                        translated_text=translated_text,
                        keep_unmatched=keep_unmatched,
                    )
                else:
                    result_styles = []
                    summary["skipped_no_translation"] += 1
                    logging.info("   ↳ no translated_text; writing empty []")

                dv.patch_row(
                    table=TABLE_LEGALNAMES,
                    row_id=row_id,
                    body={target_col: json.dumps(result_styles, ensure_ascii=False)},
                )
                logging.info(
                    f"Updated column {target_col} with {len(result_styles)} spans"
                )
                summary["records_updated"] += 1

        return summary
    except Exception:
        logging.exception("process_label_translation failed")
        raise


def process_product_registration(
    *,
    case_id: str,
    dv: DataverseClient,
    keep_unmatched: bool = False,
) -> Dict[str, Any]:
    try:
        summary = {
            "case_id": case_id,
            "table": TABLE_PRODUCT_REGISTRATIONS,
            "language_used": None,
            "records_updated": 0,
            "skipped_no_translation": 0,
        }

        # 1) First language (lowest order)
        lang_rows = dv.list_rows(
            table=TABLE_LANGUAGE_ORDERS,
            select=[COL_LANG_ORDER, COL_LANG_DEST],
            odata_filter=f"{COL_CASE_ID} eq '{case_id}'",
            orderby=f"{COL_LANG_ORDER} asc",
        )
        if not lang_rows:
            logging.info(
                "No language order rows found for ProductRegistration; nothing to do."
            )
            return summary
        translated_language = lang_rows[0].get(COL_LANG_DEST, "")
        summary["language_used"] = translated_language
        logging.info(f"ProductRegistration language: {translated_language}")

        # 2) ProductRegistration rows for SPECIAL_KEYS_PR
        key_filter = " or ".join([f"{COL_KEY} eq '{k}'" for k in SPECIAL_KEYS_PR])
        pr_rows = dv.list_rows(
            table=TABLE_PRODUCT_REGISTRATIONS,
            select=[COL_PR_ID, COL_KEY, COL_VALUE, COL_SOURCESTYLES, COL_PR_TRANSLATED],
            odata_filter=f"{COL_CASE_ID} eq '{case_id}' and ({key_filter})",
        )
        logging.info(f"Fetched productregistration rows: {len(pr_rows)}")

        for row in pr_rows:
            row_id = row[COL_PR_ID]
            row_key = row.get(COL_KEY)
            source_text = row.get(COL_VALUE) or ""
            styles = _parse_styles(row.get(COL_SOURCESTYLES))
            translated_text = (row.get(COL_PR_TRANSLATED) or "").strip()

            t = (translated_text or "").replace("\n", " ")
            t = t if len(t) <= 160 else (t[:160] + "…")
            logging.info(
                f"Row id={row_id} key={row_key} src_len={len(source_text)} styles={len(styles)} "
                f"t_len={len(translated_text)} sample='{t}'"
            )

            if translated_text:
                payload = {"value": source_text, "styles": styles}
                alignment_pairs = align_styled_terms_with_translation(
                    payload=payload,
                    translated_language=translated_language,
                    translated_text=translated_text,
                    api_url=APIM_URL,
                    api_key=APIM_KEY,
                )
                result_styles = transform_alignment_to_styles(
                    alignment=alignment_pairs,
                    translated_text=translated_text,
                    keep_unmatched=keep_unmatched,
                )
            else:
                result_styles = []
                summary["skipped_no_translation"] += 1
                logging.info("   ↳ no translated_text; writing empty []")

            dv.patch_row(
                table=TABLE_PRODUCT_REGISTRATIONS,
                row_id=row_id,
                body={COL_PR_TARGET: json.dumps(result_styles, ensure_ascii=False)},
            )
            logging.info(
                f"Updated column {COL_PR_TARGET} with {len(result_styles)} spans"
            )
            summary["records_updated"] += 1

        return summary
    except Exception:
        logging.exception("process_product_registration failed")
        raise


# =============================================================================
# ================================ DISPATCHER =================================
# =============================================================================
def process_case(
    *,
    requestType: str,
    case_id: str,
    dv: DataverseClient,
    keep_unmatched: bool = False,
) -> Dict[str, Any]:
    try:
        rt = (requestType or "").strip().lower()
        logging.info(f"Process start: requestType={rt}, case_id={case_id}")
        if rt == "labeltranslation":
            summary = process_label_translation(
                case_id=case_id, dv=dv, keep_unmatched=keep_unmatched
            )
        elif rt == "productregistration":
            summary = process_product_registration(
                case_id=case_id, dv=dv, keep_unmatched=keep_unmatched
            )
        else:
            raise ValueError(
                "requestType must be 'LabelTranslation' or 'ProductRegistration'."
            )
        logging.info("Process complete.")
        return summary
    except Exception:
        logging.exception("process_case failed")
        raise


# =============================================================================
# ============================== AZURE FUNCTION ===============================
# =============================================================================
app = func.FunctionApp()


def main(req: HttpRequest) -> HttpResponse:
    logging.info("Function triggered: ProcessPreserveStyling_http")
    """
    HTTP trigger expecting JSON:
    {
      "requestType": "LabelTranslation" | "ProductRegistration",
      "caseId": "CCLR-2968",
      "keepUnmatched": false   // optional
    }
    """
    try:
        try:
            body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON body."}),
                status_code=400,
                mimetype="application/json",
            )

        request_type = body.get("requestType") or body.get(
            "REQUEST_TYPE"
        )  # "LabelTranslation"
        case_id = body.get("caseId") or body.get("CASE_ID")  # "CCLR-2968"
        keep_unmatched = bool(body.get("keepUnmatched", False))

        if not request_type or not case_id:
            return func.HttpResponse(
                json.dumps({"error": "Both 'requestType' and 'caseId' are required."}),
                status_code=400,
                mimetype="application/json",
            )

        # Build DV client and process
        dv_client = get_dataverse_client()
        result = process_case(
            requestType=request_type,
            case_id=case_id,
            dv=dv_client,
            keep_unmatched=keep_unmatched,
        )

        return func.HttpResponse(
            json.dumps(result, ensure_ascii=False),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as exc:
        logging.exception("Unhandled failure in HTTP function")
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=500,
            mimetype="application/json",
        )
