# config.py
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class TrelloConfig:
    api_key: str
    api_token: str
    short_link_base: str = "https://trello.com/c/"
    api_base: str = "https://api.trello.com/1"
    actions_limit: int = 1000
    timeout_seconds: int = 30

@dataclass(frozen=True)
class HubSpotConfig:
    private_app_token: str
    api_base: str = "https://api.hubapi.com"
    timeout_seconds: int = 30
    page_limit: int = 500
    max_retries: int = 6
    backoff_base_seconds: float = 0.8
    note_to_contact_type_id: int = 0
    note_to_deal_type_id: int = 0

@dataclass(frozen=True)
class OpenAIConfig:
    api_key: str
    assistant_id: str
    # Polling / safety
    poll_interval_seconds: float = 1.0
    max_poll_seconds: int = 120
    max_retries: int = 4
    backoff_base_seconds: float = 0.8

@dataclass(frozen=True)
class AppConfig:
    output_dir: str = "output"

    duplicates_csv_name: str = "step1_duplicates.csv"
    trello_enriched_jsonl_name: str = "step1_trello_enriched.jsonl"
    trello_ready_csv_name: str = "step1_ready_for_step2.csv"

    hubspot_enriched_jsonl_name: str = "step2_hubspot_enriched.jsonl"
    merged_ready_csv_name: str = "step2_merged_ready_for_ai.csv"

    step3_ai_jsonl_name: str = "step3_ai_results.jsonl"
    step3_output_csv_name: str = "step3_final_review.csv"
    step3_failed_parse_csv_name: str = "step3_failed_parse.csv"


def load_config() -> tuple[AppConfig, TrelloConfig, HubSpotConfig, OpenAIConfig]:
    trello_key = os.getenv("TRELLO_API_KEY", "").strip()
    trello_token = os.getenv("TRELLO_API_TOKEN", "").strip()
    if not trello_key or not trello_token:
        raise RuntimeError("Missing Trello credentials. Set TRELLO_API_KEY and TRELLO_API_TOKEN")

    hs_token = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN", "").strip()
    if not hs_token:
        raise RuntimeError("Missing HubSpot token. Set HUBSPOT_PRIVATE_APP_TOKEN")

    oa_key = os.getenv("OPENAI_API_KEY", "").strip()
    oa_asst = os.getenv("OPENAI_ASSISTANT_ID", "").strip()
    if not oa_key or not oa_asst:
        raise RuntimeError("Missing OpenAI config. Set OPENAI_API_KEY and OPENAI_ASSISTANT_ID")
    note_to_contact = int(os.getenv("HS_ASSOC_NOTE_TO_CONTACT_TYPE_ID", "0"))
    note_to_deal = int(os.getenv("HS_ASSOC_NOTE_TO_DEAL_TYPE_ID", "0"))

    return (
        AppConfig(),
        TrelloConfig(api_key=trello_key, api_token=trello_token),
        HubSpotConfig(private_app_token=hs_token, note_to_contact_type_id=note_to_contact, note_to_deal_type_id=note_to_deal),
        OpenAIConfig(api_key=oa_key, assistant_id=oa_asst),
    )
