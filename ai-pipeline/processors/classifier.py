import json, logging, time
from groq import Groq

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a conflict event classifier for the 2026 US-Iran war monitoring system.

Given a raw event (from GDELT or news RSS), extract and classify it into a structured format.

Respond ONLY with valid JSON, no markdown, no explanation. Use this exact schema:
{
    "category": "battle|explosion_remote_violence|violence_against_civilians|protest|riot|strategic_development|naval_engagement|cyber_attack|humanitarian_incident",
    "sub_type": "specific type like: airstrike, ballistic_missile, drone_strike, naval_blockade, troop_movement, sanctions, diplomatic_statement, civilian_casualty, protest_rally, etc.",
    "summary": "One clear sentence describing what happened, where, and who was involved.",
    "location": "Most specific place name mentioned",
    "country": "Primary country where event occurred",
    "region": "Region/province if available",
    "actor1": "Primary actor (who did it)",
    "actor1_type": "state_military|rebel|militia|civilian|government|international_org",
    "actor2": "Secondary actor (target/other party)",
    "actor2_type": "state_military|rebel|militia|civilian|government|international_org",
    "severity": 1-10 (1=minor diplomatic statement, 5=significant military action, 8=major battle/mass casualties, 10=WMD/catastrophic),
    "fatalities": null or estimated number if mentioned,
    "fatalities_precision": "exact|estimated|unknown"
}

Context: The US and Israel launched Operation Epic Fury / Operation Roaring Lion against Iran on Feb 28, 2026. Iran responded with Operation True Promise IV. Key actors: US military (CENTCOM), IDF, IRGC, Hezbollah, Houthis. Key locations: Tehran, Isfahan, Strait of Hormuz, Persian Gulf, Bahrain (Fifth Fleet), various Gulf states."""

CAMEO_HINTS = {
    "13": "threaten",
    "14": "protest",
    "15": "exhibit_force_posture",
    "17": "coerce",
    "18": "assault",
    "19": "fight",
    "190": "use_conventional_force",
    "193": "fight_with_artillery",
    "194": "occupy_territory",
    "195": "fight_with_air_weapons",
    "20": "use_unconventional_mass_violence",
}


class EventClassifier:
    def __init__(self, api_key: str):
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"
        self._call_count = 0
        self._last_reset = time.time()

    def _rate_limit(self):
        """Simple rate limiter: max 25 calls per minute for Groq free tier."""
        self._call_count += 1
        if self._call_count >= 25:
            elapsed = time.time() - self._last_reset
            if elapsed < 60:
                wait = 60 - elapsed + 1
                log.info(f"⏳ Rate limit: waiting {wait:.0f}s")
                time.sleep(wait)
            self._call_count = 0
            self._last_reset = time.time()

    def classify(self, raw_event: dict) -> dict:
        """Classify a raw event using Groq LLM."""
        # Build context from raw event
        title = raw_event.get("title", "")
        text = raw_event.get("raw_text", "")
        source = raw_event.get("source", "")
        cameo = raw_event.get("gdelt_cameo_code", "")
        goldstein = raw_event.get("gdelt_goldstein", 0)
        actor1 = raw_event.get("actor1", "")
        actor2 = raw_event.get("actor2", "")
        location = raw_event.get("location_raw", "")
        country = raw_event.get("country", "")

        cameo_hint = ""
        if cameo:
            for prefix, desc in CAMEO_HINTS.items():
                if cameo.startswith(prefix):
                    cameo_hint = f"CAMEO code {cameo} suggests: {desc}"
                    break

        user_msg = f"""Classify this conflict event:

Source: {source}
Title: {title}
Text: {text[:1500] if text else 'N/A'}
Location: {location}
Country: {country}
Actor1: {actor1}
Actor2: {actor2}
CAMEO: {cameo_hint}
Goldstein Scale: {goldstein}

Respond with JSON only."""

        self._rate_limit()

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.1,
                max_tokens=500,
            )

            content = resp.choices[0].message.content.strip()
            # Clean potential markdown wrapping
            if content.startswith("```"):
                content = content.split("\n", 1)[-1].rsplit("```", 1)[0]

            return json.loads(content)

        except json.JSONDecodeError as e:
            log.warning(f"⚠️ Failed to parse LLM response: {e}")
            return self._fallback_classify(raw_event)
        except Exception as e:
            log.warning(f"⚠️ LLM classification failed: {e}")
            return self._fallback_classify(raw_event)

    def _fallback_classify(self, raw: dict) -> dict:
        """Rule-based fallback when LLM fails."""
        cameo = raw.get("gdelt_cameo_code", "")
        cat = "strategic_development"
        sub = "unknown"

        if cameo.startswith("19") or cameo.startswith("18"):
            cat = "battle" if cameo.startswith("19") else "explosion_remote_violence"
            sub = "military_engagement"
        elif cameo.startswith("20"):
            cat = "violence_against_civilians"
            sub = "mass_violence"
        elif cameo.startswith("14"):
            cat = "protest"
            sub = "demonstration"
        elif cameo.startswith("15"):
            cat = "strategic_development"
            sub = "force_posture"
        elif cameo.startswith("17"):
            cat = "strategic_development"
            sub = "coercion"
        elif cameo.startswith("13"):
            cat = "strategic_development"
            sub = "threat"

        return {
            "category": cat,
            "sub_type": sub,
            "summary": raw.get("title", "Unclassified conflict event"),
            "location": raw.get("location_raw", ""),
            "country": raw.get("country", "Unknown"),
            "region": "",
            "actor1": raw.get("actor1", ""),
            "actor1_type": "state_military",
            "actor2": raw.get("actor2", ""),
            "actor2_type": "state_military",
            "severity": 5,
            "fatalities": None,
            "fatalities_precision": "unknown",
        }
    