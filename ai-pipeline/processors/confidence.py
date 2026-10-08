import logging

log = logging.getLogger(__name__)

TIER_WEIGHTS = {
    "tier1": 0.40,   # Wire services (Reuters, AP, AFP)
    "tier2": 0.30,   # Major outlets (BBC, Al Jazeera, CNN)
    "tier3": 0.20,   # Regional outlets, NGO reports
    "social": 0.10,  # Verified social accounts
}


class ConfidenceScorer:
    """
    Scores event confidence from 0.0 to 1.0 based on:
    - Source tier (higher tier = more trustworthy)
    - Data completeness (has coords, actors, text)
    - GDELT-specific signals (num mentions, num sources)
    """

    def score(self, raw_event: dict, classification: dict) -> float:
        score = 0.0

        # 1. Source tier base score
        tier = raw_event.get("source_tier", "tier3")
        score += TIER_WEIGHTS.get(tier, 0.15)

        # 2. Data completeness (max +0.25)
        completeness = 0.0
        if raw_event.get("lat", 0) != 0:
            completeness += 0.05
        if raw_event.get("actor1"):
            completeness += 0.05
        if raw_event.get("actor2"):
            completeness += 0.05
        if raw_event.get("raw_text"):
            completeness += 0.05
        if classification.get("summary"):
            completeness += 0.05
        score += completeness

        # 3. GDELT signals (max +0.20)
        num_mentions = raw_event.get("gdelt_num_mentions", 0)
        if num_mentions > 20:
            score += 0.20
        elif num_mentions > 10:
            score += 0.15
        elif num_mentions > 5:
            score += 0.10
        elif num_mentions > 1:
            score += 0.05

        # 4. Goldstein intensity bonus (more negative = more clearly conflict)
        goldstein = raw_event.get("gdelt_goldstein", 0)
        if goldstein < -7:
            score += 0.10
        elif goldstein < -4:
            score += 0.05

        # 5. News articles get a boost (they have actual text content)
        if raw_event.get("source") == "rss" and raw_event.get("raw_text"):
            score += 0.10

        return min(score, 1.0)