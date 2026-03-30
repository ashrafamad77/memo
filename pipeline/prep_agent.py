"""Prep Agent v2: universal semantic decomposition before CIDOC modeling."""
import json
import re
from typing import Any, Dict, List, Optional, Tuple


PREP_PROMPT = """Tu es un agent de decomposition semantique.
Tu transformes un texte de journal brut en une representation structuree universelle.

Retourne UNIQUEMENT un JSON valide avec cette structure:

{
  "micro_events": [
    {
      "id": "e1",
      "text": "description courte de l'activite",
      "type_hint": "social|transport|work|communication|transfer|creation|consumption|reflection|other",
      "time_text": "7H40",
      "participants": [
        {"name": "Marie", "role": "participant|giver|receiver|speaker|listener|owner|other"}
      ],
      "objects": [
        {"name": "livre", "role": "transferred_object|tool|topic|other"}
      ],
      "places": [
        {"name": "Paris", "role": "physical_location|remote_context|destination|origin"}
      ]
    }
  ],
  "event_links": [
    {"from": "e1", "to": "e2", "type": "sequence|causes|enables|impacts|motivates"}
  ],
  "mental_states": [
    {
      "id": "s1",
      "text": "description de l'etat mental",
      "type_hint": "emotion|expectation|disappointment|satisfaction|fear|stress|reflection|other",
      "caused_by": "e1",
      "affects": "user"
    }
  ],
  "expectations": [
    {
      "id": "x1",
      "text": "description de l'attente",
      "set_by": "e1",
      "violated_by": "e2",
      "fulfilled_by": null
    }
  ],
  "habits": [
    {
      "id": "h1",
      "text": "description de l'habitude",
      "influenced_by_propositions": ["decalage horaire entre France et Palestine"]
    }
  ],
  "reflections": [
    {
      "id": "r1",
      "text": "meta-commentaire ou reflexion",
      "motivated_by": "e1",
      "about": ["h1", "s1"]
    }
  ],
  "entities": [
    {"name": "Marie", "type": "person|place|organization|object|concept", "known": true}
  ],
  "normalized_text": "reformulation claire et explicite",
  "confidence": 0.8
}

Regles:
- micro_events: chaque ACTIVITE concrete (temps et/ou lieu) — quelque chose que le narrateur **fait** ou qui se passe comme **action** (transport, arrivee, achat, envoi, codage, etude, etc.). Ordonne-les chronologiquement. id commence a e1.
- **OBLIGATOIRE:** si le texte decrit une ou plusieurs actions dans la journee (ex. "Spent the morning at Victoria, then had a heavy bout of coding in the library"), tu **dois** remplir **micro_events** avec **au moins un** evenement par segment d'action (souvent deux micro_events relies par event_links type "sequence"). **Jamais** laisser **micro_events** vide dans ce cas — sinon le graphe CIDOC ne sera pas construit.
- Ne cree **pas** de micro_event pour un **etat ou une sensation purement vecue** (ex. avoir faim, se sentir fatigue, ressentir du stress) : mets cela uniquement dans **mental_states**, avec **caused_by** pointant vers le micro_event **declencheur** (ex. arrivee au bureau). Une seule representation dans le JSON pour ce genre d'episode.
- Un transfer (donner/preter/rendre un objet) est un micro_event avec type_hint "transfer" et les roles giver/receiver/transferred_object.
- event_links: liens entre micro_events uniquement quand le texte le justifie.
- mental_states: emotions, douleur, joie, stress, deception. Utilise caused_by pour pointer vers le micro_event ou expectation qui cause cet etat.
- expectations: quand le texte implique une attente (retour d'un objet, reponse, comportement attendu). violated_by pointe vers l'evenement qui brise l'attente, fulfilled_by si elle est satisfaite.
- habits: regles habituelles/recurrentes mentionnees. influenced_by_propositions liste les raisons de l'habitude.
- reflections: quand l'utilisateur pense a/commente sur un evenement ou habitude. motivated_by pointe vers le micro_event, about liste les ids des habits/states concernes.
- entities: toutes les entites mentionnees. known=false si la personne est inconnue ("une fille que je connais pas").
- Ne pas inventer des faits absents du texte.
- Si ambigu, mettre dans reflections.
- confidence entre 0 et 1.
"""

# When the LLM returns micro_events: [], the modeling agent skips the graph entirely (no E7 / E55).
_ACTIVITY_VERB_RE = re.compile(
    r"\b(spent|spend|went|go|walked|walk|drove|drive|ate|eat|had|have|met|meet|coding|code|"
    r"worked|work|studied|study|visited|visit|arrived|arrive|bought|buy|called|call|reading|read|"
    r"writing|write|running|run|morning|afternoon|evening|library|station|office|lecture|then)\b",
    re.IGNORECASE,
)


def _text_suggests_concrete_activity(text: str) -> bool:
    return bool(_ACTIVITY_VERB_RE.search(text or ""))


def _likely_pure_mental_without_activity(text: str) -> bool:
    """Avoid synthesizing micro_events for short mood-only lines."""
    t = (text or "").strip()
    if len(t) > 160:
        return False
    tl = t.lower()
    mood = (
        "i feel",
        "i felt",
        "i'm sad",
        "im sad",
        "i am sad",
        "i am anxious",
        "je me sens",
        "j'ai mal",
        "triste",
        "anxious",
        "depressed",
        "stressed",
    )
    if not any(m in tl for m in mood):
        return False
    return not _text_suggests_concrete_activity(t)


def _split_activity_clauses(text: str) -> List[str]:
    t = (text or "").strip()
    if not t:
        return []
    for pattern in (r",\s*then\s+", r";\s*then\s+", r"\.\s+Then\s+", r";\s+"):
        parts = re.split(pattern, t, flags=re.IGNORECASE)
        if len(parts) > 1:
            return [p.strip() for p in parts if p.strip()]
    return [t]


def _guess_type_hint(clause: str) -> str:
    c = (clause or "").lower()
    if any(x in c for x in ("code", "coding", "work", "lecture", "meeting", "stud")):
        return "work"
    if any(x in c for x in ("coffee", "café", "ate", "lunch", "dinner", "breakfast", "drank")):
        return "consumption"
    if any(x in c for x in ("bus", "train", "drove", "walked", "metro", "taxi", "flight")):
        return "transport"
    return "other"


def _places_for_clause(clause: str, entities: List[Any]) -> List[Dict[str, str]]:
    cl = (clause or "").lower()
    out: List[Dict[str, str]] = []
    for e in entities:
        if not isinstance(e, dict):
            continue
        if str(e.get("type", "")).strip().lower() != "place":
            continue
        nm = str(e.get("name", "")).strip()
        if nm and nm.lower() in cl:
            out.append({"name": nm, "role": "physical_location"})
    return out


def _synthesize_micro_events(text: str, entities: List[Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    clauses = _split_activity_clauses(text)
    micro: List[Dict[str, Any]] = []
    for c in clauses:
        c = c.strip()
        if len(c) < 4:
            continue
        idx = len(micro) + 1
        micro.append(
            {
                "id": f"e{idx}",
                "text": c[:280],
                "type_hint": _guess_type_hint(c),
                "time_text": "",
                "participants": [],
                "objects": [],
                "places": _places_for_clause(c, entities),
            }
        )
    links: List[Dict[str, Any]] = []
    for i in range(len(micro) - 1):
        links.append({"from": f"e{i + 1}", "to": f"e{i + 2}", "type": "sequence"})
    return micro, links


class PrepAgent:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        base_url: Optional[str] = None,
        azure_endpoint: Optional[str] = None,
        api_version: Optional[str] = None,
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.azure_endpoint = azure_endpoint
        self.api_version = api_version
        self._client = None

    def _get_client(self):
        if self._client is None:
            key = self.api_key or __import__("os").environ.get("AZURE_OPENAI_API_KEY")
            if not key:
                raise ValueError("AZURE_OPENAI_API_KEY non configuree.")
            if self.azure_endpoint:
                from openai import AzureOpenAI

                self._client = AzureOpenAI(
                    api_key=key,
                    azure_endpoint=self.azure_endpoint.rstrip("/"),
                    api_version=self.api_version or "2024-12-01-preview",
                )
            else:
                from openai import OpenAI

                kwargs = {"api_key": key}
                if self.base_url:
                    kwargs["base_url"] = self.base_url.rstrip("/") + "/"
                self._client = OpenAI(**kwargs)
        return self._client

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "micro_events": [],
            "event_links": [],
            "mental_states": [],
            "expectations": [],
            "habits": [],
            "reflections": [],
            "entities": [],
            "normalized_text": "",
            "confidence": 0.0,
        }

    def run(self, text: str) -> Dict[str, Any]:
        if not text or not text.strip():
            return self._empty_result()
        client = self._get_client()
        prompt = PREP_PROMPT + "\n\nTexte:\n" + text.strip()
        res = client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        content = (res.choices[0].message.content or "").strip()
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
        if match:
            content = match.group(1).strip()
        try:
            data = json.loads(content)
        except Exception:
            data = {}
        out = self._validate(data)
        return self._ensure_micro_events_for_activity_journal(text.strip(), out)

    def _ensure_micro_events_for_activity_journal(self, text: str, out: Dict[str, Any]) -> Dict[str, Any]:
        """If the model left micro_events empty but the line looks like real activities, recover minimally."""
        me = out.get("micro_events")
        if isinstance(me, list) and len(me) > 0:
            return out
        if len(text) < 12:
            return out
        if _likely_pure_mental_without_activity(text):
            return out
        if not _text_suggests_concrete_activity(text):
            return out
        entities = out.get("entities") if isinstance(out.get("entities"), list) else []
        micro, links = _synthesize_micro_events(text, entities)
        if not micro:
            return out
        out["micro_events"] = micro
        existing_links = out.get("event_links") if isinstance(out.get("event_links"), list) else []
        out["event_links"] = list(existing_links) + links
        if not (out.get("normalized_text") or "").strip():
            out["normalized_text"] = text
        try:
            conf = float(out.get("confidence") or 0.0)
        except (TypeError, ValueError):
            conf = 0.0
        out["confidence"] = max(conf, 0.45)
        return out

    def _validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        empty = self._empty_result()
        out: Dict[str, Any] = {}
        for key in empty:
            val = data.get(key)
            if isinstance(empty[key], list):
                out[key] = val if isinstance(val, list) else []
            elif isinstance(empty[key], str):
                out[key] = str(val or "").strip()
            elif isinstance(empty[key], float):
                try:
                    out[key] = float(val or 0.0)
                except (TypeError, ValueError):
                    out[key] = 0.0
            else:
                out[key] = val
        return out
