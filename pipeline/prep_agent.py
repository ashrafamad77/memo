"""Prep Agent v2: universal semantic decomposition before CIDOC modeling."""
import json
import re
from typing import Any, Dict, List, Optional


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
- micro_events: chaque ACTIVITE concrete (temps et/ou lieu). Ordonne-les chronologiquement. id commence a e1.
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
        return self._validate(data)

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
