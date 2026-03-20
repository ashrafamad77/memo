"""Prep Agent v1: externalize implicit narrative into explicit structure."""
import json
import re
from typing import Any, Dict, Optional


PREP_PROMPT = """Tu es un agent de preparation semantique.
Tu transformes un texte de journal brut en representation explicite AVANT extraction.

Retourne UNIQUEMENT un JSON valide avec cette structure:
{
  "facts_today": ["faits observes aujourd'hui, concrets"],
  "habits": ["regles habituelles/recurrentes"],
  "causal_rules": ["regles causales explicites style 'X -> Y'"],
  "reflections": ["meta-commentaires, ressenti, evaluation"],
  "entities_hint": ["noms, lieux, organisations, concepts saillants"],
  "normalized_text": "reformulation claire et explicite en langage naturel",
  "confidence": 0.0
}

Regles:
- Ne pas inventer des faits absents du texte.
- Separer clairement aujourd'hui vs habitudes generales.
- Si ambigu, conserver l'ambiguite dans reflections.
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

    def run(self, text: str) -> Dict[str, Any]:
        if not text or not text.strip():
            return {
                "facts_today": [],
                "habits": [],
                "causal_rules": [],
                "reflections": [],
                "entities_hint": [],
                "normalized_text": "",
                "confidence": 0.0,
            }
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
        out = {
            "facts_today": data.get("facts_today") if isinstance(data.get("facts_today"), list) else [],
            "habits": data.get("habits") if isinstance(data.get("habits"), list) else [],
            "causal_rules": data.get("causal_rules") if isinstance(data.get("causal_rules"), list) else [],
            "reflections": data.get("reflections") if isinstance(data.get("reflections"), list) else [],
            "entities_hint": data.get("entities_hint") if isinstance(data.get("entities_hint"), list) else [],
            "normalized_text": str(data.get("normalized_text") or "").strip(),
            "confidence": float(data.get("confidence", 0.0) or 0.0),
        }
        return out
