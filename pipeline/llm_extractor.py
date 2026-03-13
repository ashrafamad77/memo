"""LLM-based entity extraction (OpenAI or Azure AI Foundry)."""
import json
import re
from typing import Any, Dict, List, Optional

from .extractor import ExtractedEntity, ExtractionResult


EXTRACTION_PROMPT = """Tu es un assistant qui extrait des entités et métadonnées d'une entrée de journal personnel.

Pour le texte suivant, extrais toutes les entités pertinentes et retourne un JSON valide avec cette structure exacte :

{
  "entities": [
    {"text": "nom ou expression", "type": "Person|Place|Organization|Event|Date|Concept"}
  ],
  "emotions": ["émotion1", "émotion2"],
  "event_type": "travail|social|santé|familial|loisirs|autre"
}

Règles :
- Person : noms de personnes (Marie, Jean, etc.)
- Place : lieux (Paris, le café, bureau, etc.)
- Organization : entreprises, équipes, groupes
- Event : événements nommés (réunion, déjeuner, conférence)
- Date : dates explicites (aujourd'hui, lundi 15, 2024-03-15)
- Concept : thèmes, sujets, activités, émotions en tant que concepts
- emotions : sentiments ressentis (joie, stress, nostalgie, etc.)
- event_type : catégorie globale (une seule valeur)

Retourne UNIQUEMENT le JSON, sans texte avant ou après.
"""


class LLMExtractor:
    """Extract entities, emotions, and event type using OpenAI GPT."""

    VALID_TYPES = ("Person", "Place", "Organization", "Concept", "Event", "Date")

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
            try:
                key = self.api_key or __import__("os").environ.get("AZURE_OPENAI_API_KEY") or __import__("os").environ.get("OPENAI_API_KEY")
                if not key:
                    raise ValueError("Aucune clé LLM configurée (AZURE_OPENAI_API_KEY ou OPENAI_API_KEY).")

                # Azure OpenAI (AzureOpenAI client) si endpoint fourni
                if self.azure_endpoint:
                    from openai import AzureOpenAI

                    self._client = AzureOpenAI(
                        api_key=key,
                        azure_endpoint=self.azure_endpoint.rstrip("/"),
                        api_version=self.api_version or "2024-12-01-preview",
                    )
                else:
                    # OpenAI public (OpenAI client)
                    from openai import OpenAI

                    kwargs = {"api_key": key}
                    if self.base_url:
                        kwargs["base_url"] = self.base_url.rstrip("/") + "/"
                    self._client = OpenAI(**kwargs)
            except ImportError as e:
                raise ImportError("Installez openai: pip install openai") from e
        return self._client

    def extract(self, text: str) -> ExtractionResult:
        """Extract entities from journal text using LLM."""
        if not text or not text.strip():
            return ExtractionResult(entities=[], raw_text=text)

        client = self._get_client()
        full_prompt = EXTRACTION_PROMPT + "\n\nTexte :\n" + text.strip()

        # Let errors surface instead of silently returning no entities
        response = client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": full_prompt}],
            temperature=0.2,
        )
        content = response.choices[0].message.content or "{}"

        data = self._parse_response(content, text)
        entities = self._to_entities(data, text)
        return ExtractionResult(entities=entities, raw_text=text.strip())

    def _parse_response(self, content: str, original_text: str) -> Dict[str, Any]:
        """Parse LLM response into structured data."""
        content = content.strip()
        # Extract JSON from markdown code block if present
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
        if match:
            content = match.group(1).strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"entities": [], "emotions": [], "event_type": "autre"}

    def _to_entities(
        self,
        data: Dict[str, Any],
        original_text: str,
    ) -> List[ExtractedEntity]:
        """Convert parsed data to ExtractedEntity list."""
        entities: List[ExtractedEntity] = []
        seen: set = set()

        for item in data.get("entities", []):
            if not isinstance(item, dict):
                continue
            text_val = item.get("text", "").strip()
            type_val = item.get("type", "Concept")
            if not text_val:
                continue
            type_val = type_val if type_val in self.VALID_TYPES else "Concept"
            key = (text_val.lower(), type_val)
            if key in seen:
                continue
            seen.add(key)
            entities.append(ExtractedEntity(
                text=text_val,
                label=type_val,
                start_char=0,
                end_char=len(text_val),
            ))

        # Add emotions as Concept entities
        for emotion in data.get("emotions", []):
            if isinstance(emotion, str) and emotion.strip():
                e = emotion.strip().lower()
                key = (e, "Concept")
                if key not in seen:
                    seen.add(key)
                    entities.append(ExtractedEntity(
                        text=emotion.strip(),
                        label="Concept",
                        start_char=0,
                        end_char=len(emotion),
                    ))

        # Add event_type as Concept if meaningful
        et = data.get("event_type")
        if isinstance(et, str) and et.strip() and et.lower() != "autre":
            key = (et.strip().lower(), "Concept")
            if key not in seen:
                seen.add(key)
                entities.append(ExtractedEntity(
                    text=et.strip(),
                    label="Concept",
                    start_char=0,
                    end_char=len(et),
                ))

        return entities
