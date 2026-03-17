"""LLM-based entity extraction (OpenAI or Azure AI Foundry)."""
import json
import re
from typing import Any, Dict, List, Optional

from .extractor import ExtractedEntity, ExtractedRelation, ExtractionResult


EXTRACTION_PROMPT = """Tu es un assistant qui extrait des entités (mentions), des relations (triplets) et des métadonnées d'une entrée de journal personnel.

Pour le texte suivant, extrais :
1) Les entités (Person, Place, Concept, Event, Date)
2) Les relations entre elles au format sujet-prédicat-objet

Retourne un JSON valide avec cette structure exacte :

{
  "entities": [
    {"text": "nom ou expression", "type": "Person|Place|Organization|Event|Date|Concept"}
  ],
  "relations": [
    {"subject": "Marie", "predicate": "MET_AT", "object": "Paris", "sentiment": 0.8},
    {"subject": "Marie", "predicate": "DISCUSSED", "object": "projet", "sentiment": 0.6}
  ],
  "metadata": {
    "emotions": ["joie", "stress"],
    "event_type": "social",
    "event_time_text": "ce matin",
    "event_time_iso": "2026-03-13T12:30:00Z",
    "event_time_confidence": 0.6
  }
}

Règles entités :
- Person : noms de personnes. Place : lieux. Concept : thèmes, sujets.
- Event : type d'événement (déjeuner, réunion, etc.). Date : dates explicites.
⚠️ Les entités doivent être des mentions dans le texte (pas d'inférences).

Règles relations (triplets) :
- subject et object doivent être des entités ou le nom de l'auteur.
- predicate : LUNCHED_WITH, MET_AT, DISCUSSED, WORKED_ON, OCCURRED_AT, HAS_TOPIC, etc.
- sentiment : 0 (négatif) à 1 (positif), 0.5 = neutre.
- Extrais 2 à 6 relations. Quand le texte dit "je", "j'ai", "nous" (auteur inclus), le SUJET doit être l'auteur.

{user_context}

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
        user_name: Optional[str] = None,
    ):
        self.api_key = api_key
        self.model = model
        self.user_name = (user_name or "").strip()
        self.base_url = base_url
        self.azure_endpoint = azure_endpoint
        self.api_version = api_version
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                key = self.api_key or __import__("os").environ.get("AZURE_OPENAI_API_KEY")
                if not key:
                    raise ValueError("AZURE_OPENAI_API_KEY non configurée.")

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
        user_context = ""
        if self.user_name:
            user_context = f"L'auteur du journal s'appelle {self.user_name}. Quand le texte dit 'je', 'j'ai', 'on a', 'nous', c'est {self.user_name} qui agit. Utilise TOUJOURS exactement '{self.user_name}' comme sujet (jamais 'auteur', 'je', 'moi'). Exemple: subject='{self.user_name}', predicate='LUNCHED_WITH', object='Marie'."
        else:
            user_context = "Tu ne connais pas le nom de l'auteur ; extrais les relations à partir des entités mentionnées."
        prompt = EXTRACTION_PROMPT.replace("{user_context}", user_context)
        full_prompt = prompt + "\n\nTexte :\n" + text.strip()

        # Let errors surface instead of silently returning no entities
        response = client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": full_prompt}],
            temperature=0.2,
        )
        content = response.choices[0].message.content or "{}"

        data = self._parse_response(content, text)
        entities = self._to_entities(data, text)
        relations = self._to_relations(data, text)
        if self.user_name:
            relations = self._normalize_user_in_relations(relations)
        metadata = self._to_metadata(data)
        return ExtractionResult(entities=entities, relations=relations, metadata=metadata, raw_text=text.strip())

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
            return {"entities": [], "relations": [], "metadata": {}}

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

    def _to_relations(
        self,
        data: Dict[str, Any],
        original_text: str,
    ) -> List[ExtractedRelation]:
        """Convert parsed relations into ExtractedRelation list."""
        relations: List[ExtractedRelation] = []
        seen: set = set()

        for item in data.get("relations", []):
            if not isinstance(item, dict):
                continue
            subj = (item.get("subject") or "").strip()
            pred = (item.get("predicate") or "RELATED_TO").strip().upper().replace(" ", "_")
            obj = (item.get("object") or "").strip()
            if not subj or not obj:
                continue
            sent = float(item.get("sentiment", 0.5))
            sent = max(0.0, min(1.0, sent))
            key = (subj.lower(), pred, obj.lower())
            if key in seen:
                continue
            seen.add(key)
            relations.append(ExtractedRelation(
                subject=subj,
                predicate=pred,
                obj=obj,
                sentiment=sent,
            ))
        return relations

    def _to_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        meta = data.get("metadata", {})
        return meta if isinstance(meta, dict) else {}

    def _normalize_user_in_relations(
        self, relations: List[ExtractedRelation]
    ) -> List[ExtractedRelation]:
        """Replace 'auteur', 'l'auteur', 'je', etc. with the actual user_name."""
        def _is_author_ref(s: str) -> bool:
            s = s.strip().lower()
            for c in "''\u2019":  # apostrophe variants
                s = s.replace(c, " ")
            s = s.replace("l ", "").replace("le ", "").replace("la ", "").strip()
            return s in {"auteur", "author", "je", "moi", "me", "nous", "us"}

        out = []
        for r in relations:
            subj = r.subject.strip()
            obj = r.obj.strip()
            if _is_author_ref(subj):
                subj = self.user_name
            if _is_author_ref(obj):
                obj = self.user_name
            out.append(ExtractedRelation(
                subject=subj, predicate=r.predicate, obj=obj, sentiment=r.sentiment
            ))
        return out
