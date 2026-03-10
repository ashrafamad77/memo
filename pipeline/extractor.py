"""Entity extraction: spaCy NER avec fallback regex (sans compilation)."""
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

# spaCy optionnel (nécessite C++ Build Tools sur Windows)
try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False


@dataclass
class ExtractedEntity:
    """An extracted named entity."""
    text: str
    label: str  # Person, Place, Date, etc.
    start_char: int
    end_char: int


@dataclass
class ExtractionResult:
    """Result of entity extraction from a journal entry."""
    entities: List[ExtractedEntity] = field(default_factory=list)
    raw_text: str = ""
    timestamp: Optional[datetime] = None


class SimpleExtractor:
    """Fallback: extraction par regex + heuristiques (stdlib only, pas de compilation)."""
    
    # Patterns pour dates (FR et EN)
    DATE_PATTERNS = [
        r"\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}",  # 15/03/2024, 15-03-24
        r"\d{1,2}\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}",
        r"(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}",
        r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}",
        r"\d{4}-\d{2}-\d{2}",  # ISO
        r"(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+\d{1,2}",  # lundi 15
    ]
    
    # Mots capitalisés consécutifs = probablement Person ou Lieu
    CAPITALIZED = re.compile(r"\b([A-ZÀÂÄÉÈÊËÏÎÔÙÛÜÇ][a-zàâäéèêëïîôùûüç]+(?:\s+[A-ZÀÂÄÉÈÊËÏÎÔÙÛÜÇ][a-zàâäéèêëïîôùûüç]+)*)\b")
    
    def extract(self, text: str) -> ExtractionResult:
        entities = []
        seen = set()
        
        # Dates
        for pat in self.DATE_PATTERNS:
            for m in re.finditer(pat, text, re.IGNORECASE):
                t = m.group().strip()
                if t not in seen:
                    seen.add(t)
                    entities.append(ExtractedEntity(t, "Date", m.start(), m.end()))
        
        # Noms propres (mots capitalisés, 2+ caractères)
        for m in self.CAPITALIZED.finditer(text):
            t = m.group().strip()
            if len(t) < 2 or t.lower() in ("je", "aujourd'hui", "hier", "demain"):
                continue
            if t not in seen:
                seen.add(t)
                # Heuristique: souvent Person, parfois Place
                entities.append(ExtractedEntity(t, "Person", m.start(), m.end()))
        
        return ExtractionResult(entities=entities, raw_text=text.strip())


class EntityExtractor:
    """Extract entities via spaCy si dispo, sinon SimpleExtractor."""
    
    LABEL_TO_TYPE = {
        "PERSON": "Person", "ORG": "Organization", "GPE": "Place", "LOC": "Place",
        "DATE": "Date", "EVENT": "Event", "WORK_OF_ART": "Concept", "PRODUCT": "Concept",
        "FAC": "Place", "NORP": "Concept", "LAW": "Concept", "LANGUAGE": "Concept",
        "MISC": "Concept",
    }
    
    def __init__(self, model_name: str = "fr_core_news_sm"):
        self.model_name = model_name
        self._backend = None
        
        if SPACY_AVAILABLE:
            try:
                self._backend = spacy.load(model_name)
            except OSError:
                try:
                    self._backend = spacy.load("en_core_web_sm")
                except OSError:
                    pass
        
        if self._backend is None:
            self._backend = SimpleExtractor()
    
    def extract(self, text: str) -> ExtractionResult:
        if isinstance(self._backend, SimpleExtractor):
            return self._backend.extract(text)
        
        doc = self._backend(text)
        entities = []
        seen = set()
        for ent in doc.ents:
                node_type = self.LABEL_TO_TYPE.get(ent.label_, "Concept")
                key = (ent.text.strip(), node_type)
                if key in seen:
                    continue
                seen.add(key)
                entities.append(ExtractedEntity(
                    text=ent.text.strip(),
                    label=node_type,
                    start_char=ent.start_char,
                    end_char=ent.end_char,
                ))
        return ExtractionResult(entities=entities, raw_text=text.strip())
    
    def entity_to_node_type(self, label: str) -> str:
        return self.LABEL_TO_TYPE.get(label, label if label in ("Person", "Place", "Organization", "Concept", "Event", "Date") else "Concept")
