"""Modeling Agent: maps Prep Agent output to a CIDOC CRM graph specification.

Takes the universal decomposition from PrepAgent and produces a list of
nodes and edges using CIDOC CRM classes and properties. The graph writer
then executes this spec generically.
"""
import json
import re
from typing import Any, Dict, List, Optional


CIDOC_VOCAB = """
CIDOC CRM classes disponibles:
- E5_Event: evenement observable
- E7_Activity: activite realisee par un acteur (sous-classe de E5)
- E10_Transfer_of_Custody: transfert de garde d'un objet (sous-classe de E7)
- E13_Attribute_Assignment: attribution d'un attribut (emotion, attente, evaluation)
- E21_Person: personne
- E22_Human_Made_Object: objet fabrique (livre, cadeau, outil)
- E28_Conceptual_Object: concept abstrait (habitude, idee, theme)
- E39_Actor: acteur (personne ou groupe agissant)
- E52_Time_Span: intervalle temporel (jour, heure)
- E53_Place: lieu
- E55_Type: type/categorie (utilise pour typer les autres noeuds)
- E73_Information_Object: objet informationnel (journal, note, reflexion)
- E74_Group: organisation, universite, groupe
- E89_Propositional_Object: proposition, condition, regle

CIDOC CRM proprietes disponibles (edges):
- P2_has_type: noeud -> E55_Type
- P4_has_time_span: activite/evenement -> E52_Time_Span
- P7_took_place_at: activite -> E53_Place (lieu physique reel)
- P14_carried_out_by: activite -> acteur
- P14i_performed: acteur -> activite (inverse)
- P15_was_influenced_by: activite -> entite qui l'influence
- P17_was_motivated_by: activite -> entite qui la motive
- P28_custody_surrendered_by: E10 -> acteur qui donne
- P29_custody_received_by: E10 -> acteur qui recoit
- P30_transferred_custody_of: E10 -> objet transfere
- P67_refers_to: E73/E89 -> entite referencee
- P120_occurs_before: activite -> activite qui suit
- P140_assigned_attribute_to: E13 -> entite a laquelle l'attribut est assigne
- P141_assigned: E13 -> valeur/concept assigne
"""

MODELING_PROMPT = """Tu es un agent de modelisation CIDOC CRM.
Tu recois une decomposition semantique (prep) et tu produis une specification de graphe.

{cidoc_vocab}

Types existants dans la base (reutilise-les si pertinent):
{existing_types}

Utilisateur (auteur du journal): {user_name}
IMPORTANT: L'utilisateur s'appelle EXACTEMENT "{user_name}". Utilise ce nom exact pour le noeud E21_Person de l'utilisateur. Ne cree PAS de noeud separe "Utilisateur" — utilise "{user_name}".

Retourne UNIQUEMENT un JSON valide avec cette structure:

{{
  "nodes": [
    {{
      "id": "n1",
      "label": "E7_Activity",
      "name": "cours",
      "types": ["SocialActivity"],
      "properties": {{"event_time_text": "14h", "event_time_iso": "2026-03-13T14:00:00Z"}}
    }}
  ],
  "edges": [
    {{
      "from": "n1",
      "to": "n2",
      "property": "P14_carried_out_by",
      "properties": {{}}
    }}
  ]
}}

Regles de modelisation:
1. Chaque micro_event du prep devient un noeud E7_Activity (ou E10_Transfer_of_Custody si c'est un transfert).
2. L'utilisateur ({user_name}) est TOUJOURS un noeud E21_Person/E39_Actor avec type "User". Il est P14_carried_out_by pour ses activites.
3. Les personnes mentionnees deviennent des noeuds E21_Person.
4. Les lieux physiques deviennent E53_Place. Si le role est "physical_location", utilise P7_took_place_at. Si "remote_context", utilise P67_refers_to depuis le journal entry.
5. Les organisations deviennent E74_Group.
6. Les objets physiques (livre, cadeau) deviennent E22_Human_Made_Object.
7. Les habits deviennent E28_Conceptual_Object avec type "Habit".
8. Les mental_states deviennent E13_Attribute_Assignment: P140_assigned_attribute_to pointe vers la personne affectee, P141_assigned pointe vers un E55_Type decrivant l'etat.
9. Les expectations deviennent E13_Attribute_Assignment avec type specifique (ex: "ReturnExpectation").
10. Les reflections deviennent E7_Activity avec type "Reflection", liees par P17_was_motivated_by a l'evenement source, et P67_refers_to vers les sujets de reflexion.
11. Les event_links "sequence" deviennent P120_occurs_before. "causes"/"impacts"/"influences" deviennent P15_was_influenced_by.
12. Les propositions (influenced_by_propositions dans habits) deviennent E89_Propositional_Object.
13. Le journal entry principal est un E73_Information_Object qui P67_refers_to toutes les entites mentionnees.
14. Utilise P2_has_type pour typer les noeuds avec des E55_Type. Les noms de types sont en CamelCase (ex: WakeUp, BookLending, EmotionalPain).
15. P4_has_time_span: NE l'applique qu'aux noeuds E7_Activity/E10_Transfer_of_Custody/E5_Event. JAMAIS aux personnes, lieux, concepts, organisations.
16. N'invente RIEN qui n'est pas dans la decomposition prep.
17. Chaque noeud a un id unique (n1, n2, ...). Utilise ces ids dans les edges.
18. Pour E10_Transfer_of_Custody: P28_custody_surrendered_by (qui donne), P29_custody_received_by (qui recoit), P30_transferred_custody_of (l'objet).
19. Quand une attente est violee (violated_by), cree un noeud E7_Activity type "NonOccurrence" ou "FailureToReturn" pour l'evenement qui n'a pas eu lieu.
20. IMPORTANT: Le journal entry (E73) est cree automatiquement par le systeme. NE le cree PAS comme noeud. Le systeme le lie a tous les noeuds via P67.
21. Les lieux mentionnes dans la prep avec role "remote_context" deviennent des E53_Place mais NE sont PAS lies par P7_took_place_at. Seul le role "physical_location" justifie P7.
22. Les organisations deviennent E74_Group. Ne les utilise PAS comme lieu (pas de P7_took_place_at vers une organisation).
23. Quand la prep contient des "conditions du jour" (pas de cours, pas de reunion, etc.), modele-les comme E89_Propositional_Object (pas E7_Activity) avec P15_was_influenced_by vers l'evenement principal.
24. Quand un evenement (ex: wake up) est influence par une condition du jour, cree: event -[P15_was_influenced_by]-> condition.
25. Les places mentionnees dans les entities du prep doivent TOUTES devenir des noeuds E53_Place dans le graphe.
"""


class ModelingAgent:
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

    def run(
        self,
        prep: Dict[str, Any],
        user_name: str = "",
        existing_types: Optional[List[str]] = None,
        day_bucket: str = "",
    ) -> Dict[str, Any]:
        if not prep or not prep.get("micro_events"):
            return {"nodes": [], "edges": []}

        client = self._get_client()
        types_str = ", ".join(existing_types) if existing_types else "(aucun)"
        prompt = MODELING_PROMPT.format(
            cidoc_vocab=CIDOC_VOCAB,
            existing_types=types_str,
            user_name=user_name or "utilisateur",
        )
        prep_json = json.dumps(prep, ensure_ascii=False, indent=2)
        full = prompt + f"\n\nJour: {day_bucket}\n\nDecomposition prep:\n{prep_json}"

        res = client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": full}],
            temperature=0.1,
        )
        content = (res.choices[0].message.content or "").strip()
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
        if match:
            content = match.group(1).strip()
        try:
            data = json.loads(content)
        except Exception:
            return {"nodes": [], "edges": []}

        nodes = data.get("nodes", [])
        edges = data.get("edges", [])
        if not isinstance(nodes, list):
            nodes = []
        if not isinstance(edges, list):
            edges = []
        return {"nodes": nodes, "edges": edges}
