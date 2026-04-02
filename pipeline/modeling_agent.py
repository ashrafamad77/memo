"""Modeling Agent: maps Prep Agent output to a CIDOC CRM graph specification.

Takes the universal decomposition from PrepAgent and produces a list of
nodes and edges using CIDOC CRM classes and properties. The graph writer
then executes this spec generically.
"""
import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple


CIDOC_VOCAB = """
CIDOC CRM classes disponibles:
- E5_Event: evenement observable
- E7_Activity: action ou processus actif (sous-classe de E5). Pas pour un etat vecu passif (faim, fatigue, douleur ressentie) — utiliser E13.
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
- P15_was_influenced_by: activite ou E13 -> entite qui influence (ex. E13 -P15-> E7 declencheur)
- P17_was_motivated_by: activite -> entite qui la motive
- P28_custody_surrendered_by: E10 -> acteur qui donne
- P29_custody_received_by: E10 -> acteur qui recoit
- P30_transferred_custody_of: E10 -> objet transfere
- P67_refers_to: E73/E89 -> entite referencee
- P120_occurs_before: activite -> activite qui suit
- P140_assigned_attribute_to: E13 -> entite a laquelle l'attribut est assigne
- P141_assigned: E13 -> valeur/concept assigne
- P129_is_about: activite/evenement -> sujet/objet concerne
"""

MODELING_PROMPT = """Tu es un agent de modelisation CIDOC CRM.
Tu recois une decomposition semantique (prep) et tu produis une specification de graphe.

{cidoc_vocab}

Types existants dans la base (reutilise-les si pertinent):
{existing_types}

You are a professional taxonomist. When choosing an E55_Type, follow this priority order:
1. **Preferred vocabulary** (reuse exactly if semantically appropriate):
{preferred_types}

2. **Existing types in the graph** (listed above under "Types existants") — reuse the exact string.
3. **New type** — only if nothing in 1 or 2 fits. Use a standard English noun in CamelCase that could be a Wikipedia article title for the *concept* (e.g. `Visit`, `Lecture`, `Programming`). Never coin compound action phrases (`Urbanvisit`, `DeepCodingSession`, `MorningStayAtPlace`).

**E55_Type on an E7_Activity classifies the ACTIVITY TYPE, not the specific event.**
The event's name (e.g. `MorningStayAtVictoria`) already describes the instance. Its *type* should be the abstract concept: `Visit`, `WorkSession`, `Programming`, etc.
Never assign a geographic/place-category term (Neighbourhood, District, Station, Street, Building, Park, Area, Zone, Quarter, Borough) as the type of an E7_Activity. If the activity is going to or staying at a place, use `Visit`; if working there, use `WorkSession`.

Never use an E55_Type **name** that only repeats a CIDOC role or generic class: avoid Place, Person, Activity, Event, Object, Concept, Organization, Project, Location, Group, Actor, State, Type, Other, Unknown, Misc. Those words describe **node labels**, not taxonomy.

**Activities (E7_Activity):** always assign a type — pick the closest match from the preferred vocabulary. If nothing specific fits, use `WorkSession` or `Visit` rather than leaving types empty.
**Places (E53_Place):** if you cannot determine the specific place type, leave the types array **empty** — never use `Other` as a fallback.

Utilisateur (auteur du journal): {user_name}
IMPORTANT: L'utilisateur s'appelle EXACTEMENT "{user_name}". Utilise ce nom exact pour le noeud E21_Person de l'utilisateur. Ne cree PAS de noeud separe "Utilisateur" — utilise "{user_name}".

Chaque libelle E55_Type (types sur un noeud OU noeud dedie label E55_Type) doit inclure **context_category** pour le controle d'autorite Wikidata:
- **Activity** (E7, E10, E5), **Place** (lieu / mention spatiale), **Person**, **Organization**, **Object** (E22), **Concept** (E28, E89), **State** (emotion / sensation / attente via E13), **Transfer** (E10), **Event** (autre), **Other** si doute.

Tu peux donner **types** soit comme chaine (ancien style), soit comme objet: {{"name": "DeepWork", "context_category": "Activity"}}.
Pour un noeud **E55_Type** dedie, ajoute en plus du **name**: **"context_category": "State"** (au meme niveau que name, pas seulement dans properties).

Retourne UNIQUEMENT un JSON valide avec cette structure:

{{
  "nodes": [
    {{
      "id": "n1",
      "label": "E7_Activity",
      "name": "cours",
      "types": [{{"name": "SocialActivity", "context_category": "Activity"}}],
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
1. Chaque micro_event du prep devient un noeud E7_Activity (ou E10_Transfer_of_Custody si c'est un transfert), **sauf** si le prep ne contient que la trace de cet episode dans **mental_states** (etat sans micro_event dedie) — alors il n'y a pas d'E7 pour cet episode.
1b. **E7 vs E13 (distinction CIDOC):** E7 = action / changement dans le monde (deplacement, achat, arrivee, communication). **Jamais** d'E7 pour un **etat ou une sensation** (faim, fatigue, peur ressentie, joie comme humeur, douleur vecue). Ceux-ci sont **uniquement** E13_Attribute_Assignment. Si le prep a a la fois un micro_event redondant du type "ressentir la faim / feel hungry" **et** un mental_state equivalent, **ne cree pas** l'E7 : garde seulement l'E13 avec le triangle P140/P141/P15 decrit en 8c.
2. L'utilisateur ({user_name}) est TOUJOURS un noeud E21_Person/E39_Actor avec type "User". Il est P14_carried_out_by pour ses activites.
3. Les personnes mentionnees deviennent des noeuds E21_Person.
4. Les lieux physiques deviennent E53_Place. Si le role est "physical_location", utilise P7_took_place_at. Si "remote_context", utilise P67_refers_to depuis le journal entry.
5. Les organisations deviennent E74_Group.
6. Les objets physiques (livre, cadeau) deviennent E22_Human_Made_Object.
7. Les habits deviennent E28_Conceptual_Object avec type "Habit".
8. Les mental_states deviennent E13_Attribute_Assignment: P140_assigned_attribute_to pointe vers la personne affectee, P141_assigned pointe vers un E55_Type decrivant l'etat.
8b. OBLIGATOIRE pour chaque E13: cree un noeud E55_Type dedie (id dedie, label E55_Type, name en CamelCase semantique: Hunger, Fatigue, Joy, Stress, Expectation, etc.) et un edge P141_assigned de l'E13 vers ce noeud. Tu peux aussi mettre ce meme nom dans le tableau "types" de l'E13 pour coherence. N'utilise JAMAIS de type generique du genre "AssignedState" ou "State" — le nom doit etre la notion vecue (ex. faim/affame -> Hunger).
8c. **Triangle E13 complet:** pour chaque etat mental/sensation, l'E13 doit avoir **P140_assigned_attribute_to** vers la personne concernee (souvent l'auteur), **P141_assigned** vers l'E55_Type (ex. Hunger), et quand le prep indique une cause (**caused_by** / lien causal), **P15_was_influenced_by** de l'E13 vers le **micro_event E7** declencheur (ex. arrivee au bureau) — pas l'inverse. Ne te contente pas d'une seule relation P15 sans P140/P141.
9. Les expectations deviennent E13_Attribute_Assignment avec type specifique (ex: "ReturnExpectation").
10. Les reflections deviennent E7_Activity avec type "Reflection", liees par P17_was_motivated_by a l'evenement source, et P67_refers_to vers les sujets de reflexion.
11. Les event_links "sequence" deviennent P120_occurs_before. "causes"/"impacts"/"influences" deviennent P15_was_influenced_by.
11b. Pour un evenement de non-retour/vol/perte d'objet, lie l'evenement a l'objet physique via P129_is_about (pas P15).
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


def _e13_inferred_labels(nodes: List[Dict[str, Any]]) -> Set[str]:
    from .graph_writer import GraphWriter

    out: Set[str] = set()
    for n in nodes:
        if not isinstance(n, dict) or n.get("label") != "E13_Attribute_Assignment":
            continue
        lab = GraphWriter._infer_e13_p141_type(str(n.get("name") or ""), "")
        if lab:
            out.add(lab)
    return out


_LAZY_CIDOC_E55_LOWER = frozenset(
    {
        "place",
        "person",
        "activity",
        "event",
        "object",
        "concept",
        "organization",
        "organisation",
        "transfer",
        "state",
        "type",
        "location",
        "group",
        "actor",
        "project",
        "other",
        "unknown",
        "misc",
        "miscellaneous",
    }
)


def _is_lazy_e55_name(name: str) -> bool:
    return (name or "").strip().lower() in _LAZY_CIDOC_E55_LOWER


def _camel_from_human_label(s: str) -> str:
    parts = re.findall(r"[A-Za-zÀ-ÿ]+", s or "", flags=re.UNICODE)
    if not parts:
        return ""
    out: List[str] = []
    for p in parts:
        q = p.strip("'")
        if not q:
            continue
        out.append(q[0].upper() + q[1:].lower() if len(q) > 1 else q.upper())
    return "".join(out)


def _fallback_e55_for_cidoc_label(label: str) -> str:
    return {
        "E53_Place": "NamedPlace",
        "E7_Activity": "NarratedActivity",
        "E21_Person": "NamedPerson",
        "E74_Group": "NamedOrganization",
        "E22_Human_Made_Object": "NamedObject",
        "E28_Conceptual_Object": "NamedConcept",
        "E5_Event": "NarratedEvent",
        "E10_Transfer_of_Custody": "CustodyEpisode",
        "E89_Propositional_Object": "Proposition",
        "E13_Attribute_Assignment": "LivedState",
    }.get(label or "", "SemanticType")


def _type_from_node_context(human_name: str, cidoc_label: str) -> str:
    slug = _camel_from_human_label(human_name)
    if slug and not _is_lazy_e55_name(slug):
        return slug
    return _fallback_e55_for_cidoc_label(cidoc_label)


def _spec_type_name_part(t: Any) -> str:
    if isinstance(t, dict):
        return str(t.get("name") or "").strip()
    return str(t or "").strip()


def _with_spec_type_name(t: Any, new_name: str) -> Any:
    if isinstance(t, dict):
        d = dict(t)
        d["name"] = new_name
        return d
    return new_name


def _sanitize_lazy_e55_types(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> None:
    """Replace CIDOC-echo E55 names with names derived from the typed node (Wikidata / UX)."""
    id_to_node: Dict[str, Dict[str, Any]] = {}
    for n in nodes:
        if isinstance(n, dict) and str(n.get("id") or ""):
            id_to_node[str(n["id"])] = n

    def _preds(prop: str, to_id: str) -> List[str]:
        out_ids: List[str] = []
        for e in edges:
            if not isinstance(e, dict) or e.get("property") != prop:
                continue
            if str(e.get("to") or "") != to_id:
                continue
            fid = str(e.get("from") or "")
            if fid:
                out_ids.append(fid)
        return out_ids

    for n in nodes:
        if not isinstance(n, dict) or n.get("label") == "E55_Type":
            continue
        lab = str(n.get("label") or "")
        nm = str(n.get("name") or "")
        types = n.get("types")
        if not isinstance(types, list):
            continue
        for i, t in enumerate(types):
            tnm = _spec_type_name_part(t)
            if not _is_lazy_e55_name(tnm):
                continue
            rep = _type_from_node_context(nm, lab)
            n["types"][i] = _with_spec_type_name(t, rep)

    for n in nodes:
        if not isinstance(n, dict) or n.get("label") != "E55_Type":
            continue
        nid = str(n.get("id") or "")
        tnm = str(n.get("name") or "").strip()
        if not nid or not _is_lazy_e55_name(tnm):
            continue
        ctx_name = ""
        ctx_label = ""
        for fid in _preds("P2_has_type", nid) + _preds("P141_assigned", nid):
            fn = id_to_node.get(fid)
            if not fn:
                continue
            ctx_name = str(fn.get("name") or "")
            ctx_label = str(fn.get("label") or "")
            if ctx_name.strip():
                break
        rep = _type_from_node_context(ctx_name, ctx_label or "E55_Type")
        n["name"] = rep


def _state_sensation_e7_name(name: str) -> bool:
    n = name.lower()
    return any(
        m in n
        for m in (
            "ressent",
            "se sentir",
            "felt ",
            "feeling ",
            " feel ",
            "avoir faim",
            "had hunger",
            "was hungry",
        )
    )


def _prune_redundant_state_e7(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> None:
    """Remove E7 activities that duplicate an E13 state (e.g. 'ressentir la faim' + Hunger E13)."""
    from .graph_writer import GraphWriter

    e13_labels = _e13_inferred_labels(nodes)
    if not e13_labels:
        return

    remove_ids: Set[str] = set()
    for n in nodes:
        if not isinstance(n, dict) or n.get("label") != "E7_Activity":
            continue
        name = str(n.get("name") or "")
        if not _state_sensation_e7_name(name):
            continue
        inferred = GraphWriter._infer_e13_p141_type(name, "")
        if inferred and inferred in e13_labels:
            nid = str(n.get("id") or "")
            if nid:
                remove_ids.add(nid)

    if not remove_ids:
        return

    p120_out: Dict[str, List[str]] = {}
    p120_in: Dict[str, List[str]] = {}
    for e in edges:
        if not isinstance(e, dict) or e.get("property") != "P120_occurs_before":
            continue
        f, t = str(e.get("from", "")), str(e.get("to", ""))
        if not f or not t:
            continue
        p120_out.setdefault(f, []).append(t)
        p120_in.setdefault(t, []).append(f)

    bridge: Set[Tuple[str, str]] = set()
    for rid in remove_ids:
        for pred in p120_in.get(rid, []):
            if pred in remove_ids:
                continue
            for succ in p120_out.get(rid, []):
                if succ in remove_ids:
                    continue
                bridge.add((pred, succ))

    filtered: List[Dict[str, Any]] = []
    for e in edges:
        if not isinstance(e, dict):
            continue
        f, t = str(e.get("from", "")), str(e.get("to", ""))
        if f in remove_ids or t in remove_ids:
            continue
        filtered.append(e)

    existing_p120 = {
        (str(e.get("from", "")), str(e.get("to", "")))
        for e in filtered
        if isinstance(e, dict) and e.get("property") == "P120_occurs_before"
    }
    for f, s in bridge:
        if (f, s) not in existing_p120:
            filtered.append({"from": f, "to": s, "property": "P120_occurs_before", "properties": {}})
            existing_p120.add((f, s))

    edges[:] = filtered
    nodes[:] = [n for n in nodes if isinstance(n, dict) and str(n.get("id", "")) not in remove_ids]


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

        from .type_vocab import seed_type_names

        client = self._get_client()
        types_str = ", ".join(existing_types) if existing_types else "(aucun)"
        seed_names = seed_type_names()
        # Format seed vocab as a compact two-column list for readability
        cols = 4
        rows = [seed_names[i:i + cols] for i in range(0, len(seed_names), cols)]
        preferred_str = "\n".join("   " + ", ".join(row) for row in rows)
        prompt = MODELING_PROMPT.format(
            cidoc_vocab=CIDOC_VOCAB,
            existing_types=types_str,
            user_name=user_name or "utilisateur",
            preferred_types=preferred_str,
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
        _sanitize_lazy_e55_types(nodes, edges)
        _prune_redundant_state_e7(nodes, edges)
        return {"nodes": nodes, "edges": edges}
