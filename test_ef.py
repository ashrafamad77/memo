import requests, json

TEXT = "Spent the morning at Victoria, then had a heavy bout of coding in the library."

# --- REL (Radboud Entity Linker) → Wikidata ---------------------------------
print("=== REL (raw) ===")
try:
    r = requests.post(
        "https://rel.cs.ru.nl/api",
        json={"text": TEXT, "spans": []},
        timeout=15,
    )
    data = r.json()
    print(json.dumps(data, indent=2))
except Exception as ex:
    print(f"  REL failed: {ex}")

# --- OpenTapioca → Wikidata QID directly ------------------------------------
print("\n=== OpenTapioca (Wikidata-native) ===")
try:
    r = requests.post(
        "https://opentapioca.org/api/annotate",
        data={"query": TEXT},
        timeout=15,
    )
    for ann in r.json().get("annotations", []):
        surface = ann.get("surface_form", "?")
        best    = (ann.get("best_qid") or "?")
        tags    = ann.get("tags", [{}])
        label   = tags[0].get("label", "?") if tags else "?"
        score   = round(tags[0].get("score", 0), 2) if tags else 0
        print(f"  {surface!r:25} -> {best:12} ({label}) score={score}")
except Exception as ex:
    print(f"  OpenTapioca failed: {ex}")
