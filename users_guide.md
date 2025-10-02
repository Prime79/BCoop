# BAromfiCoop POC – Felhasználói útmutató belső ügynököknek

Ez a dokumentum a csapat többi tagjának segít eligazodni a BAromfiCoop POC kódalapjában, az adatok kezelésében és a legfontosabb munkafolyamatokban. A fókusz a keltetési modulon van, de kitérünk a lehetséges upstream és downstream bővítésekre is.

## 1. Alapfogalmak és cél
- **Flow log** (`flow_log.parquet`): esemény-alapú napló minden szállítmányról, gépről és barnról.
- **BarnFlow**: egy kiválasztott barn (telephely + istálló) teljes története, amelyet az elemző modul állít össze.
- **Setter/Hatcher**: előkeltető és keltető gépek, amelyeken a szállítmányok kosarai áthaladnak.
- **Parent pair (szülőpár)**: tojás eredetének azonosítója, Parquet metadata mezőben tárolva.

## 2. Könyvtárstruktúra áttekintése
- `simulation/` – SimPy alapú szimulációs modell (processzek, konfiguráció, logger).
- `analysis/` – Elemző modulok (főleg `barn_flow.py`).
- `entry_page/` – HTML alapú dashboard, amely a feldolgozási sorokat mutatja.
- `notebooks/notebooks/outputs/` – Generált SVG és interaktív HTML grafikonok.
- `barn_flow_graph*.py`, `multi_barn_flow_wall.py`, `generate_interactive_wall.py` – grafikon generátorok.
- `flow_writer.py`, `flow_log.parquet` – Parquet író és a legutóbbi flow napló.
- `run_simulation.py`, `quick_run.py` – teljes és rövid szimuláció indítók.
- `tools.py`, `generate_*` – segédszkriptek tömeges feldolgozáshoz.

## 3. Környezet előkészítése
1. **Python verzió**: 3.11 vagy újabb ajánlott.
2. **Virtuális környezet**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
3. **Kötelező csomagok**:
   ```bash
   pip install polars simpy numpy rich pyarrow jinja2 highcharts-core
   ```
   (Amennyiben a `barn_flow_graph_html.py`-t futtatod, a `jinja2` és a Highcharts JS fájlok szükségesek.)
4. **Opcionális**: `sqlite3` Python modul, ha régi workflow-kat futtatnál.
5. **Adatállomány**: győződj meg róla, hogy van friss `flow_log.parquet`. Ha nincs, futtasd a `quick_run.py`-t.

## 4. Adatfolyam és domain
- Szállítmány → setter kosarak → hatcher kosarak → kamion → barn.
- Minden állapotváltás sorosítva kerül a Parquet-be `event_ts` időbélyeggel.
- A BarnFlow builder rekonstruálja a szállítmányok részarányát a cél barnban.
- Upstream (szülőpár) és downstream (brojler telep) adatok készen állnak integrációra, de a jelenlegi ajánlat csak a keltető modulra koncentrál.

## 5. Gyakori munkafolyamatok
### 5.1 Szimuláció frissítése
- Rövid tesztfutás: `python quick_run.py`
- Teljes futás: `python run_simulation.py`
- Output: új `flow_log.parquet`, opcionálisan `hatchery_events.sqlite` WAL fájlok.

### 5.2 Elemző grafikonok generálása
- Statikus SVG:
  ```bash
  python barn_flow_graph.py Kaba-barn-01 --output notebooks/notebooks/outputs/barn_flow_Kaba-barn-01.svg
  ```
- Interaktív HTML (clickable hatcher breakdown):
  ```bash
  python barn_flow_graph_html.py Kaba-barn-01 \
    --output notebooks/notebooks/outputs/barn_flow_Kaba-barn-01_interactive.html
  ```
- Tömeges wall oldal (SVG panelek):
  ```bash
  python multi_barn_flow_wall.py --barn-prefix Kaba- \
    --out notebooks/notebooks/outputs/barn_flow_wall_Kaba.html
  ```
- Interaktív iframe wall:
  ```bash
  python generate_interactive_wall.py --barn-prefix Kaba- \
    --out notebooks/notebooks/outputs/barn_flow_wall_iframes.html
  ```

### 5.3 Dashboard (`entry_page/index.html`) frissítése
- A JavaScript tömbökben (`teleps`, `specialStatuses`) tudod módosítani a telephelyeket és státuszokat.
- A node kattintás a `notebooks/notebooks/outputs` mappában lévő interaktív HTML-ekre mutat. Ha új telepet adsz hozzá, generáld le a megfelelő fájlokat.

### 5.4 Prezentációs anyagok
- A `hatchery_network_presentation.md` tartalmaz két változatot (alap + keltetési fókusz). Szükség esetén onnan készíts slide-okat.

## 6. Elemző modul részletei
- `analysis/barn_flow.py` biztosítja a BarnFlow objektumot.
  - `BarnFlowBuilder.build(barn_id, cutoff)` visszaadja a szállítmányokat, idővonalat és állapotváltozásokat.
  - A builder csak a Parquet logot olvassa; SQLite nélkül is működik.
- `barn_flow_graph.py` a `BarnFlow`-ból épít renderer kontextust és SVG-t.
- `barn_flow_graph_html.py` Highcharts modált ad a hatcher node-okhoz (kosár hozam, veszteség benchmarkok).
- `generate_barn_svgs.py`, `generate_telep_graph.py` használható batch feldolgozásra (futtasd `--help`-pel a paraméterekért).

## 7. Adatforrások és fájlok
- `flow_log.parquet`: az elemzés elsődleges forrása. Strukturált mezők: `shipment_id`, `resource_type`, `resource_id`, `from_state`, `to_state`, `event_ts`, `quantity`, `metadata`.
- `hatchery_events.sqlite-wal`, `-shm`: a régi SQLite backend mellékfájljai – törölhetők, ha biztosan nincs rájuk szükség.
- `notebooks/notebooks/outputs/`: biztosítsd, hogy a generált fájlok verziókövetve legyenek, különben a HTML linkek 404-et adnak.

## 8. Bővítési lehetőségek
- **Upstream integráció**: szülőpár-telepi minőség, kor, takarmányozás adatainak összekötése a jelenlegi modellel.
- **Downstream integráció**: brojler telepi teljesítmény beolvasása (pl. súlygyarapodás, mortalitás), visszaírása a BarnFlow idővonalára.
- **Prediktív monitoring**: real-time alerting a Parquet stream alapján (polars + streaming).
- **Újratervezés**: a `simulation` modul paraméterezésével „what-if” forgatókönyvek futtatása.

## 9. Tesztelés és minőségbiztosítás
- Minimum ellenőrzés: futtasd `python quick_run.py`, majd generálj egy-két grafikont (`barn_flow_graph.py`).
- Ha módosítod az elemző logikát, hasonlítsd össze a korábbi SVG/HTML outputokkal (`git diff` + böngészőben vizuális ellenőrzés).
- Ügyelj arra, hogy a `flow_log.parquet` mérete nőhet; nagy futások előtt archiválj vagy készíts másolatot.

## 10. Hibakeresési tippek
- **Hiányzó szülőpár az ábrán**: futtasd újra a szimulációt, hogy bekerüljön a metadata.
- **Üres grafikon**: nincs olyan barn id a Parquet-ben; ellenőrizd a prefixet.
- **Highcharts hiba**: ellenőrizd, hogy a `highcharts.js` elérhető (a HTML generátor beágyazza, de CDN blokkolás gondot okozhat).
- **Kézi státusz módosítás az entry page-en**: frissítsd a `specialStatuses` objektumot.

## 11. Hasznos parancsok (cheat sheet)
- Virtuális környezet aktiválás: `source .venv/bin/activate`
- Gyors szimuláció: `python quick_run.py`
- Barn SVG: `python barn_flow_graph.py <barn-id>`
- Interaktív barn HTML: `python barn_flow_graph_html.py <barn-id>`
- Wall oldal: `python multi_barn_flow_wall.py --barn-prefix <telep>`
- Entry page megnyitása: dupla kattintás a `entry_page/index.html`-re vagy `python -m http.server`-rel szolgáld ki.

## 12. Kapcsolattartás és verziókezelés
- Minden érdemi módosításról készíts commitot (pl. „Update barn wall for új turnus”).
- Sensitiv adatok (pl. élő telepi mérés) ne kerüljenek a repo-ba.
- A GitHub remote: `https://github.com/Prime79/BCoop.git`; push előtt futtasd a fenti minőségellenőrzést.

---
Frissítés dátuma: 2025-10-01. Kérlek jelezd, ha új funkció kerül be vagy a folyamat változik, és frissítjük az útmutatót.
