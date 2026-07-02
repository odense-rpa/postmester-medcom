# Postmester Medcom

Robot der behandler indkommende MedCom-korrespondancemeddelelser fra KMD Nexus ved at matche emnefeltet mod et regelsæt defineret i en Excel-fil og automatisk rute hver meddelelse til organisationer, forløb og/eller opgaver.

## Hvad gør robotten?

1. Henter alle ventende MedCom-korrespondancer fra KMD Nexus-aktivitetslisten "MedCom - Korrespondancer: venter + accepterede" (op til 50 sider).
2. Tilføjer nye aktiviteter til arbejdskøen, hvis de ikke allerede er registreret (deduplikering sker på aktivitets-ID).
3. For hvert arbejdskøelement udtrækkes borgerens CPR-nummer, som bruges til at slå borgeren op i KMD Nexus.
4. Emnefeltet (Emne) matches mod regler indlæst fra `Regler.xlsx`, som understøtter både eksakt matchning og wildcard-præfiksmatchning.
5. Hvis en regel matcher og angiver en **Organisation**: tilknyttes borgeren til den pågældende organisation i KMD Nexus (springes over, hvis borgeren allerede er medlem).
6. Hvis en regel matcher og angiver **Forløb**: oprettes grundforløb og/eller forløb for borgeren i KMD Nexus (springes over ved dubletter).
7. Hvis en regel matcher og angiver en **Opgavetype**: oprettes en opgave på MedCom-meddelelsen og tildeles den angivne organisation (springes over, hvis en identisk opgave allerede eksisterer).
8. Alle udførte handlinger logges til rapporten `postmester_medcom` via odk-tools, og opgavernes afslutning spores i Tracker (SQL Server).
9. Meddelelser uden matchende regel registreres som delvise opgaver; hårde fejl fejler arbejdselementet til manuel opfølgning.

## Regelsættet

Routingen styres udelukkende af `Regler.xlsx`, som er committed til repositoriet. Hver række definerer en regel med følgende kolonner:

| Kolonne | Beskrivelse |
|---|---|
| Emne | Emnefeltet der matches mod |
| Wildcard søgning i emnefelt | Aktiverer præfiksmatchning |
| Organisation | Målorganisation i KMD Nexus |
| Forløb | Linjeskilt liste af grundforløb/forløb-par (separator: `/`) |
| Opgavetype | Opgavetype der skal oprettes |

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø)
- Adgang til **KMD Nexus** (produktion og database)
- Adgang til **Odense SQL Server** (Tracker)

## Installation

```sh
uv sync
```

## Konfiguration

Credentials registreres i Automation Server:

- `KMD Nexus - produktion`
- `KMD Nexus - database`
- `Odense SQL Server`

Miljøvariabler:

| Variabel | Beskrivelse |
|---|---|
| `ATS_URL` | Base-URL til Automation Server API (f.eks. `http://localhost/api`) |
| `ATS_TOKEN` | Bearer-token til autentificering mod Automation Server API |
| `ATS_WORKQUEUE_OVERRIDE` | Tilsidesæt standard-arbejdskø-ID |

## Kørsel

```sh
uv run python main.py --queue   # Fyld arbejdskøen
uv run python main.py           # Behandl arbejdskøen
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Forbinder til Automation Server til arbejdskøhåndtering og hentning af credentials |
| `odk-tools` | Opgavesporing (Tracker) og handlingsrapportering |
| `kmd-nexus-client` | API-klient til KMD Nexus — henter borgere, organisationer, forløb, opgaver og MedCom-aktivitetslister |
| `openpyxl` | Læser Excel-regelfilen `Regler.xlsx` |

## GDPR og sikkerhed

Robotten behandler CPR-numre fra MedCom-meddelelser til borgeropslagning i KMD Nexus. CPR-numre skrives også til handlingsrapporten `postmester_medcom`. Adgang til rapporten bør begrænses til relevante medarbejdere.
