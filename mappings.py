DISTRICT_TO_LAU1 = {
    "Bratislava I": 101,
    "Bratislava II": 102,
    "Bratislava III": 103,
    "Bratislava IV": 104,
    "Bratislava V": 105,
    "Malacky": 106,
    "Pezinok": 107,
    "Senec": 108,
    "Dunajská Streda": 201,
    "Galanta": 202,
    "Hlohovec": 203,
    "Piešťany": 204,
    "Senica": 205,
    "Skalica": 206,
    "Trnava": 207,
    "Bánovce nad Bebravou": 301,
    "Ilava": 302,
    "Myjava": 303,
    "Nové Mesto nad Váhom": 304,
    "Partizánske": 305,
    "Považská Bystrica": 306,
    "Prievidza": 307,
    "Púchov": 308,
    "Trenčín": 309,
    "Komárno": 401,
    "Levice": 402,
    "Nitra": 403,
    "Nové Zámky": 404,
    "Šaľa": 405,
    "Topoľčany": 406,
    "Zlaté Moravce": 407,
    "Bytča": 501,
    "Čadca": 502,
    "Dolný Kubín": 503,
    "Kysucké Nové Mesto": 504,
    "Liptovský Mikuláš": 505,
    "Martin": 506,
    "Námestovo": 507,
    "Ružomberok": 508,
    "Turčianske Teplice": 509,
    "Tvrdošín": 510,
    "Žilina": 511,
    "Banská Bystrica": 601,
    "Banská Štiavnica": 602,
    "Brezno": 603,
    "Detva": 604,
    "Krupina": 605,
    "Lučenec": 606,
    "Poltár": 607,
    "Revúca": 608,
    "Rimavská Sobota": 609,
    "Veľký Krtíš": 610,
    "Zvolen": 611,
    "Žarnovica": 612,
    "Žiar nad Hronom": 613,
    "Bardejov": 701,
    "Humenné": 702,
    "Kežmarok": 703,
    "Levoča": 704,
    "Medzilaborce": 705,
    "Poprad": 706,
    "Prešov": 707,
    "Sabinov": 708,
    "Snina": 709,
    "Stará Ľubovňa": 710,
    "Stropkov": 711,
    "Svidník": 712,
    "Vranov nad Topľou": 713,
    "Gelnica": 801,
    "Košice I": 802,
    "Košice II": 803,
    "Košice III": 804,
    "Košice IV": 805,
    "Košice - okolie": 806,
    "Michalovce": 807,
    "Rožňava": 808,
    "Sobrance": 809,
    "Spišská Nová Ves": 810,
    "Trebišov": 811,
    "Cudzina": 900,
}

UNIVERSAL_PARTY_NAMES = {
    # SMER – sociálna demokracia (Direction – Social Democracy)
    "SMER": "SMER-SD",
    "SMER (tretia cesta)": "SMER-SD",      # former name in 2003
    "SMER - SD": "SMER-SD",              # variant
    "SMER-SD": "SMER-SD",
    "SMER - sociálna demokracia": "SMER-SD",

    # Slovenská demokratická a kresťanská únia – Demokratická strana (SDKÚ-DS)
    "SDKÚ": "SDKÚ-DS",
    "SDKÚ - DS": "SDKÚ-DS",
    "SDKÚ- DS": "SDKÚ-DS",
    "SDKÚ-DS": "SDKÚ-DS",
    "Slovenská demokratická a kresťanská únia - Demokratická strana": "SDKÚ-DS",
    "SDKÚ - DS - Slovenská demokratická a kresťanská únia - Demokratická strana": "SDKÚ-DS",

    # OĽANO (Ordinary People and Independent Personalities) and coalitions
    "OĽANO": "OĽANO / SLOVENSKO",
    "Obyčajní ľudia": "OĽANO / SLOVENSKO",
    "OBYČAJNÍ ĽUDIA a nezávislé osobnosti": "OĽANO / SLOVENSKO",
    "OBYČAJNÍ ĽUDIA a nezávislé osobnosti (OĽANO - NOVA)": "OĽANO / SLOVENSKO",
    "OBYČAJNÍ ĽUDIA a nezávislé osobnosti (OĽANO)": "OĽANO / SLOVENSKO",
    "OBYČAJNÍ ĽUDIA a nezávislé osobnosti (OĽANO), NOVA, Kresťanská únia (KÚ), ZMENA ZDOLA": "OĽANO / SLOVENSKO",
    "OĽANO A PRIATELIA: OBYČAJNÍ ĽUDIA (OĽANO), NEZÁVISLÍ KANDIDÁTI (NEKA), NOVA, SLOBODNÍ A ZODPOVEDNÍ, PAČIVALE ROMA, MAGYAR SZÍVEK a Kresťanská únia a ZA ĽUDÍ": "OĽANO / SLOVENSKO",
    "SLOVENSKO, ZA ĽUDÍ": "OĽANO / SLOVENSKO",  # coalition name including OĽANO

    # Sloboda a Solidarita (SaS)
    "SaS": "SaS",
    "Sloboda a Solidarita": "SaS",

    # Hlas - sociálna demokracia (Voice – Social Democracy)
    "HLAS - sociálna demokracia": "Hlas-SD",
    "HLAS ĽUDU": "Hlas-SD",  # variant of party name

    # Progressive Slovakia (PS)
    "PS": "Progresívne Slovensko",
    "Koalícia Progresívne Slovensko a SPOLU - občianska demokracia": "Progresívne Slovensko",
    "Progresívne Slovensko": "Progresívne Slovensko",

    # Kresťanskodemokratické hnutie (KDH)
    "KDH": "KDH",
    "Kresťanskodemokratické hnutie": "KDH",

    # Slovenská národná strana (SNS)
    "SNS": "SNS",
    "SNS, P SNS": "SNS",  # coalition entry (Partyfacts indicates P SNS allied with SNS)
    "Slovenská národná strana": "SNS",

    # Slovenská ľudová strana (Andrej Hlinka) (SĽS)
    "Slovenská ľudová strana": "Slovenská ľudová strana",  # fallback if variant found
    "SĽS": "Slovenská ľudová strana",
    "Slovenská ľudová strana Andreja Hlinku": "Slovenská ľudová strana",

    # Democratic Party (DS)
    "DS": "Demokratická strana",
    "DS - Ľudovít Kaník": "Demokratická strana",
    "Demokratická strana": "Demokratická strana",

    # Civic Conservative Party (OKS)
    "OKS": "OKS",
    "Občianska konzervatívna strana": "OKS",

    # Most-Híd (Bridge)
    "MOST - HÍD": "Most-Híd",
    "Most - Híd": "Most-Híd",
    "Most-Híd": "Most-Híd",
    "Modrí, Most - Híd": "Most-Híd",  # coalition name (Híd main party)

    # Komunistická strana Slovenska (KSS)
    "KSS": "KSS",
    "Komunistická strana Slovenska": "KSS",

    # Strana zelených (SZ, SZS)
    "SZ": "Strana zelených",
    "SZS": "Strana zelených",
    "Zelení": "Strana zelených",
    "Strana zelených": "Strana zelených",
    "Strana zelených Slovenska": "Strana zelených",

    # Ľudová strana Naše Slovensko - Kotlebovci (ĽSNS)
    "Ľudová strana Naše Slovensko": "Ľudová strana Naše Slovensko",
    "Kotlebovci - Ľudová strana Naše Slovensko": "Ľudová strana Naše Slovensko",
    "Kotleba - Ľudová strana Naše Slovensko": "Ľudová strana Naše Slovensko",

    # Sme Rodina
    "SME RODINA - Boris Kollár": "Sme Rodina",
    "SME RODINA": "Sme Rodina",

    # Republika
    "REPUBLIKA": "Republika",

    # Vlasť and related
    "VLASŤ": "Vlasť",
    "Vlastenecký blok": "Vlastenecký blok",

    # Kresťanská únia (KÚ) – Christian Union
    "Kresťanská únia (KÚ)": "Kresťanská únia",

    # VZDOR – strana práce
    "VZDOR - strana práce": "VZDOR",
    "VZDOR": "VZDOR",

    # TIP (Strana TIP)
    "TIP": "Strana TIP",
    "Strana TIP": "Strana TIP",

    # 99 % – občiansky hlas
    "99 %": "99 % - občiansky hlas",
    "99 % - občiansky hlas": "99 % - občiansky hlas",

    # Priama Demokracia (PD)
    "PD": "PRIAMA DEMOKRACIA",
    "PRIAMA DEMOKRACIA": "PRIAMA DEMOKRACIA",
    "PRIAMA DEMOKRACIA, Kresťanská ľudová strana": "PRIAMA DEMOKRACIA",

    # -- Unknown/Small parties, listed as themselves (with note) --
    "SDPO": "SDPO",                         # small, defunct Social Democratic Party
    "S.O.S.": "S.O.S.",                     # small
    "Misia 21": "Misia 21",                 # small (founded by Ivan Šimko)
    "Nádej": "Nádej",                       # small party (Nádej = "Hope")
    "ANO": "ANO",                           # Alliance of the New Citizen (Aliancia nového občana)
    "Slobodná vzbura": "Slobodná vzbura",   # small
    "Priama demokracia": "PRIAMA DEMOKRACIA",  # alternative spelling
    "Zmena zdola, DÚ": "Zmena zdola, DÚ",   # small (Change from Below)
}

PARTY_COLORS = {
    "SMER-SD": "#CC0000",                # red (Smer's primary color)
    "SDKÚ-DS": "#003399",                # blue (SDKÚ's official color)
    "OĽANO / SLOVENSKO": "#8DC63F",      # light green (OĽaNO's traditional iconic brand color)
    "SaS": "#7AC143",                    # green (SaS logo color)
    "Hlas-SD": "#E30613",                # red (Hlas uses a strong red)
    "Progresívne Slovensko": "#00AEEF",  # capri blue / cyan (PS signature color)
    "KDH": "#1E3E84",                    # dark blue (KDH's official conservative blue)
    "SNS": "#0B4EA2",                    # blue (SNS traditional color)
    "ĽS-HZDS": "#005DA3",                # blue (HZDS logo was strictly blue, white, and red)
    "Sme Rodina": "#00A4AC",             # turquoise (party's branding)
    "SMK / Aliancia": "#00914D",         # green (Hungarian parties) 
    "Most-Híd": "#FF6600",               # orange
    "Most-Híd / Modrí": "#FF6600",       # same coalition color
    "ĽSNS": "#00611C",                   # dark green
    "Republika": "#1D1D1B",              # black (party's logo is black and red) 
    "Vlasť": "#ED1C24",                  # red
    "Vlastenecký blok": "#C1272D",       # dark red
    "Slovenská ľudová strana": "#8B4513",# brown (approximate/historical)
    "KSS": "#FF0000",                    # red
    "VZDOR": "#990000",                  # dark red
    "Strana zelených": "#33CC33",        # green (Zelení)
    "Demokratická strana": "#002366",    # navy blue
    "OKS": "#FFCC00",                    # yellow/gold (OKS's logo is yellow and black)
    "HZD": "#8B0000",                    # dark red (Gašparovič's HZD used a dark red/burgundy logo)
    "Strana TIP": "#E6007E",             # magenta (TIP's color)
    "99 % - občiansky hlas": "#FFD700",  # gold/yellow
    "PRIAMA DEMOKRACIA": "#708090",      # slate grey
    "ANO": "#FDDA0D",                    # small/defunct
    "Zmena zdola, DÚ": "#0A33FF",        # small/defunct
    "S.O.S.": "#2BB1DE",                 # small/defunct
    "Misia 21": "#808080",               # small/defunct
    "UU": "#808080",                     # placeholder
}