import requests
import json
import os
import glob
import time
import ssl
from dotenv import load_dotenv
load_dotenv()
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime, timedelta, timezone

ssl._create_default_https_context = ssl._create_unverified_context

# ---------------------------------------------
# CONFIGURACION GLOBAL
# Lee desde variables de entorno en Railway
# ---------------------------------------------
EMAIL_ORIGEN    = os.environ.get("EMAIL_ORIGEN", "")
SENDGRID_APIKEY = os.environ.get("SENDGRID_APIKEY", "")
TZ_LOCAL        = timezone(timedelta(hours=-6))  # El Salvador UTC-6
# ---------------------------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.copart.com",
    "Referer": "https://www.copart.com/",
}

MAX_PAGINAS = 20

def ahora_local():
    return datetime.now(TZ_LOCAL)

def cargar_lotes_ayer(cliente_id):
    archivos = sorted(glob.glob(f"resultados/{cliente_id}_*.json"), reverse=True)
    hoy_str  = ahora_local().strftime("%Y%m%d")
    archivos = [a for a in archivos if hoy_str not in a]
    if not archivos:
        print(f"  Sin reporte anterior para comparar")
        return set()
    ruta = archivos[0]
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            lotes_ant = json.load(f)
        numeros = {l["lote"] for l in lotes_ant}
        print(f"  Reporte anterior cargado: {os.path.basename(ruta)} -- {len(numeros)} lotes")
        return numeros
    except Exception as e:
        print(f"  Error cargando reporte anterior: {e}")
        return set()

def construir_search_body(cfg):
    fa = cfg["filtros_api"]
    makes  = [f'lot_make_desc:"{m}"'  for m in fa["makes"]]
    models = [f'lot_model_desc:"{m}"' for m in fa["models"]]
    locs   = [f'yard_name:"{l}"'      for l in fa["locations"]]

    year_min = fa.get("year_min", 2000)
    year_max = fa.get("year_max", 2027)
    odo_max  = fa.get("odometer_max", 999999)

    filtro = {
        "LOC":  locs,
        "MAKE": makes,
        "MISC": ["#VehicleTypeCode:VEHTYPE_V"],
        "MODL": models,
        "ODM":  [f"odometer_reading_received:[0 TO {odo_max}]"],
        "VEHT": ["vehicle_type_code:VEHTYPE_V"],
        "YEAR": [f"lot_year:[{year_min} TO {year_max}]"]
    }

    if cfg["filtros_locales"].get("solo_hoy_manana", True):
        hoy_local    = ahora_local().replace(hour=0, minute=0, second=0, microsecond=0)
        manana_local = hoy_local + timedelta(days=2) - timedelta(seconds=1)
        hoy_utc      = hoy_local.astimezone(timezone.utc)
        manana_utc   = manana_local.astimezone(timezone.utc)
        filtro["SDAT"] = [f'auction_date_utc:["{hoy_utc.strftime("%Y-%m-%dT%H:%M:%SZ")}" TO "{manana_utc.strftime("%Y-%m-%dT%H:%M:%SZ")}"]']

    return {
        "query": ["*"],
        "filter": filtro,
        "sort": ["auction_date_type asc", "auction_date_utc asc"],
        "watchListOnly": False,
        "freeFormSearch": False,
        "hideImages": False,
        "defaultSort": False,
        "specificRowProvided": False,
        "displayName": "",
        "searchName": ""
    }

def get_current_bid(lot):
    """Puja activa real. 0 = nadie ha pujado todavia."""
    dld = lot.get("dynamicLotDetails")
    if dld and isinstance(dld, dict):
        cb = dld.get("currentBid", 0)
        if cb and cb > 0:
            return float(cb)
    hb = lot.get("hb", 0) or 0
    return float(hb)

def get_buy_it_now(lot):
    """Precio Buy It Now si existe. 0 = no disponible."""
    bnp  = lot.get("bnp", 0) or 0
    bndc = (lot.get("bndc") or "").upper()
    if bnp > 0 and "BUY" in bndc:
        return float(bnp)
    return 0.0

def get_estimated_value(lot):
    """Valor estimado de mercado (ACV) segun Copart."""
    return float(lot.get("lotPlugAcv", 0) or 0)

def ad_to_fecha_local(ad_ms):
    if not ad_ms:
        return None, "N/A"
    try:
        dt_utc   = datetime.fromtimestamp(ad_ms / 1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone(TZ_LOCAL)
        return dt_local.date(), dt_local.strftime("%d/%m/%Y")
    except Exception as e:
        print(f"  Error convirtiendo fecha {ad_ms}: {e}")
        return None, "N/A"

def aplicar_filtros_locales(lotes_raw, fl):
    filtrados = []
    desc = {"precio": 0, "daño": 0, "titulo": 0, "run_drive": 0}

    precio_max     = fl.get("precio_maximo", 0)
    excluir_daño   = [x.upper() for x in fl.get("excluir_daño", [])]
    excluir_titulo = [x.upper() for x in fl.get("excluir_titulo", [])]
    solo_run       = fl.get("solo_run_and_drive", False)

    for lot in lotes_raw:

        # Solo descartar si hay puja ACTIVA que supera el maximo
        if precio_max > 0:
            current_bid = get_current_bid(lot)
            if current_bid > 0 and current_bid > precio_max:
                desc["precio"] += 1
                continue

        daño = (lot.get("dd") or "").upper()
        if any(p in daño for p in excluir_daño):
            desc["daño"] += 1
            continue

        titulo = (lot.get("td") or lot.get("htsmn") or "").upper()
        if any(p in titulo for p in excluir_titulo):
            desc["titulo"] += 1
            continue

        if solo_run:
            lcd = (lot.get("lcd") or "").upper()
            ess = (lot.get("ess") or "").upper()
            if not ("RUN" in lcd or "DRIVE" in lcd or "ENGINE" in ess or "RUN" in ess):
                desc["run_drive"] += 1
                continue

        filtrados.append(lot)

    print(f"    Descartados -- precio: {desc['precio']} | daño: {desc['daño']} | titulo: {desc['titulo']} | run&drive: {desc['run_drive']}")
    print(f"    Lotes que pasan filtros: {len(filtrados)}")
    return filtrados

def formato_lote(lot):
    url         = f"https://www.copart.com/lot/{lot.get('ln')}/{lot.get('ldu', '')}"
    odo         = lot.get("orr", 0) or 0
    current_bid = get_current_bid(lot)
    buy_now     = get_buy_it_now(lot)
    est_value   = get_estimated_value(lot)
    tiene_puja  = current_bid > 0
    tiene_bin   = buy_now > 0

    ad                    = lot.get("ad")
    fecha_date, fecha_str = ad_to_fecha_local(ad)
    odometro_str = f"{int(odo):,} mi" if odo and odo > 0 else "No reportado"

    return {
        "lote":            str(lot.get("lotNumberStr", lot.get("ln", "N/A"))),
        "descripcion":     lot.get("ld", "N/A"),
        "color":           lot.get("clr", "N/A"),
        "odometro":        odometro_str,
        "daño":            lot.get("dd", "N/A"),
        "patio":           lot.get("yn", "N/A"),
        "llaves":          lot.get("hk", "N/A"),
        "transmision":     lot.get("tmtp", "N/A"),
        # Precios correctos
        "current_bid":     f"${current_bid:,.0f}" if tiene_puja else "Sin pujas",
        "buy_it_now":      f"${buy_now:,.0f}"     if tiene_bin  else "N/A",
        "est_value":       f"${est_value:,.0f}"   if est_value > 0 else "N/A",
        # Numericos para ordenamiento
        "current_bid_num": current_bid,
        "buy_now_num":     buy_now,
        "est_value_num":   est_value,
        "tiene_puja":      tiene_puja,
        "tiene_bin":       tiene_bin,
        "hora":            lot.get("at", ""),
        "fecha":           fecha_str,
        "fecha_iso":       fecha_date.isoformat() if fecha_date else None,
        "url":             url,
        "imagen":          lot.get("tims", ""),
    }

def scrape_cliente(cfg):
    search_body = construir_search_body(cfg)
    fl          = cfg["filtros_locales"]
    todos       = []
    page_num    = 0
    page_size   = 100

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        print("  Iniciando sesion con Copart...")
        session.get("https://www.copart.com", timeout=15)
        time.sleep(2)
        session.get("https://www.copart.com/lotSearchResults", timeout=15)
        time.sleep(2)
    except Exception as e:
        print(f"  Advertencia en warm-up: {e}")

    while page_num < MAX_PAGINAS:
        body = {**search_body, "page": page_num, "size": page_size, "start": page_num * page_size}
        print(f"  Consultando pagina {page_num + 1}...")
        time.sleep(3)

        try:
            resp = session.post(
                "https://www.copart.com/public/lots/search-results",
                json=body,
                timeout=30
            )
            if resp.status_code == 403:
                print(f"  ERROR 403 -- Copart bloqueo el request")
                break
            elif resp.status_code != 200:
                print(f"  ERROR HTTP {resp.status_code}")
                break
            data = resp.json()

        except requests.exceptions.Timeout:
            print(f"  ERROR -- timeout en pagina {page_num + 1}")
            break
        except requests.exceptions.ConnectionError as e:
            print(f"  ERROR -- conexion en pagina {page_num + 1}: {e}")
            break
        except Exception as e:
            print(f"  ERROR -- pagina {page_num + 1}: {e}")
            break

        results = data.get("data", {}).get("results", {})
        content = results.get("content", [])
        total   = results.get("totalElements", 0)

        if not content:
            print("  -> Sin resultados")
            break

        print(f"  -> {len(content)} lotes recibidos de API (total: {total})")
        content = aplicar_filtros_locales(content, fl)

        for lot in content:
            todos.append(formato_lote(lot))

        if (page_num + 1) * page_size >= total:
            break
        page_num += 1

    vistos, unicos = set(), []
    for l in todos:
        if l["lote"] not in vistos:
            vistos.add(l["lote"])
            unicos.append(l)

    return unicos

def build_tabla(lista, lotes_ayer):
    if not lista:
        return "<p style='color:#999;font-style:italic;'>Sin vehiculos.</p>"
    filas = ""
    for l in lista:
        img      = f'<img src="{l["imagen"]}" width="120" style="border-radius:4px;">' if l["imagen"] else "Sin imagen"
        es_nuevo = l["lote"] not in lotes_ayer
        badge    = '<span style="background:#e53935;color:white;font-size:10px;font-weight:bold;padding:2px 6px;border-radius:10px;margin-left:6px;">NUEVO</span>' if es_nuevo else ""
        fila_bg  = 'background:#fff8e1;' if es_nuevo else ""

        # Puja actual
        if l["current_bid"] == "Sin pujas":
            puja_display = '<span style="color:#999;font-size:11px;">Sin pujas</span>'
        else:
            puja_display = f'<strong style="color:#e53935;">{l["current_bid"]}</strong>'

        # Buy It Now — resaltado en verde si existe
        if l["buy_it_now"] == "N/A":
            bin_display = '<span style="color:#ccc;font-size:11px;">N/A</span>'
        else:
            bin_display = f'<strong style="color:#2e7d32;">{l["buy_it_now"]}</strong>'

        filas += f"""
        <tr style="{fila_bg}">
            <td style="padding:8px;border-bottom:1px solid #eee;">{img}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">
                <strong><a href="{l['url']}" style="color:#1a73e8;">#{l['lote']}</a></strong>{badge}<br>
                {l['descripcion']}
            </td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{l['color']}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{l['odometro']}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{l['daño']}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{l['patio']}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{l['llaves']}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{l['fecha']}<br>{l['hora']}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{puja_display}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{bin_display}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;color:#555;">{l['est_value']}</td>
        </tr>"""
    return f"""
    <table style="border-collapse:collapse;width:100%;margin-bottom:30px;">
        <thead style="background:#1a73e8;color:white;">
            <tr>
                <th style="padding:8px;">Foto</th>
                <th style="padding:8px;">Lote / Descripcion</th>
                <th style="padding:8px;">Color</th>
                <th style="padding:8px;">Odometro</th>
                <th style="padding:8px;">Daño</th>
                <th style="padding:8px;">Patio</th>
                <th style="padding:8px;">Llaves</th>
                <th style="padding:8px;">Fecha Subasta</th>
                <th style="padding:8px;">Puja Actual</th>
                <th style="padding:8px;">Buy It Now</th>
                <th style="padding:8px;">Valor Est. (ACV)</th>
            </tr>
        </thead>
        <tbody>{filas}</tbody>
    </table>"""

def generar_html(lotes, cfg, lotes_ayer):
    fa     = cfg["filtros_api"]
    fl     = cfg["filtros_locales"]
    ahora  = ahora_local()
    hoy    = ahora.date()
    manana = hoy + timedelta(days=1)
    fecha_hoy_str    = hoy.strftime("%d/%m/%Y")
    fecha_manana_str = manana.strftime("%d/%m/%Y")

    nuevos_count = sum(1 for l in lotes if l["lote"] not in lotes_ayer)

    precio_str  = f"${fl.get('precio_maximo', 0):,} (solo si hay puja activa)" if fl.get('precio_maximo', 0) > 0 else "Sin limite"
    daño_str    = ", ".join(fl.get("excluir_daño", [])) or "Ninguno"
    run_str     = "Si" if fl.get("solo_run_and_drive") else "No"
    marcas_str  = ", ".join(fa.get("makes", []))
    modelos_str = ", ".join(fa.get("models", []))
    patios_str  = ", ".join(fa.get("locations", []))
    odo_str     = f"{fa.get('odometer_max', 0):,} mi"
    anos_str    = f"{fa.get('year_min')} - {fa.get('year_max')}"

    aviso_nuevos = ""
    if nuevos_count > 0 and lotes_ayer:
        aviso_nuevos = f"""
        <div style="background:#fff3e0;border-left:4px solid #e53935;padding:10px 16px;margin-bottom:16px;border-radius:4px;">
            <strong style="color:#e53935;">{nuevos_count} vehiculo(s) nuevo(s)</strong> agregados desde el reporte anterior.
            Las filas resaltadas en amarillo son las novedades.
        </div>"""

    resumen = f"""
    <div style="background:#f8f9fa;border-left:4px solid #1a73e8;padding:12px 16px;margin-bottom:20px;border-radius:4px;font-size:12px;">
        <strong style="font-size:13px;">Filtros aplicados:</strong><br><br>
        <b>Marcas:</b> {marcas_str}<br>
        <b>Modelos:</b> {modelos_str}<br>
        <b>Anos:</b> {anos_str}<br>
        <b>Odometro max:</b> {odo_str}<br>
        <b>Patios:</b> {patios_str}<br>
        <b>Puja maxima:</b> {precio_str}<br>
        <b>Excluir daño:</b> {daño_str}<br>
        <b>Solo Run and Drive:</b> {run_str}
    </div>"""

    lotes_hoy, lotes_manana = [], []
    for l in lotes:
        iso = l.get("fecha_iso")
        try:
            fd = datetime.strptime(iso, "%Y-%m-%d").date() if iso else None
        except ValueError as e:
            print(f"  Error parseando fecha {iso}: {e}")
            fd = None
        if fd == hoy:
            lotes_hoy.append(l)
        else:
            lotes_manana.append(l)

    return f"""
    <html><body style="font-family:Arial,sans-serif;font-size:13px;max-width:1400px;margin:auto;">
    <h2 style="color:#333;">Reporte Copart -- {cfg['nombre']} -- {fecha_hoy_str}</h2>
    <p><strong>{len(lotes)} vehiculos encontrados</strong> con subastas hoy y manana.</p>
    {aviso_nuevos}
    {resumen}
    <h3 style="color:#1a73e8;border-bottom:2px solid #1a73e8;padding-bottom:6px;">
        HOY -- {fecha_hoy_str} ({len(lotes_hoy)} vehiculos)
    </h3>
    {build_tabla(lotes_hoy, lotes_ayer)}
    <h3 style="color:#e8711a;border-bottom:2px solid #e8711a;padding-bottom:6px;">
        MANANA -- {fecha_manana_str} ({len(lotes_manana)} vehiculos)
    </h3>
    {build_tabla(lotes_manana, lotes_ayer)}
    <p style="color:#999;font-size:11px;margin-top:20px;">Generado automaticamente -- Copart Search Bot</p>
    </body></html>"""

def enviar_email(lotes, cfg, lotes_ayer):
    ahora        = ahora_local()
    fecha_hoy    = ahora.strftime("%d/%m/%Y")
    makes        = ", ".join(cfg["filtros_api"]["makes"])
    nuevos_count = sum(1 for l in lotes if l["lote"] not in lotes_ayer)
    nuevo_tag    = f" | {nuevos_count} NUEVOS" if nuevos_count > 0 and lotes_ayer else ""
    asunto       = f"Copart {makes} -- {len(lotes)} vehiculos{nuevo_tag} -- {fecha_hoy}"

    mensaje = Mail(
        from_email=EMAIL_ORIGEN,
        to_emails=cfg["email_destino"],
        subject=asunto,
        html_content=generar_html(lotes, cfg, lotes_ayer)
    )

    intentos = 3
    for intento in range(1, intentos + 1):
        try:
            sg = SendGridAPIClient(SENDGRID_APIKEY)
            sg.send(mensaje)
            print(f"  Email enviado a {cfg['email_destino']}")
            return
        except Exception as e:
            print(f"  Intento {intento}/{intentos} fallido: {e}")
            if intento < intentos:
                time.sleep(5)

    print(f"  ERROR -- no se pudo enviar email tras {intentos} intentos")

def procesar_cliente(ruta_json):
    with open(ruta_json, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    if not cfg.get("activo", True):
        print(f"  Cliente inactivo -- saltando")
        return

    print(f"\n{'='*50}")
    print(f"Cliente: {cfg['nombre']}")
    print(f"{'='*50}")

    lotes_ayer = cargar_lotes_ayer(cfg["id"])
    lotes      = scrape_cliente(cfg)
    print(f"  Total lotes unicos: {len(lotes)}")

    nuevos = sum(1 for l in lotes if l["lote"] not in lotes_ayer)
    if lotes_ayer:
        print(f"  Lotes nuevos vs reporte anterior: {nuevos}")

    os.makedirs("resultados", exist_ok=True)
    archivo = f"resultados/{cfg['id']}_{ahora_local().strftime('%Y%m%d')}.json"
    with open(archivo, "w", encoding="utf-8") as f:
        json.dump(lotes, f, indent=2, ensure_ascii=False)

    if lotes:
        enviar_email(lotes, cfg, lotes_ayer)
    else:
        print(f"  Sin resultados -- no se envia email")