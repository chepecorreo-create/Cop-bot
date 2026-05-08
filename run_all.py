import os
import glob
import json
import time
from copart_scraper import procesar_cliente, ahora_local, EMAIL_ORIGEN, SENDGRID_APIKEY
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

EMAIL_ADMIN = os.environ.get("EMAIL_ADMIN", "")

def enviar_alerta(cliente_nombre, error_msg):
    ahora     = ahora_local()
    fecha_hoy = ahora.strftime("%d/%m/%Y %H:%M:%S")
    asunto    = f"ALERTA -- Fallo en cliente {cliente_nombre} -- {fecha_hoy}"
    cuerpo    = f"""
    <html><body style="font-family:Arial,sans-serif;font-size:13px;">
    <h2 style="color:#e53935;">Fallo en Copart Bot</h2>
    <p><b>Cliente:</b> {cliente_nombre}</p>
    <p><b>Fecha:</b> {fecha_hoy}</p>
    <p><b>Error:</b></p>
    <pre style="background:#f5f5f5;padding:12px;border-radius:4px;">{error_msg}</pre>
    <p style="color:#999;font-size:11px;">Copart Search Bot -- Alerta automatica</p>
    </body></html>
    """
    try:
        mensaje = Mail(
            from_email=EMAIL_ORIGEN,
            to_emails=EMAIL_ADMIN,
            subject=asunto,
            html_content=cuerpo
        )
        sg = SendGridAPIClient(SENDGRID_APIKEY)
        sg.send(mensaje)
        print(f"  Alerta enviada a {EMAIL_ADMIN}")
    except Exception as e:
        print(f"  ERROR enviando alerta: {e}")

print(f"\nCOPART BOT -- {ahora_local().strftime('%d/%m/%Y %H:%M:%S')}")
print("=" * 50)

archivos = sorted(glob.glob("clientes/*.json"))

if not archivos:
    print("Sin clientes en /clientes/")
else:
    print(f"Clientes encontrados: {len(archivos)}")
    for i, archivo in enumerate(archivos):
        nombre = archivo
        try:
            with open(archivo, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            nombre = cfg.get("nombre", archivo)
            procesar_cliente(archivo)
        except Exception as e:
            print(f"\n  FALLO en {nombre}: {e}")
            enviar_alerta(nombre, str(e))

        if i < len(archivos) - 1:
            print(f"\n  Esperando 30 segundos antes del siguiente cliente...")
            time.sleep(30)

print(f"\nProceso completado -- {ahora_local().strftime('%H:%M:%S')}")