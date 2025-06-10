import os
import logging
from collections import defaultdict, Counter
from datetime import datetime
from dateutil import parser
from PIL import Image, ImageTk, ImageDraw, ImageFont
import openpyxl
from openpyxl.styles import PatternFill
import ttkbootstrap as ttk
from ttkbootstrap.constants import *

# --- CONFIGURACIÓN ---
RUTA_INFORMES = "InformesSinProcesar"
RUTA_EXPORTACION = "InformesExportados"
CONFIGURACION_FECHA_HORA = '%Y-%m-%d %H:%M:%S' # Se mantiene para la exportación detallada

# --- Logging ---
logging.basicConfig(filename='log_analisis_backups_app.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def buscar_encabezados(sheet):
    posibles = {
        'object name': ['object name', 'servidor'],
        'job name': ['job name', 'nombre del job'],
        'start time': ['start time', 'hora de inicio'],
        'finish time': ['finish time', 'hora de fin'],
        'duration': ['duration', 'duración'],
        'data read, gb': ['data read, gb', 'datos leídos'],
        'actual total backup size, gb': ['actual total backup size, gb', 'tamaño backup'],
        'backup status': ['backup status', 'estado']
    }
    for row_idx in range(1, 21):
        row = sheet[row_idx]
        encabezados = {}
        for idx, cell in enumerate(row):
            valor = str(cell.value).strip().lower() if cell.value else ""
            for clave, aliases in posibles.items():
                if valor in aliases and clave not in encabezados:
                    encabezados[clave] = idx
        if len(encabezados) == len(posibles):
            return encabezados, row_idx + 1
    return None, None


def filtrar_fallos_reales(backups_por_servidor):
    filtrados = defaultdict(list)
    agrupados = defaultdict(list)
    for servidor, historial in backups_por_servidor.items():
        for b in historial:
            # Clave para agrupar por servidor, nombre de trabajo y día
            clave = (servidor, b['nombre_trabajo'], b['inicio'].date())
            agrupados[clave].append(b)

    for clave, ejecuciones in agrupados.items():
        # Si ninguna de las ejecuciones para esa clave fue 'success', se considera falla
        if not any(e['estado'] == 'success' for e in ejecuciones):
            for e in ejecuciones:
                filtrados[clave[0]].append(e) # Agrega las fallas al servidor correspondiente
    return filtrados


def analizar_informes(ruta_informes):
    backups_por_servidor = defaultdict(list)
    trabajos_por_servidor = defaultdict(set)
    fechas_por_servidor_y_trabajo = defaultdict(set)
    trabajos = set()
    servidores = set()
    fechas = set()

    archivos = [os.path.join(ruta_informes, f) for f in os.listdir(ruta_informes) if f.endswith('.xlsx')]
    if not archivos:
        logging.warning("No hay archivos en la carpeta de informes.")
        return backups_por_servidor, trabajos_por_servidor, trabajos, servidores, fechas, fechas_por_servidor_y_trabajo

    for archivo in archivos:
        try:
            workbook = openpyxl.load_workbook(archivo, data_only=True)
            sheet = workbook.active
            headers, fila_inicio = buscar_encabezados(sheet)
            if not headers:
                logging.error(f"No se encontraron encabezados válidos en {archivo}")
                continue

            for row in sheet.iter_rows(min_row=fila_inicio, values_only=True):
                try:
                    servidor = row[headers['object name']]
                    if not servidor:
                        continue
                    fecha_inicio = parser.parse(str(row[headers['start time']]))
                    fecha_fin = parser.parse(str(row[headers['finish time']]))
                    estado = str(row[headers['backup status']]).lower().strip()
                    trabajo = row[headers['job name']]

                    backup = {
                        'nombre_trabajo': trabajo,
                        'inicio': fecha_inicio,
                        'fin': fecha_fin,
                        'duracion': row[headers['duration']],
                        'data_read': row[headers['data read, gb']],
                        'tamano_backup': row[headers['actual total backup size, gb']],
                        'estado': estado
                    }

                    backups_por_servidor[servidor].append(backup)
                    trabajos.add(trabajo)
                    trabajos_por_servidor[servidor].add(trabajo)
                    servidores.add(servidor)
                    fechas.add(fecha_inicio.date())
                    fechas_por_servidor_y_trabajo[(servidor, trabajo)].add(fecha_inicio.date())
                except Exception as e:
                    logging.warning(f"Error en fila de {archivo}: {e}")
        except Exception as e:
            logging.error(f"No se pudo procesar {archivo}: {e}")

    backups_filtrados = filtrar_fallos_reales(backups_por_servidor)
    return backups_filtrados, trabajos_por_servidor, sorted(trabajos), sorted(servidores), sorted(fechas), fechas_por_servidor_y_trabajo


def exportar_excel(resultados):
    if not os.path.exists(RUTA_EXPORTACION):
        os.makedirs(RUTA_EXPORTACION)

    fecha_actual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    ruta_archivo = os.path.join(RUTA_EXPORTACION, f"InformeGenerado_{fecha_actual}.xlsx")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Auditoría Fallas" # Título más específico
    # Columnas para la exportación sin 'Duración' y 'Data Read (GB)'
    ws.append(["Servidor", "Nombre Trabajo", "Inicio", "Fin", "Tamaño Backup (GB)", "Estado"])

    colores = {
        "success": "C6EFCE", # No se espera 'success' en las fallas, pero se mantiene por si acaso
        "failed": "FFC7CE",
        "warning": "FFEB9C"
    }

    resumen_estado = Counter()
    # Solo exportar las fallas de cada día por servidor (comportamiento de distinct)
    fallas_por_dia_servidor_trabajo = {}
    for r in resultados:
        clave_diaria = (r['servidor'], r['nombre_trabajo'], r['inicio'].date())
        # Guarda solo la última ocurrencia del día si hay varias, o la primera si es la única
        if clave_diaria not in fallas_por_dia_servidor_trabajo or r['inicio'] > fallas_por_dia_servidor_trabajo[clave_diaria]['inicio']:
            fallas_por_dia_servidor_trabajo[clave_diaria] = r

    for r in fallas_por_dia_servidor_trabajo.values():
        fila = [
            r['servidor'], r['nombre_trabajo'],
            r['inicio'].strftime(CONFIGURACION_FECHA_HORA), # Se mantiene hora para el detalle en Excel
            r['fin'].strftime(CONFIGURACION_FECHA_HORA),     # Se mantiene hora para el detalle en Excel
            r['tamano_backup'], r['estado']
        ]
        ws.append(fila)
        resumen_estado[r['estado']] += 1
        color = colores.get(r['estado'], "FFFFFF")
        # Ajuste de columnas para el color
        for col in range(1, len(fila) + 1):
            ws.cell(row=ws.max_row, column=col).fill = PatternFill(start_color=color, end_color=color, fill_type="solid")

    ws_resumen = wb.create_sheet(title="Resumen Fallas") # Título más específico
    ws_resumen.append(["Estado", "Cantidad"])
    for estado, cantidad in resumen_estado.items():
        ws_resumen.append([estado, cantidad])

    wb.save(ruta_archivo)
    print(f"✅ Informe de Fallas exportado en: {ruta_archivo}")


def crear_interfaz(backups_por_servidor, trabajos_por_servidor, trabajos, servidores, fechas, fechas_por_servidor_y_trabajo):
    app = ttk.Window(themename="superhero")
    app.title("Explorador de Backups")
    app.geometry("1300x750")
    app.iconbitmap ("C:/Users/eromerov/scriptTIGO.py/ServidorICONO.ico")

    # --- Frame superior con ícono y título ---
    frame_top = ttk.Frame(app)
    frame_top.pack(fill="x", padx=10, pady=10)

    try:
        icono_buscar = Image.open("C:/Users/eromerov/scriptTIGO.py/ICONObuscar.png")
        icono_buscar = icono_buscar.resize((30, 30), Image.LANCZOS)
        icono_tk = ImageTk.PhotoImage(icono_buscar)
        label_icono = ttk.Label(frame_top, image=icono_tk)
        label_icono.image = icono_tk
        label_icono.pack(side="left", padx=(5, 10))
    except Exception as e:
        print(f"⚠️ No se pudo cargar ICONObuscar.png: {e}")

    label_titulo = ttk.Label(frame_top, text="Explorador de Backups", font=("Segoe UI", 18, "bold"))
    label_titulo.pack(side="left")

    # --- Frame de filtros ---
    frame_filtros = ttk.LabelFrame(app, text="Filtros de Búsqueda", padding=15)
    frame_filtros.pack(padx=10, pady=5, fill="x")

    # Filtros de búsqueda
    ttk.Label(frame_filtros, text="Servidor").grid(row=1, column=0, sticky="w")
    combo_servidor = ttk.Combobox(frame_filtros, values=servidores, state="readonly", width=25)
    combo_servidor.grid(row=1, column=1, padx=5)

    ttk.Label(frame_filtros, text="Trabajo").grid(row=1, column=2, sticky="w")
    combo_trabajo = ttk.Combobox(frame_filtros, values=[], state="readonly", width=25)
    combo_trabajo.grid(row=1, column=3, padx=5)

    ttk.Label(frame_filtros, text="Fecha").grid(row=1, column=4, sticky="w")
    combo_fecha = ttk.Combobox(frame_filtros, values=[], state="readonly", width=20)
    combo_fecha.grid(row=1, column=5, padx=5)

    ttk.Label(frame_filtros, text="Estado").grid(row=1, column=6, sticky="w")
    combo_estado = ttk.Combobox(frame_filtros, values=["", "success", "failed", "warning"], state="readonly", width=15)
    combo_estado.grid(row=1, column=7, padx=5)

    # Botones
    ttk.Button(frame_filtros, text="Limpiar Filtros", command=lambda: limpiar_filtros(), bootstyle=INFO).grid(row=2, column=1, pady=10)
    ttk.Button(frame_filtros, text="Buscar", command=lambda: buscar(), bootstyle=PRIMARY).grid(row=2, column=3, pady=10)
    ttk.Button(frame_filtros, text="Exportar a Excel", command=lambda: exportar(), bootstyle=SUCCESS).grid(row=2, column=4, pady=10)

    # Logo SAVIA con créditos, mini y más a la derecha
    try:
        ruta_logo = "C:/Users/eromerov/scriptTIGO.py/logoSAVIA.png"
        logo_img = Image.open(ruta_logo).convert("RGBA")
        ancho, alto = logo_img.size
        nuevo_alto = alto + 25
        lienzo = Image.new("RGBA", (ancho, nuevo_alto), (255, 255, 255, 0))
        lienzo.paste(logo_img, (0, 0))

        draw = ImageDraw.Draw(lienzo)
        texto = "Desarrollado por Eber Romero"
        fuente = ImageFont.load_default()
        x = (ancho - draw.textlength(texto, font=fuente)) // 2
        y = alto + 5
        draw.text((x, y), texto, fill="white", font=fuente)

        lienzo = lienzo.resize((120, int(nuevo_alto * 120 / ancho)), Image.LANCZOS)
        logo_tk = ImageTk.PhotoImage(lienzo)

        # Colocarlo a la derecha (columna extra al final)
        label_logo = ttk.Label(frame_filtros, image=logo_tk)
        label_logo.image = logo_tk
        label_logo.grid(row=0, column=10, rowspan=3, padx=30, pady=10, sticky="ns")
    except Exception as e:
        print(f"⚠️ No se pudo cargar el logo SAVIA: {e}")

    # Área de resultados
    text_resultado = ttk.ScrolledText(app, width=150, height=30, wrap="none", font=("Consolas", 10))
    text_resultado.pack(padx=10, pady=10, fill="both", expand=True)

    def limpiar_filtros():
        combo_servidor.set("")
        combo_trabajo.set("")
        combo_fecha.set("")
        combo_estado.set("")
        # Limpiar resultados también
        text_resultado.delete(1.0, ttk.END)

    def buscar():
        text_resultado.delete(1.0, ttk.END)
        filtro_srv = combo_servidor.get().strip()
        filtro_job = combo_trabajo.get().strip()
        filtro_fecha = combo_fecha.get().strip()
        filtro_estado = combo_estado.get().strip()

        resultados_filtrados = [] # Para almacenar todos los backups que cumplen los filtros
        # Para el resumen diario (distinct por servidor, trabajo y fecha)
        resumen_diario_fallas = {}

        for servidor, historial in backups_por_servidor.items():
            if filtro_srv and filtro_srv != servidor:
                continue
            for intento in historial:
                # Si el estado es "success", no lo consideramos como una falla real para el resumen diario
                if intento['estado'] == 'success':
                    continue

                if filtro_job and filtro_job != intento['nombre_trabajo']:
                    continue
                # Aquí se formatea la fecha para comparar solo la parte de la fecha
                if filtro_fecha and filtro_fecha != intento['inicio'].strftime('%Y-%m-%d'):
                    continue
                if filtro_estado and filtro_estado != intento['estado']:
                    continue

                resultados_filtrados.append({**intento, 'servidor': servidor})

                # Lógica para el "distinct" por día del mismo servidor y trabajo
                clave_diaria = (servidor, intento['nombre_trabajo'], intento['inicio'].date())
                # Si la clave no está o si el intento actual es más reciente, lo actualiza
                if clave_diaria not in resumen_diario_fallas or intento['inicio'] > resumen_diario_fallas[clave_diaria]['inicio']:
                    resumen_diario_fallas[clave_diaria] = {**intento, 'servidor': servidor}


        if resultados_filtrados:
            # Ordenar todos los resultados para una mejor visualización (opcional)
            resultados_filtrados.sort(key=lambda x: (x['servidor'], x['nombre_trabajo'], x['inicio']))

            text_resultado.insert(ttk.END, "📝 Todos los intentos (filtrados por falla):\n")
            # Mostrar todos los intentos que cumplen los filtros (incluyendo fallas individuales)
            for r in resultados_filtrados:
                # Aquí se muestra solo la fecha
                text_resultado.insert(ttk.END, f"{r['inicio'].strftime('%Y-%m-%d %H:%M:%S')} | {r['servidor']} | {r['nombre_trabajo']} | Estado: {r['estado']}\n")

            text_resultado.insert(ttk.END, "\n--- Resumen de Fallas Diarias (por Servidor y Trabajo) ---\n")
            # Convertir el diccionario a una lista y ordenar para mostrar consistentemente
            fallas_para_mostrar = sorted(resumen_diario_fallas.values(), key=lambda x: (x['servidor'], x['nombre_trabajo'], x['inicio'].date()))

            if fallas_para_mostrar:
                for r in fallas_para_mostrar:
                    # Aquí se muestra solo la fecha, como un distinct por día
                    text_resultado.insert(ttk.END, f"📅 {r['inicio'].strftime('%Y-%m-%d')} | Servidor: {r['servidor']} | Trabajo: {r['nombre_trabajo']} | Estado: {r['estado']}\n")
            else:
                text_resultado.insert(ttk.END, "No hay fallas únicas por día que cumplan los filtros.\n")

        else:
            text_resultado.insert(ttk.END, "No se encontraron fallas con los filtros dados.\n")


    def exportar():
        filtro_srv = combo_servidor.get().strip()
        filtro_job = combo_trabajo.get().strip()
        filtro_fecha = combo_fecha.get().strip()
        filtro_estado = combo_estado.get().strip()

        resultados_para_exportar = []
        for servidor, historial in backups_por_servidor.items():
            if filtro_srv and filtro_srv != servidor:
                continue
            for intento in historial:
                # Solo exportar fallas (failed, warning), ignorar 'success' para el informe de fallas
                if intento['estado'] == 'success':
                    continue

                if filtro_job and filtro_job != intento['nombre_trabajo']:
                    continue
                if filtro_fecha and filtro_fecha != intento['inicio'].strftime('%Y-%m-%d'):
                    continue
                if filtro_estado and filtro_estado != intento['estado']:
                    continue
                resultados_para_exportar.append({**intento, 'servidor': servidor})

        if resultados_para_exportar:
            exportar_excel(resultados_para_exportar) # Llama a la función de exportación
        else:
            ttk.messagebox.showinfo("Exportar", "No hay fallas para exportar con los filtros dados.")

    app.mainloop()


# --- MAIN ---
if __name__ == "__main__":
    try:
        print("🚀 Cargando datos...")
        backups_servidor, trabajos_por_servidor, trabajos, servidores, fechas, fechas_por_servidor_y_trabajo = analizar_informes(RUTA_INFORMES)
        crear_interfaz(backups_servidor, trabajos_por_servidor, trabajos, servidores, fechas, fechas_por_servidor_y_trabajo)
    except Exception as e:
        logging.error(f"Error general: {e}")
        print(f"❌ Error en la aplicación: {e}")
