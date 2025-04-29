import os
import logging
from collections import defaultdict
from datetime import datetime
from dateutil import parser
import openpyxl
import tkinter as tk
from tkinter import messagebox
from openpyxl.styles import PatternFill

# --- CONFIGURACIÓN ---
RUTA_INFORMES = "C:/Users/eromerov/TIGOinformes"  
RUTA_EXPORTACION = "C:/Users/eromerov/resumenes_backups"
CONFIGURACION_FECHA_HORA = '%Y-%m-%d %H:%M:%S'

# --- Logging ---
logging.basicConfig(filename='log_analisis_backups_app.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def buscar_encabezados(sheet):
    """
    Busca los encabezados correctos en las primeras 20 filas.
    """
    for row_idx in range(1, 21):
        headers = {}
        for idx, cell in enumerate(sheet[row_idx]):
            if cell.value:
                headers[cell.value.strip().lower()] = idx

        requeridos = [
            'object name', 'job name', 'start time', 'finish time',
            'duration', 'data read, gb', 'actual total backup size, gb', 'backup status'
        ]
        if all(r in headers for r in requeridos):
            return headers, row_idx + 1  # Fila de inicio de datos

    return None, None

def analizar_informes(ruta_informes):
    """
    Lee todos los informes y devuelve backups agrupados por servidor.
    """
    backups_por_servidor = defaultdict(list)
    archivos = [os.path.join(ruta_informes, f) for f in os.listdir(ruta_informes) if f.endswith('.xlsx')]

    if not archivos:
        logging.warning("No hay archivos en la carpeta de informes.")
        return backups_por_servidor

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

                    backup = {
                        'nombre_trabajo': row[headers['job name']],
                        'inicio': fecha_inicio,
                        'fin': fecha_fin,
                        'duracion': row[headers['duration']],
                        'data_read': row[headers['data read, gb']],
                        'tamano_backup': row[headers['actual total backup size, gb']],
                        'estado': str(row[headers['backup status']]).lower().strip()
                    }

                    backups_por_servidor[servidor].append(backup)
                except Exception as e:
                    logging.warning(f"Error en fila de {archivo}: {e}")

        except Exception as e:
            logging.error(f"No se pudo procesar {archivo}: {e}")

    return backups_por_servidor

def determinar_estado_final(historial):
    fallo_detectado = False

    for intento in historial:
        if intento['estado'] in ('failed', 'warning'):
            fallo_detectado = True
        elif intento['estado'] == 'success':
            return "Éxito (Recuperado de Fallo)" if fallo_detectado else "Éxito"

    return "Fallido" if fallo_detectado else "Sin Datos"

def exportar_resumenes(backups_por_servidor, resumen_estados):
    """
    Exporta dos archivos:
    1. Resumen de estados finales
    2. Historial detallado
    """
    if not os.path.exists(RUTA_EXPORTACION):
        os.makedirs(RUTA_EXPORTACION)

    fecha_actual = datetime.now().strftime("%Y-%m")

    # Resumen de Estados
    wb_resumen = openpyxl.Workbook()
    sheet_resumen = wb_resumen.active
    sheet_resumen.title = "Resumen Estado Final"
    sheet_resumen.append(["Servidor", "Estado Final"])

    verde = PatternFill(start_color="00FF00", end_color="00FF00", fill_type="solid")
    rojo = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    amarillo = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    for servidor, estado in resumen_estados.items():
        sheet_resumen.append([servidor, estado])
        celda_estado = sheet_resumen.cell(row=sheet_resumen.max_row, column=2)
        if estado.startswith("Éxito"):
            celda_estado.fill = verde
        elif estado == "Fallido":
            celda_estado.fill = rojo
        else:
            celda_estado.fill = amarillo

    wb_resumen.save(os.path.join(RUTA_EXPORTACION, f"resumen_estado_backups_{fecha_actual}.xlsx"))
    print(f"✅ Resumen exportado en: resumenes_backups/resumen_estado_backups_{fecha_actual}.xlsx")

        # Historial Detallado
    wb_historial = openpyxl.Workbook()
    sheet_historial = wb_historial.active
    sheet_historial.title = "Historial Detallado"
    sheet_historial.append(["Servidor", "Nombre Trabajo", "Inicio", "Fin", "Duración", "Data Read (GB)", "Tamaño Backup (GB)", "Estado"])

    # Definir colores pastel
    colores = ["ADD8E6", "CCFFCC", "FFFFCC", "E6CCFF", "FFD9C2", "F2F2F2"]
    servidores_unicos = sorted(backups_por_servidor.keys())
    servidor_a_color = {servidor: colores[idx % len(colores)] for idx, servidor in enumerate(servidores_unicos)}

    for servidor, historial in backups_por_servidor.items():
        color_servidor = servidor_a_color[servidor]
        fill = PatternFill(start_color=color_servidor, end_color=color_servidor, fill_type="solid")

        historial_ordenado = sorted(historial, key=lambda x: x['inicio'])
        for intento in historial_ordenado:
            sheet_historial.append([
                servidor,
                intento['nombre_trabajo'],
                intento['inicio'].strftime(CONFIGURACION_FECHA_HORA),
                intento['fin'].strftime(CONFIGURACION_FECHA_HORA),
                intento['duracion'],
                intento['data_read'],
                intento['tamano_backup'],
                intento['estado'].capitalize()
            ])
            # Aplicar color de fondo a la fila actual
            for col in range(1, 9):  # 8 columnas
                sheet_historial.cell(row=sheet_historial.max_row, column=col).fill = fill

    wb_historial.save(os.path.join(RUTA_EXPORTACION, f"historial_detallado_backups_{fecha_actual}.xlsx"))
    print(f"✅ Historial exportado en: resumenes_backups/historial_detallado_backups_{fecha_actual}.xlsx")

def mostrar_historial(servidor, text_widget, historial):
    text_widget.config(state=tk.NORMAL)
    text_widget.delete(1.0, tk.END)

    historial_ordenado = sorted(historial, key=lambda x: x['inicio'])

    for intento in historial_ordenado:
        detalles = [
            f"Nombre Trabajo: {intento['nombre_trabajo']}",
            f"Inicio: {intento['inicio'].strftime(CONFIGURACION_FECHA_HORA)}",
            f"Fin: {intento['fin'].strftime(CONFIGURACION_FECHA_HORA)}",
            f"Duración: {intento['duracion']}",
            f"Data Read, GB: {intento['data_read']}",
            f"Tamaño Backup, GB: {intento['tamano_backup']}",
            f"Estado: {intento['estado'].capitalize()}",
            "--"
        ]

        color = "black"
        if intento['estado'] == 'success':
            color = "green"
        elif intento['estado'] == 'failed':
            color = "red"
        elif intento['estado'] == 'warning':
            color = "orange"

        for linea in detalles:
            text_widget.insert(tk.END, linea + "\n", color)

    estado_final = determinar_estado_final(historial)
    text_widget.insert(tk.END, f"\nEstado Final: {estado_final}\n", "blue")

    text_widget.config(state=tk.DISABLED)

def crear_interfaz(backups_por_servidor, resumen_estados):
    ventana = tk.Tk()
    ventana.title("Explorador de Backups")

    tk.Label(ventana, text="Servidores:").pack()
    lista_servidores = tk.Listbox(ventana, width=50)
    lista_servidores.pack(pady=5)

    servidores = sorted(backups_por_servidor.keys())
    for servidor in servidores:
        lista_servidores.insert(tk.END, servidor)

    tk.Label(ventana, text="Historial de Backup:").pack()
    text_historial = tk.Text(ventana, width=100, height=25)
    text_historial.pack(pady=5)

    text_historial.tag_configure("green", foreground="green")
    text_historial.tag_configure("red", foreground="red")
    text_historial.tag_configure("orange", foreground="orange")
    text_historial.tag_configure("black", foreground="black")
    text_historial.tag_configure("blue", foreground="blue")

    def seleccionar_servidor(event):
        seleccion = lista_servidores.curselection()
        if seleccion:
            servidor = lista_servidores.get(seleccion[0])
            mostrar_historial(servidor, text_historial, backups_por_servidor[servidor])

    lista_servidores.bind('<<ListboxSelect>>', seleccionar_servidor)

    ventana.mainloop()

if __name__ == "__main__":
    try:
        print("🚀 Cargando datos...")
        backups_servidor = analizar_informes(RUTA_INFORMES)

        if not backups_servidor:
            print("⚠️ No se encontraron backups.")
            exit()

        resumen_estados = {srv: determinar_estado_final(historial) for srv, historial in backups_servidor.items()}

        exportar_resumenes(backups_servidor, resumen_estados)

        crear_interfaz(backups_servidor, resumen_estados)

    except Exception as e:
        logging.error(f"Error general: {e}")
        print(f"❌ Error en la aplicación: {e}")
