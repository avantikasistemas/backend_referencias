from Utils.tools import Tools, CustomException
from Utils.querys import Querys
import base64
import pandas as pd
from io import BytesIO
import traceback

class Referencias:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.querys = Querys(self.db)

    # Función para cargar el archivo de solicitudes
    def cargar_archivo(self, data: dict):
        """ Api que realiza la consulta de los estados. """
        try:
            
            extensiones_permitidas = ['xlsx']
            
            archivo = data["archivo"]
            nombre = data["nombre"]
            ext = nombre.split(".")[1]

            # Verificamos si el archivo existe
            if not archivo:
                raise CustomException("El archivo no existe.")
            
            # Verificamos si la extensión es válida
            if ext not in extensiones_permitidas:
                raise CustomException("La extensión del archivo no es válida.")

            referencias = self.procesar_archivo(archivo)

            # Retornamos la información.
            return self.tools.output(200, "Archivo cargado con éxito.", referencias)

        except CustomException as e:
            traceback.print_exc()
            print(f"Error al cargar archivo: {e}")
            raise CustomException(str(e))

    # Función para procesar el archivo excel
    def procesar_archivo(self, archivo):
        """ Procesa el archivo de solicitudes. """
        try:
            # 1. Decodificar base64 a binario
            archivo_excel = base64.b64decode(archivo)

            # 2. Convertir binario en archivo legible (BytesIO)
            excel_io = BytesIO(archivo_excel)

            # 3. Leer el archivo con pandas
            df = pd.read_excel(
                excel_io, 
                engine='openpyxl',
                dtype={
                    "clase": str,
                    "contable": str,
                    "generico": str,
                    "maneja_inventario": str,
                    "grupo": str,
                    "subgrupo": str,
                    "subgrupo2": str,
                    "subgrupo3": str
                }
            )

            # 4. Eliminar filas vacías y verificar si hay datos (sin contar cabecera)
            df = df.dropna(how='all')  # Quita filas completamente vacías

            if df.shape[0] == 0:
                raise CustomException("El archivo no contiene datos.")

            # 5. Convertir DataFrame a lista de diccionarios
            data = df.fillna("").to_dict(orient='records')

            return data

        except CustomException as e:
            traceback.print_exc()
            print(f"Error al procesar archivo: {e}")
            raise CustomException(str(e))

    # Función para guardar las referencias en la base de datos
    def guardar_referencias(self, data: dict):
        """ Api que guarda las referencias. """
        try:
            referencias = data.get("referencias", [])
            if not referencias:
                raise CustomException("No se proporcionaron referencias para guardar.")

            # Guardar las referencias en la base de datos
            refs = self.querys.guardar_referencias(referencias)
            
            if not refs["encontrados"] and refs["insertados"] > 0:
                return self.tools.output(200, "Referencias guardadas con éxito.",)

            if refs["encontrados"] and refs["insertados"] > 0:
                mensaje = f"Referencias guardadas con éxito. Las siguientes referencias ya existen: {', '.join(refs['encontrados'])}"
                return self.tools.output(200, mensaje)
            
            if not refs["encontrados"] and not refs["insertados"] > 0:
                mensaje = f"No hay referencias para guardar."
                return self.tools.output(200, mensaje)
            
            if refs["encontrados"] and not refs["insertados"] > 0:
                mensaje = f"Las siguientes referencias ya existen: {', '.join(refs['encontrados'])}"
                return self.tools.output(200, mensaje)

        except CustomException as e:
            traceback.print_exc()
            print(f"Error al guardar referencias: {e}")
            raise CustomException(str(e))
