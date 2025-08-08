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
            
            if 'actualizar' in nombre:
                referencias = self.procesar_archivo_actualizar(archivo)
                # Retornamos la información.
                return self.tools.output(200, "Archivo cargado con éxito.", referencias)

            referencias = self.procesar_archivo(archivo)

            # Retornamos la información.
            return self.tools.output(200, "Archivo cargado con éxito.", referencias)

        except CustomException as e:
            traceback.print_exc()
            print(f"Error al cargar archivo: {e}")
            raise CustomException(str(e))

    # Función para procesar el archivo excel
    def procesar_archivo(self, archivo):
        """ Procesa el archivo de referencias. """
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
                    "codigo": str,
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

    # Función para procesar el archivo excel de actualización
    def procesar_archivo_actualizar(self, archivo):
        """ Procesa el archivo de referencias. """
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
                    "grupo": str,
                    "subgrupo": str,
                    "subgrupo2": str,
                    "subgrupo3": str,
                    "und_1": str,
                    "can_1": str,
                    "und_vta": str,
                    "und_com": str,
                    "tipo_1": str,
                    "tipo_2": str,
                    "tipo_3": str,
                    "tipo_4": str,
                    "tipo_5": str,
                    "tipo_6": str,
                    "tipo_7": str,
                    "tipo_8": str,
                    "tipo_9": str,
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
            
            if not refs["encontrados"] and refs["insertados"] > 0 and refs["anulados"]:
                mensaje = f"""Referencias guardadas con éxito. \n
                    Las siguientes referencias existen pero están anuladas: \n
                    {', '.join(refs['anulados'])}
                """
                return self.tools.output(200, mensaje)

            if refs["encontrados"] and refs["insertados"] > 0:
                mensaje = f"""
                    Referencias guardadas con éxito. \n
                    Las siguientes referencias ya existen: \n
                    {', '.join(refs['encontrados'])}"""
                return self.tools.output(200, mensaje)
            
            if not refs["encontrados"] and not refs["insertados"] > 0 and refs["anulados"]:
                mensaje = f"""
                    No hay referencias para guardar.\n
                    Las siguientes referencias existen pero están anuladas: \n
                    {', '.join(refs['anulados'])}
                """
                return self.tools.output(200, mensaje)
            
            if refs["encontrados"] and not refs["insertados"] > 0 and refs["anulados"]:
                mensaje = f"""No se insertaron nuevas referencias. \n
                Las siguientes referencias ya existen: \n
                {', '.join(refs['encontrados'])} \n
                Las siguientes referencias existen pero están anuladas: \n
                {', '.join(refs['anulados'])}
                """
                return self.tools.output(200, mensaje)
            
            if refs["encontrados"] and not refs["insertados"] > 0 and not refs["anulados"]:
                mensaje = f"""No se insertaron nuevas referencias. \n
                Las siguientes referencias ya existen: \n
                {', '.join(refs['encontrados'])}
                """
                return self.tools.output(200, mensaje)

            return self.tools.output(
                200, 
                f"""Referencias insertadas: {refs["insertados"]}. \n
                Referencias existentes: {', '.join(refs['encontrados'])} \n
                Referencias anuladas: {', '.join(refs['anulados'])}
                """
            )

        except CustomException as e:
            traceback.print_exc()
            print(f"Error al guardar referencias: {e}")
            raise CustomException(str(e))

    # Función para actualizar las referencias en la base de datos
    def actualizar_referencias(self, data: dict):
        """ Api que actualiza las referencias. """
        try:
            referencias = data.get("referencias", [])
            campos = data.get("campos", {})
            if not referencias:
                raise CustomException("No se proporcionaron referencias para actualizar.")
            
            self.construir_update(referencias, campos)

            
            return self.tools.output(200, "ok", data)

        except CustomException as e:
            traceback.print_exc()
            print(f"Error al actualizar referencias: {e}")
            raise CustomException(str(e))

    # Construye las consultas de actualización para las referencias.
    def construir_update(self, referencias, campos):
        
        # Mapeo entre nombres recibidos y nombres reales en la base de datos
        field_mapping = {
            "unidad_venta": "und_vta",
            "unidad_compra": "und_com",
            "tipo1": "tipo_1",
            "tipo2": "tipo_2",
            "tipo3": "tipo_3",
            "tipo4": "tipo_4",
            "tipo5": "tipo_5",
            "tipo6": "tipo_6",
            "tipo7": "tipo_7",
            "tipo8": "tipo_8",
            "tipo9": "tipo_9",
            "bloqueo": "ref_anulada",
        }

        for ref in referencias:
            set_referencias = []
            set_des_adi = []
            set_imp = []
            params_referencias = {}
            params_des_adi = {}
            params_imp = {}

            codigo = ref.get("codigo")
            nit = ref.get("nit")

            for campo, actualiza in campos.items():
                if not actualiza:
                    continue

                # Validación de campos especiales por tabla
                if campo == "und_y_cant1":
                    set_referencias.append("und_1 = :und_1")
                    set_referencias.append("can_1 = :can_1")
                    params_referencias["und_1"] = ref.get("und_1")
                    params_referencias["can_1"] = ref.get("can_1")

                elif campo == "descripcion2":
                    set_des_adi.append("descripcion_2 = :descripcion_2")
                    params_des_adi["descripcion_2"] = ref.get("descripcion_2")

                elif campo == "descripcion3":
                    set_des_adi.append("descripcion_3 = :descripcion_3")
                    params_des_adi["descripcion_3"] = ref.get("descripcion_3")

                elif campo == "precio_fob":
                    set_imp.append("costo_unitario_FOB = :precio_fob")
                    params_imp["precio_fob"] = ref.get("precio_fob")

                elif campo == "proveedor":
                    continue

                else:
                    # Mapeamos el campo si es necesario
                    campo_bd = field_mapping.get(campo, campo)
    
                    # Obtener el valor directamente del JSON usando la clave original
                    valor = ref.get(campo_bd)

                    # Transformaciones específicas
                    if campo == "bloqueo":
                        valor_original = ref.get("bloqueo")  # usamos el nombre del formulario, no el mapeado
                        valor = "S" if valor_original == "SI" else "N"
                    else:
                        valor = ref.get(campo_bd)

                    # Agregar al SET y parámetros
                    set_referencias.append(f"{campo_bd} = :{campo_bd}")
                    params_referencias[campo_bd] = valor

            # Construcción de sentencias SQL dinámicas
            if set_referencias:
                sql_referencias = f"""
                UPDATE referencias
                SET {', '.join(set_referencias)}
                WHERE codigo = :codigo AND nit = :nit
                """.strip()
                params_referencias["codigo"] = codigo
                params_referencias["nit"] = nit
                self.querys.actualizar_referencias(
                    sql_referencias, 
                    params_referencias
                )

            if set_des_adi:
                sql_des_adi = f"""
                UPDATE referencias_des_adi
                SET {', '.join(set_des_adi)}
                WHERE codigo = :codigo
                """.strip()
                params_des_adi["codigo"] = codigo
                self.querys.actualizar_referencias(
                    sql_des_adi, 
                    params_des_adi
                )

            if set_imp:
                sql_imp = f"""
                UPDATE referencias_imp
                SET {', '.join(set_imp)}
                WHERE codigo = :codigo
                """.strip()
                params_imp["codigo"] = codigo
                self.querys.actualizar_referencias(
                    sql_imp, 
                    params_imp
                )
