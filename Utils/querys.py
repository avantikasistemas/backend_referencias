from Utils.tools import Tools, CustomException
from sqlalchemy import text
import traceback
from datetime import datetime

class Querys:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.query_params = dict()

    # Query para validar el año de la proyección si existe.
    def guardar_referencias(self, referencias: list):

        try:
            # Convertir la lista de referencias a un formato adecuado para la inserción
            referencia_encontrada = list()
            cont = 0
            for ref in referencias:
                sql = """
                SELECT * FROM referencias WHERE codigo = :codigo AND ref_anulada = 'N'
                """
                result = self.db.execute(text(sql), {"codigo": ref["codigo"]}).fetchone()
                if result:
                    referencia_encontrada.append(ref["codigo"])
                    
                else:
                    maneja_lote = ''
                    if ref["maneja_lotes"] == 'SI':
                        maneja_lote = 'S'
                    elif ref["maneja_lotes"] == 'NO':
                        maneja_lote = 'N'
                        
                    maneja_series = ''
                    if ref["maneja_series"] == 'SI':
                        maneja_series = 'S'
                    elif ref["maneja_series"] == 'NO':
                        maneja_series = 'N'
                        
                    controlado = ''
                    if ref["controlado"] == 'SI':
                        controlado = 'S'
                    elif ref["controlado"] == 'NO':
                        controlado = 'N'

                    fecha_creacion = datetime.now()

                    # Realizar la inserción en la base de datos
                    self.db.execute(
                        text("""
                        INSERT INTO referencias (codigo, descripcion, generico, clase, contable, grupo, subgrupo, nit, porcentaje_iva, costo_unitario, maneja_inventario, und_1, can_1, und_2, can_2, und_3, can_3, und_vta, und_com, porcentaje_iva_compra, fecha_creacion, maneja_lotes, subgrupo2, subgrupo3, controlado, maneja_series, codigo_enlace, calificacion_abc)
                        VALUES (:codigo, :descripcion, :generico, :clase, :contable, :grupo, :subgrupo, :nit, :porcentaje_iva, :costo_unitario, :maneja_inventario, :und_1, :can_1, :und_2, :can_2, :und_3, :can_3, :und_vta, :und_com, :porcentaje_iva_compra, :fecha_creacion, :maneja_lotes, :subgrupo2, :subgrupo3, :controlado, :maneja_series, :codigo_enlace, :calificacion_abc)
                        """), {
                            "codigo": str(ref["codigo"]).strip(),
                            "descripcion": str(ref["descripcion"]).strip(),
                            "generico": str(ref["generico"]).strip(),
                            "clase": str(ref["clase"]).strip(),
                            "contable": str(ref["contable"]).strip(),
                            "grupo": str(ref["grupo"]).strip() if ref["grupo"] else None,
                            "subgrupo": str(ref["subgrupo"]).strip() if ref["subgrupo"] else None,
                            "nit": str(ref["nit"]).strip() if ref["nit"] else None,
                            "porcentaje_iva": ref.get("porcentaje_iva", 0),
                            "costo_unitario": ref.get("costo_unitario", 0),
                            "maneja_inventario": ref.get("maneja_inventario", ''),
                            "und_1": ref.get("und_1", ''),
                            "can_1": ref.get("can_1", ''),
                            "und_2": ref.get("und_2", ''),
                            "can_2": ref.get("can_2", ''),
                            "und_3": ref.get("und_3", ''),
                            "can_3": ref.get("can_3", ''),
                            "und_vta": ref.get("und_vta", ''),
                            "und_com": ref.get("und_com", ''),
                            "porcentaje_iva_compra": ref.get("porcentaje_iva_compra", 0),
                            "fecha_creacion": fecha_creacion,
                            "maneja_lotes": maneja_lote,
                            "subgrupo2": str(ref["subgrupo2"]).strip() if ref["subgrupo2"] else None,
                            "subgrupo3": str(ref["subgrupo3"]).strip() if ref["subgrupo3"] else None,
                            "controlado": controlado,
                            "maneja_series": maneja_series,
                            "codigo_enlace": ref.get("codigo_enlace", ''),
                            "calificacion_abc": ref.get("calificacion_abc", '')
                        }
                    )
                    self.db.commit()
                    cont += 1
                    
                    # Verificar si el código ya existe en referencias_des_adi
                    sql_check = """
                        SELECT 1 FROM referencias_des_adi WHERE codigo = :codigo
                    """
                    result_adi = self.db.execute(
                        text(sql_check), {
                            "codigo": str(ref["codigo"]).strip()
                        }
                    ).fetchone()

                    if result_adi:
                        # Si existe, actualizar descripcion_2 y descripcion_3
                        sql_update = """
                            UPDATE referencias_des_adi
                            SET descripcion_2 = :descripcion_2, descripcion_3 = :descripcion_3
                            WHERE codigo = :codigo
                        """
                        self.db.execute(
                            text(sql_update), {
                                "codigo": str(ref["codigo"]).strip(),
                                "descripcion_2": str(ref["descripcion_2"]).strip() if ref["descripcion_2"] else '',
                                "descripcion_3": str(ref["descripcion_3"]).strip() if ref["descripcion_3"] else ''
                            }
                        )
                    else:
                        # Si no existe, insertar nuevo registro
                        sql2 = """
                            INSERT INTO referencias_des_adi (codigo, descripcion_2, descripcion_3)
                            VALUES (:codigo, :descripcion_2, :descripcion_3)
                        """
                        self.db.execute(
                            text(sql2), {
                                "codigo": str(ref["codigo"]).strip(),
                                "descripcion_2": str(ref["descripcion_2"]).strip() if ref["descripcion_2"] else '',
                                "descripcion_3": str(ref["descripcion_3"]).strip() if ref["descripcion_3"] else ''
                            }
                        )
                    self.db.commit()
                    
                    # Insertar en referencias_imp
                    sql_imp = """
                        INSERT INTO referencias_imp (codigo, costo_unitario_FOB, requiere_registro)
                        VALUES (:codigo, :costo_unitario, 'N')
                    """
                    self.db.execute(
                        text(sql_imp), {
                            "codigo": str(ref["codigo"]).strip(),
                            "costo_unitario": ref.get("costo_unitario", 0)
                        }
                    )
                    self.db.commit()
                    

            return {"insertados": cont, "encontrados": referencia_encontrada}

        except Exception as ex:
            traceback.print_exc()
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()
