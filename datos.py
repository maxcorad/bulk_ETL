# importar SparkSession y tipos y funciones de spark.sql
from spark import spark
from pyspark.sql import types
from pyspark.sql import functions as F


class Datos(object):
    """
    [ACTUALIZAR]
    Objeto que aglutina el proceso de ETL de datos almacenados en archivos mediante
    el uso de Spark DataFrames.

    Cada instancia requiere aportar la ruta del directorio del proyecto así como:
    - carpeta que contiene los archivos con los datos sin procesar ('data' por defecto)
    - carpeta que contiene los scripts de transformación ('scripts' por defecto).
    Si no se suministra ruta a proyecto, las rutas a datos y scripts deberán ser absolutas.

    Los métodos invocados durante el proceso de ETL añaden nuevos atributos a los objetos,
    incluyendo nombre y DataFrame con los datos procesados por Spark. Al finalizar dicho proceso,
    se generará el directorio 'data_output' que contendrá las carpetas con los datos de salida.

    Atributos:
        project_dir     Directorio donde se ubican:
                        - la carpeta con los datos en bruto,
                        - la carpeta con los scripts de transformación,
                        - las carpetas con los datos de salida.
        raw_dir         Directorio con los archivos con los datos originales.
        input_file      Ruta completa al archivo con los datos en bruto.
        name            Nombre asignado al conjunto de datos, derivado del nombre de archivo.
        df              DataFrame que almacena los datos del archivo y sus transformaciones.
        script_dir      Directorio con los scripts de transformación.
        script_file     Nombre del script de transformación del DF con los datos de cada archivo.
        output_os       Ruta donde se ubicarán los datos procesados en el sistema operativo.
        output_hdfs     Ruta en HDFS donde se ubicarán los datos procesados.
    """

    df = None

    def __init__(self, file_name, raw_folder="data/", script_folder="scripts/", project_dir="/tmp/ETL/", hdfs_dir="/data_out/"):
        """
        Inicializa objeto con la ruta al proyecto, si existe,
        ruta a la carpeta con los datos originales
        y ruta a la carpeta con los scripts de transformación.
        """

        self.name = file_name
        self.project_dir = project_dir
        self.raw_dir = raw_folder if raw_folder.startswith("/") else project_dir + raw_folder
        self.script_dir = script_folder if script_folder.startswith("/") else project_dir + script_folder
        self.input_file = f"file:{self.raw_dir}{self.name}.txt"
        self.script_file = f"{self.script_dir}{self.name}.py"
        self.output_hdfs = f"hdfs:{hdfs_dir}{self.name}"
        self.output_os = f"file:{self.project_dir}data_output/{self.name}"
        print(f"objeto {self.name} creado a partir de {self.input_file}.")

    def __eq__(self, other):
        return (self.input_file == other.input_file) if isinstance(other, type(self)) else NotImplemented

    def __hash__(self):
        return hash(self.name)

    def extract_data(self):
        """
        Asigna a un objeto de la clase 'Datos' la propiedad 'df' en la que carga los datos del archivo correspondiente.
        """
        print(f"extrayendo {self.name}")
        self.df = spark.read.format("csv") \
            .option("header", "true").option("sep", "|") \
            .load(self.input_file)
        print(f"{self.name} DF creado con éxito")
        return self

    def transform_data(self):
        """ Ejecuta transformaciones sobre el df almacenado en el objeto aplicando script externo. """
        print(f"transformando {self.name}")
        # pasar a minúsculas todos los nombres de las columnas
        for col_name in self.df.columns:
            self.df = self.df.withColumnRenamed(col_name, col_name.lower())
        # cargar script transformación individualizado
        exec(open(self.script_file).read())

    def load_data(self, hdfs=False):
        """ Establece ruta de destino de los datos procesados y guarda el df resultante de la transformación. """
        print(f"guardando {self.name}")
        output_dir = self.output_hdfs if hdfs else self.output_os
        self.df.write.format("csv").option("header", "true") \
            .mode("overwrite").save(output_dir)


def format_decimals(col_name):
    """ Cambia el separador decimal de ',' a '.' para preparar el paso de string a float. """
    return F.regexp_replace(col_name, ",", "\.")
