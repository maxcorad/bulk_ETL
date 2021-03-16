#
from os import scandir      # para el manejo de directorios del SO
from sys import argv        # para el manejo de argumentos
from datos import Datos     # importa la clase 'Datos'


def process_project(raw_folder, script_folder, project_dir=""):
    """
    Obtiene la lista de archivos .txt de raw_folder en project_dir y genera un set
    con los correspondientes objetos de clase 'Datos'.
    """
    data_files = [f.name.split('.')[0] for f in scandir(project_dir + raw_folder) if f.name.endswith(".txt")]
    datos_set = {Datos(file_name, raw_folder, script_folder, project_dir) for file_name in data_files}
    return datos_set


def main(*args):
    """ Realiza proceso de ETL en un conjunto de datos con Spark. """

    # convertir los argumentos recibidos por el script en parámetros apropiados para process_project()
    args = [arg if arg.endswith('/') else arg + '/' for arg in args]
    kwargs = {arg.split("=")[0]: arg.split("=")[1] for arg in args if "=" in arg}
    args = [arg for arg in args if "=" not in arg]

    # procesar archivos a conjunto de objetos de clase 'Datos'
    data_set = process_project(*args, **kwargs)

    # ejecutar ETL
    for data in data_set:
        Datos.extract_data(data)
        Datos.transform_data(data)
        Datos.load_data(data)


if __name__ == '__main__':
    main(*argv[1:])
