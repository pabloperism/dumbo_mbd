import csv

def load_comunidades_provincias(comunidades_file):
   comunidad_provincia = {}
   try:
      # Read table - comunidades|provincia
      with open(comunidades_file) as f:
         reader = csv.reader(f, delimiter=';', quotechar='"', doublequote=False)
         reader.next()
         for line in reader:
            comunidad_provincia[line[1]] = line[0]
   
   except:
      pass

   return comunidad_provincia

class Parse_contratos_municipio_mapper:
    def __init__(self):
        self.provincia = load_comunidades_provincias('./Comunidades_y_provincias.txt')

    def __call__(self, key, value):
        try:
            total_contratos_mujeres = 0
            total_contratos_hombres = 0
            
            codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres = value.split(';')
            int(contratos_hombres)
            int(contratos_mujeres)
            
            comunidad = self.provincia.get(provincia)
            
            if contratos_mujeres > 0 and provincia in comunidad:
                total_contratos_mujeres += int(contratos_mujeres)

            if contratos_hombres > 0 and provincia in comunidad:
                total_contratos_hombres += int(contratos_hombres)


            yield (comunidad), (total_contratos_mujeres, total_contratos_hombres)

        except:
            pass

def join_comunidades_provincias_contratos_reduce(key, values):
    acc_mujeres = 0
    acc_hombres = 0

    comunidad = key[:]

    for v in values:
        total_contratos_mujeres, total_contratos_hombres = v[:]
        acc_mujeres += int(total_contratos_mujeres)
        acc_hombres += int(total_contratos_hombres)

    yield comunidad, (acc_mujeres, acc_hombres)

from dumbo import main

def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(Parse_contratos_municipio_mapper, join_comunidades_provincias_contratos_reduce, opts=inout_opts)


if __name__ == "__main__":
    main(runner)
