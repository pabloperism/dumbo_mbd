import csv

from dumbo import main

def load_comunidades_provincias(comunidades_file):
 	comunidades = {}
 	try:
 		# Read table
 		with open(comunidades_file) as f:
 			reader = csv.reader(f, delimiter=';', quotechar='"', doublequote=False)
			reader.next()
	 		for line in reader:
	 			comunidades[line[0]] = line[1]
	
	except:
		pass

	return comunidades


def parse_contratos_municipio_mapper(key, value):
		"""
			Parse table
		"""

	try:
        codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres = value.split(';')
        yield (provincia), (contratos_mujeres, contratos_hombres)

		except:
            pass


class join_comunidades_provincias_contratos_reduce(key, values):
   def __init__(self):
        self.provincia = load_comunidades_provincias('./Comunidades_y_provincias.txt')

    def __call__(self, key, value):
        try:
		    acc_mujeres = 0
		    acc_hombres = 0

    		Comunidad_Autonoma = key[:]

		    for v in values:
		        total_contratos_mujeres, total_contratos_hombres = v[:]
    		    acc_mujeres += int(total_contratos_hombres)
    		    acc_hombres += int(total_contratos_hombres)

    	        if contratos_mujeres > 0 and provincia in self.provincia:
            	    total_contratos_mujeres += int(contratos_mujeres)

    	        if contratos_hombres > 0 and provincia in self.provincia:
            	    total_contratos_hombres += int(contratos_hombres)


    yield Comunidad_Autonoma, (acc_mujeres, acc_hombres)


def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(parse_contratos_municipio_mapper, join_comunidades_provincias_contratos_reduce, opts=inout_opts)


if __name__ == "__main__":
    main(runner)
