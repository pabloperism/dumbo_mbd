from dumbo import main, MultiMapper, primary, secondary, JoinReducer


def parse_comunidades_provincias_map(key, value):
    """
    Parse table
    """
    try:
        comunidad, provincia = value.split(';')
        yield (comunidad), (provincia)
    except:
        pass

def parse_contratos_municipios_map(key, value):
    """
    Parse table
    """
    try:
        codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres = value.split(';')
        yield (provincia), (int(contratos_mujeres),int(contratos_hombres))
    except:
        pass

class Join_comunidades_contratos_reduce(JoinReducer):
    def __init__(self):
        super(Join_comunidades_contratos_reduce, self).__init__()

    def primary(self, key, values):
        self.provincia_cache = {}
        for v in values:
            self.provincia_cache[(key[0], v[1])] = v[1]

    def secondary(self, key, values):
        acc_mujeres = 0
        acc_hombres = 0
        for v in values:
            contratos_mujeres, contratos_hombres = v[:]
                if contratos_mujeres > 0 and (value[1]) in self.provincia_cache:
                acc_mujeres += int(self.provincia_cache[(value[1], 'contratos_mujeres')])

                if contratos_hombres > 0 and (value[1]) in self.provincia_cache:
                acc_hombres += int(self.provincia_cache[(value[1], 'contratos_hombres')])

        # Emit values
        yield key, (acc_hombres, acc_mujeres)

    def secondary_blocked(self, b):
        if self._key != b:
            self.country_cache = {}
        return False

def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    multimap = MultiMapper()
    multimap.add("comunidad", primary(parse_comunidades_provincias_map))
    multimap.add("provincia", secondary(parse_contratos_municipios_map))
    o1 = job.additer(multimap, Join_athlete_country_medals_reduce, opts=inout_opts)
