import logging
import mosaik_api
import pandapower as pp
import pandapower.networks as pn

def create_mieterstrom(net, bus_building, n):
    """
    bus_building: bus to which the rooms are connected
    n: number of rooms in the building
    """

    bus_room = {}
    for x in range(0, n):
        bus_room[x] = pp.create_bus(net, vn_kv=0.4, type="n", geodata=(0, x/10))

    switch_room = {}
    for x in range(0, n):
        switch_room[x] = pp.create_switch(net, bus_building, bus_room[x], et="b", closed=True)

    line_room = {}
    for x in range(0, n):
        line_room[x] = pp.create_line(net, bus_building, bus_room[x], length_km=0.7, std_type="NAYY 4x50 SE")

    measurement_v_room = {}
    for x in range(0, n):
        measurement_v_room[x] = pp.create_measurement(net, meas_type="v", element_type="bus", value=0.0051,
                                                      element=bus_room[x], std_dev=0.65)

    measurement_p_room = {}
    for x in range(0, n):
        measurement_p_room[x] = pp.create_measurement(net, meas_type="p", element_type="bus", value=0.0051,
                                                      element=bus_room[x], std_dev=0.65)

    measurement_q_room = {}
    for x in range(0, n):
        measurement_q_room[x] = pp.create_measurement(net, meas_type="q", element_type="bus", value=0.0051,
                                                      element=bus_room[x], std_dev=0.65)

    measurement_i_room = {}
    for x in range(0, n):
        measurement_i_room[x] = pp.create_measurement(net, meas_type="i", element_type="line", value=0.0051,
                                                      element=line_room[x], std_dev=0.65, side="to")

    load_room = {}
    for x in range(0, n):
        load_room[x] = pp.create_load(net, bus_room[x], p_mw=0.0051)

    #pv = pp.create_sgen(net, bus_building, p_mw=0.06)
    return net

logger = logging.getLogger('pandapower.mosaik')

META = {
    'models': {
        'VorStadtNetz': {
            'public': True,
            'params': ['num_of_PV', 'num_of_prosumer'],  # Total size = #PV + #Prosumer + #Consumer
            'attrs': [],
        },
        'LandNetz': {
            'public': True,
            'params': ['num_of_PV', 'num_of_prosumer'],  # Total size = #PV + #Prosumer + #Consumer
            'attrs': [],
        },
        'DorfNetz': {
            'public': True,
            'params': ['num_of_PV', 'num_of_prosumer'],  # Total size = #PV + #Prosumer + #Consumer
            'attrs': [],
        },
        'DemoNetz': {
            'public': True,
            'params': ['num_of_PV', 'num_of_prosumer'],  # Total size = #PV + #Prosumer + #Consumer
            'attrs': [],
        },
        'MieterStromNetz': {
            'public': True,
            'params': ['num_of_PV', 'num_of_prosumer'],  # Total size = #PV + #Prosumer + #Consumer
            'attrs': [],
        },
        'Consumer': {  # contains load
            'public': False,
            'params': [],
            'attrs': ['p_mw',  # resulting active power demand after scaling and
                      # after considering voltage dependence [MW]
                      'q_mvar'],  # resulting reactive power demand after scaling and
                                  # after considering voltage dependence [MVar]
        },
        'PV': {  # contains static generator
            'public': False,
            'params': [],
            'attrs': ['p_mw',  # resulting active power demand after scaling [MW]
                      'q_mvar'],  # resulting reactive power demand after scaling [MVar]
        },
        'Prosumer': {  # contains load, static generator
            'public': False,
            'params': [],
            'attrs': ['p_mw_load',
                      'q_mvar_load',
                      'p_mw_pv',
                      'q_mvar_pv',
                      'p_mw_total',
                      'q_mvar_total'],
        },
        'Trafo': {  # contains Trafo
            'public': False,
            'params': [],
            'attrs': ['p_hv_mw',  # active power flow at the high voltage Trafo bus [MW]
                      'q_hv_mvar',  # reactive power flow at the high voltage Trafo bus [MVar]
                      'p_lv_mw',  # active power flow at the low voltage Trafo bus [MW]
                      'q_lv_mvar',  # reactive power flow at the low voltage Trafo bus [MVar]
                      'pl_mw',  # active power losses of the Trafo [MW]
                      'ql_mvar',  # reactive power consumption of the Trafo [Mvar]
                      'i_hv_ka',  # current at the high voltage side of the Trafo [kA]
                      'i_lv_ka',  # current at the low voltage side of the Trafo [kA]
                      'loading_percent'],  # load utilization relative to rated power [%]
        },
        'Bus': {  # contains bus
            'public': False,
            'params': [],
            'attrs': ['vm_pu',  # voltage magnitude [p.u]
                      'va_degree',  # voltage angle [degree]
                      'p_mw',  # resulting active power demand [MW]
                      'q_mvar'],  # resulting reactive power demand [Mvar]
        },
        'Line': {  # contains line
            'public': False,
            'params': [],
            'attrs': ['p_from_mw',  # active power flow into the line at “from” bus [MW]
                      'q_from_mvar',  # reactive power flow into the line at “from” bus [MVar]
                      'p_to_mw',  # active power flow into the line at “to” bus [MW]
                      'q_to_mvar',  # reactive power flow into the line at “to” bus [MVar]
                      'pl_mw',  # active power losses of the line [MW]
                      'ql_mvar',  # reactive power consumption of the line [MVar]
                      'i_from_ka',  # Current at to bus [kA]
                      'i_to_ka',  # Current at from bus [kA]
                      'i_ka',  # Maximum of i_from_ka and i_to_ka [kA]
                      'loading_percent'],  # line loading [%]
        },
        'ExtGrid': {
            'public': False,
            'params': [],
            'attrs': ['p_mw',  # active power supply at the external grid [MW]
                      'q_mvar'],  # reactive power supply at the external grid [MVar]
        },
    },
}


class PandapowerMosaik(mosaik_api.Simulator):
    def __init__(self):
        super().__init__(META)
        self.net = None
        self.step_size = None
        self.model = None

        self._entities = {}
        self._relations = []  # List of pair-wise related entities (IDs)
        self._prosumer = {}
        self._pv_pos = []
        self._prosumer_pos = []
        self._prosumer_pv_pos = []


    def init(self, sid, step_size):
        self.step_size = step_size
        logger.debug('Power flow will be computed every %d seconds.' %
                     step_size)
        return self.meta

    def create(self, num, model, num_of_PV =0, num_of_prosumer=0):
        if num > 1:
            raise RuntimeError('Can only create one instance of pandaPower_city.')

        if model == 'VorStadtNetz':
            self.net = pn.create_kerber_vorstadtnetz_kabel_1()
        elif model == 'LandNetz':
            self.net = pn.create_kerber_landnetz_freileitung_1()
        elif model == 'MieterStromNetz':
            self.net = create_mieterstrom(pn.create_kerber_landnetz_freileitung_1(),1,7)
        elif model == 'DorfNetz':
            self.net = pn.create_kerber_dorfnetz()
        elif model == 'DemoNetz':
            self.net = pn.example_simple()
        else:
            raise Exception("Model \"" + model + "\" does not exist!")

        self._pv_pos = []
        num_of_loads = self.net.load.T.shape[1]
        prosumer_added = 0
        if num_of_PV > 0:
            counter = 0
            for ind in range(0, num_of_loads):
                if ind >= ((num_of_loads / num_of_PV) * counter + 1) - 1:
                    counter += 1
                    load = self.net.load.loc[ind, :]
                    self.net.load.drop(ind, inplace=True)
                    pp.create_sgen(self.net,
                                  load.loc["bus"],
                                  0,
                                  name='PV_' + str(ind),
                                  index=ind)
                    self._pv_pos.append(ind)

        if num_of_prosumer > 0:
            counter = 0
            for ind in range(0, num_of_loads):
                if ind >= ((num_of_loads / num_of_prosumer) * counter + 1) - 3 and ind not in self._pv_pos:
                    load = self.net.load.loc[ind, :]
                    self.net.load.drop(ind, inplace=True)
                    # add gen and load to bus with specific name
                    load_index = pp.create_load(self.net,
                                  load.loc["bus"], 0, 0, 0, 0, 0,
                                  name="prosumer_load_" + str(ind), index=ind)
                    gen_index = pp.create_sgen(self.net,
                                  load.loc["bus"], 0,
                                  name="prosumer_gen_" + str(ind), index=ind)
                    self._prosumer["prosumer_" + str(counter)] = {"bus_index": load.loc["bus"],
                                                                  "load_index": load_index,
                                                                  "gen_index": gen_index}
                    self._prosumer_pos.append(load_index)
                    self._prosumer_pv_pos.append(gen_index)
                    counter += 1
                    if counter == num_of_prosumer:
                        break
            prosumer_added = counter
            assert prosumer_added == num_of_prosumer
        pvs_added = self.net.sgen.T.shape[1] - prosumer_added
        if model != 'DemoNetz' and num_of_PV > 0:
            assert pvs_added == num_of_PV

        self.net.load.sort_index(inplace=True)
        self.net.sgen.sort_index(inplace=True)

        self.model = model
        grids = []
        children = []

        high_voltage_bus = None
        # add ExtGrid
        for i in range(0, self.net.ext_grid.T.shape[1]):
            ext_bus = self.net.ext_grid.iloc[i].loc["bus"]
            relations = ['extgrid_' + str(i), 'bus_' + str(ext_bus)]

            children.append({
                'eid': 'extgrid_' + str(i),
                'type': 'ExtGrid',
                'rel': relations,
            })

        # add Trafo
        for i in range(0, self.net.trafo.T.shape[1]):
            low_voltage_bus = self.net.trafo.iloc[i].loc["lv_bus"]
            high_voltage_bus = self.net.trafo.iloc[i].loc["hv_bus"]
            relations = ['trafo_' + str(i), 'bus_' + str(low_voltage_bus)]

            children.append({
                'eid': 'trafo_' + str(i),
                'type': 'Trafo',
                'rel': relations,
            })

        # add bus
        for i in range(0, self.net.bus.T.shape[1]):
            relations = []
            if i == high_voltage_bus:
                relations = ['trafo_' + str(0), 'bus_' + str(high_voltage_bus)]
            children.append({
                'eid': 'bus_' + str(i),
                'type': 'Bus',
                'rel': relations,
            })

        # add line
        for i in range(0, self.net.line.T.shape[1]):
            from_bus = self.net.line.iloc[i].loc["from_bus"]
            to_bus = self.net.line.iloc[i].loc["to_bus"]
            relations = ['bus_' + str(from_bus), 'bus_' + str(to_bus)]

            children.append({
                'eid': 'line_' + str(i),
                'type': 'Line',
                'rel': relations,
            })

        # add consumer

        for i in range(0, self.net.load.T.shape[1]):
            name = self.net.load.iloc[i].loc["name"]
            if name is None or "prosumer" not in self.net.load.iloc[i].loc["name"]:
                connected_bus = self.net.load.iloc[i].loc["bus"]
                eid = 'consumer_' + str(i)
                relations = [eid, 'bus_' + str(connected_bus)]

                children.append({
                    'eid': eid,
                    'type': 'Consumer',
                    'rel': relations,
                })

        # add PV
        index = 0
        for i in range(0, self.net.sgen.T.shape[1]):
            name = self.net.sgen.iloc[i].loc["name"]
            if name is None or "prosumer" not in name:
                connected_bus = self.net.sgen.iloc[i].loc["bus"]
                relations = ['pv_' + str(index), 'bus_' + str(connected_bus)]

                children.append({
                    'eid': 'pv_' + str(index),
                    'type': 'PV',
                    'rel': relations,
                })

                index += 1

        for prosumer in self._prosumer.keys():
            connected_bus = self._prosumer[prosumer]["bus_index"]
            relations = [prosumer, 'bus_' + str(connected_bus)]

            children.append({
                'eid': prosumer,
                'type': 'Prosumer',
                'rel': relations,
            })

        grids.append({
            'eid': self.model,
            'type': model,
            'rel': [],
            'children': children,
        })

        return grids

    def step(self, time, inputs):
        for eid, attrs in inputs.items():
            if eid.find("prosumer_") != -1:
                self.set_prosumer_input(attrs, eid)
            elif eid.find("consumer_") != -1:
                self.set_load_input_from_attr_list(attrs, eid)
            elif eid.find("pv_") != -1:
                self.set_generator_input_from_attr_list(attrs, eid)
        pp.runpp(self.net)
        return time + self.step_size

    def set_prosumer_input(self, attrs, eid):
        for name, values in attrs.items():
            if name == "p_mw_pv":
                id = self._prosumer[eid]["gen_index"]
                self.set_generator_input(id, values)
            elif name == "p_mw_load":
                id = self._prosumer[eid]["load_index"]
                self.set_prosumer_load_input(id, values)
            else:
                raise Exception("Unexpected input name: " + name)

    def set_generator_input_from_attr_list(self, attrs, eid):
        i = int(eid.split("pv_")[1])
        id = self._pv_pos[i]
        # set all new attr for that load
        for name, values in attrs.items():
            if name == "p_mw":
                self.set_generator_input(id, values)
            else:
                raise Exception("Unexpected input name: " + name)

    def set_generator_input(self, id, values):
        gen = self.net.sgen.loc[id]

        genname = gen.loc["name"]
        self.net.sgen.drop(id, inplace=True)
        p_mw = sum(float(v) for v in values.values())
        pp.create_sgen(self.net,
                       gen.loc["bus"],
                       p_mw,
                       name=genname,
                       index=id)
        self.net.sgen.sort_index(inplace=True)

    def set_load_input_from_attr_list(self, attrs, eid):
        id = int(eid.split("consumer_")[1])
        # set all new attr for that load
        for name, values in attrs.items():
            if name == "p_mw":
                self.set_load_input(id, values)
            else:
                raise Exception("Unexpected input name: " + name)

    def set_prosumer_load_input(self, id, values):
        load = self.net.load.loc[id]
        p_mw = sum(float(v) for v in values.values())
        self.replace_load(load, id, p_mw)

    def set_load_input(self, id, values):
        intern_id = self.get_correct_id_for_consumers(id)
        load = self.net.load.loc[intern_id]
        p_mw = sum(float(v) for v in values.values())
        self.replace_load(load, intern_id, p_mw)

    def replace_load(self, load, intern_id, p_mw=None):
        loadname = load.loc["name"]

        if p_mw is None:
            p_mw = load.loc["p_mw"]

        self.net.load.drop(intern_id, inplace=True)
        pp.create_load(self.net,
                       load.loc["bus"],
                       p_mw,
                       load.loc["q_mvar"],
                       load.loc["const_z_percent"],
                       load.loc["const_i_percent"],
                       load.loc["sn_mva"],
                       name= loadname,
                       index=intern_id,
                       scaling=load.loc["scaling"],
                       in_service=load.loc["in_service"])
        self.net.load.sort_index(inplace=True)

    def get_correct_id_for_consumers(self, id):
        # get real id
        missing_elements = 0
        jump_list = self._pv_pos
        for ele in jump_list:
            if ele <= id + missing_elements:
                missing_elements += 1
            else:
                break
        intern_id = id + missing_elements
        return intern_id

    def get_data(self, outputs):
        data = {}
        for eid, attrs in outputs.items():
            for attr in attrs:
                if eid.find("prosumer_") != -1:
                    val = self.get_prosumer_attribute(attr, eid)
                elif eid.find("consumer_") != -1:
                    val = self.get_load_attribute(attr, eid)
                elif eid.find("pv_") != -1:
                    val = self.get_pv_attribute(attr, eid)
                elif eid.find("line_") != -1:
                    val = self.get_line_attribute(attr, eid)
                elif eid.find("bus_") != -1:
                    val = self.get_bus_attribute(attr, eid)
                elif eid.find("trafo_") != -1:
                    val = self.get_trafo_attribute(attr, eid)
                elif eid.find("extgrid_") != -1:
                    val = self.get_extgrid_attribute(attr, eid)
                else:
                    raise Exception("Unexpected output demanded: name: " + eid + " attr: " + attr)
                data.setdefault(eid, {})[attr] = val
        return data

    def get_prosumer_attribute(self, attr, eid):
        if attr in ("p_mw_pv", "q_mvar_pv"):
            id = self._prosumer[eid]["gen_index"]
            gen = self.net.res_sgen.loc[id, :]
            attr = attr[:-3]  # remove "_pv"
            val = -gen.loc[attr]
        elif attr in ("p_mw_load", "q_mvar_load"):
            id = self._prosumer[eid]["load_index"]
            load = self.net.res_load.loc[id, :]
            attr = attr[:-5]  # remove "_load"
            val = load.loc[attr]
        elif attr in ("p_mw_total", "q_mvar_total"): # TODO correct?
            attr = attr[:-6]  # remove "_total"
            id = self._prosumer[eid]["gen_index"]
            gen = self.net.res_sgen.loc[id, :]
            val = - gen.loc[attr]
            id = self._prosumer[eid]["load_index"]
            load = self.net.res_load.loc[id, :]

            val =  val + load.loc[attr]
        return val

    def get_extgrid_attribute(self, attr, eid):
        id = int(eid.split("extgrid_")[1])

        if attr in ("p_mw", "q_mvar"):
            extgrid = self.net.res_ext_grid.iloc[id]
        else:
            extgrid = self.net.ext_grid.iloc[id]
        val = extgrid.loc[attr]
        return val

    def get_trafo_attribute(self, attr, eid):
        id = int(eid.split("trafo_")[1])

        if (attr in ("p_hv_mw", "q_hv_mvar", "p_lv_mw",
                     "q_lv_mvar", "pl_mw", "ql_mvar",
                     "i_hv_ka", "i_lv_ka", "loading_percent")):
            Trafo = self.net.res_trafo.iloc[id]
        else:
            Trafo = self.net.Trafo.iloc[id]
        val = Trafo.loc[attr]
        return val

    def get_line_attribute(self, attr, eid):
        id = int(eid.split("line_")[1])

        if attr in ('p_from_mw', 'q_from_mvar', 'p_to_mw', 'q_to_mvar', 'pl_mw', 'ql_mvar',
                    'i_from_ka', 'i_to_ka', 'i_ka', 'loading_percent'):
            line = self.net.res_line.iloc[id]
        else:
            line = self.net.line.iloc[id]
        val = line.loc[attr]
        return val

    def get_load_attribute(self, attr, eid):
        id = int(eid.split("consumer_")[1])
        intern_id = self.get_correct_id_for_consumers(id)
        load = self.net.res_load.loc[intern_id]
        val = load.loc[attr]
        return val

    def get_pv_attribute(self, attr, eid):
        i = int(eid.split("pv_")[1])
        id = self._pv_pos[i]
        gen = -self.net.res_sgen.loc[id]
        val = gen.loc[attr]
        return val

    def get_bus_attribute(self, attr, eid):
        id = int(eid.split("bus_")[1])

        if attr in ('vm_pu', 'va_degree', 'p_mw', 'q_mvar'):
            bus = self.net.res_bus.iloc[id]
        else:
            bus = self.net.bus.iloc[id]
        val = bus.loc[attr]
        return val


def main():
    return mosaik_api.start_simulation(PandapowerMosaik())


if __name__ == '__main__':
    main()
