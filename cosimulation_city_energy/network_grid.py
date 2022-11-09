import mosaik


def get_grid(NET, pandapower):
    if NET == "VorStadtNetz":
        grid = pandapower.VorStadtNetz(
            num_of_PV=0, num_of_prosumer=0).children  # only consumer
    elif NET == "VorStadtNetzMit10PV":
        grid = pandapower.VorStadtNetz(
            num_of_PV=14, num_of_prosumer=0).children
    elif NET == "VorStadtNetzMitProsumer":
        grid = pandapower.VorStadtNetz(
            num_of_PV=0, num_of_prosumer=14).children
    elif NET == "VorStadtNetzMitProsumerundPV":
        grid = pandapower.VorStadtNetz(num_of_PV=7, num_of_prosumer=7).children
    elif NET == "LandNetzMitPV":
        grid = pandapower.LandNetz(num_of_PV=1, num_of_prosumer=1).children
    elif NET == "MieterStromNetz":
        grid = pandapower.MieterStromNetz(
            num_of_PV=1, num_of_prosumer=1).children
    elif NET == "DorfNetz":
        grid = pandapower.DorfNetz(
            num_of_PV=0, num_of_prosumer=0).children  # only consumer
    elif NET == "DorfNetzMit2PV":
        grid = pandapower.DorfNetz(num_of_PV=2, num_of_prosumer=0).children
    elif NET == "DorfNetzMit5PV":
        grid = pandapower.DorfNetz(num_of_PV=5, num_of_prosumer=0).children
    elif NET == "DorfNetzMit7PV":
        grid = pandapower.DorfNetz(num_of_PV=7, num_of_prosumer=0).children
    elif NET == "DorfNetzMit10PV":
        grid = pandapower.DorfNetz(num_of_PV=10, num_of_prosumer=0).children
    elif NET == "DorfNetzMit5Prosumer":
        grid = pandapower.DorfNetz(num_of_PV=0, num_of_prosumer=5).children
    elif NET == "DorfNetzMitPVundProsumer":
        grid = pandapower.DorfNetz(num_of_PV=4, num_of_prosumer=4).children
    elif NET == "DemoNetz":
        grid = pandapower.DemoNetz(num_of_PV=0, num_of_prosumer=0).children
    return grid
