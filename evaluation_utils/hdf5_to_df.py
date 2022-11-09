from typing import Dict, Iterable, List, Set, Any
import h5py
import pandas as pd
from pathlib import PurePath
import numpy as np
from pandas.core.indexes.multi import MultiIndex


class HDF5Keys:

    ENERGY_BALANCE = "energy_balance"  # The Energy Balance of the Household
    # Balance on the P2P Market (WH)
    P2P_TRADED = "p2p_traded"
    # Energy Production of the Household (WH)
    PRODUCTION = "p_mw_pv"
    # Energy Production of the Household (WH)
    CONSUMPTION = "p_mw_load"
    # Average Price in enerDAG Money Units for which the Energy was sold/bought on the P2P Market
    AVG_P2P_PRICE = "avg_p2p_price"
    # Number of trades in the P2P Market
    TRADED = "trades"
    # The Battery Charge in the time period
    CHARGE = "battery_charge"
    # The Energy Balance the Household published to the P2P Market
    PUBLISHED_BALANCE = "published_energy_balance"
    # The Disposable ENergy
    DISPOSABLE_ENERGY = "disposable_energy"


class MarketplaceKeys:
    GRID_LOAD = 'grid_power_load'
    NEIGHBORHOOD_ENERGY_BALANCE = "total"
    NEIGHBORHOOD_DISP_ENERGY = "total_disposable_energy"
    NEIGHBORHOOD_TRADED = "trades"


def get_rust_sim_entities(file: h5py.File) -> List[str]:
    series = file["Series"].keys()
    rust_sim_ents = list(filter(lambda x: "Rust_Sim-0." in x, series))
    rust_sim_ents.remove("Rust_Sim-0.Neighborhood0")
    return list(map(lambda x: "Series/%s" % (x,), rust_sim_ents))


def get_attributes_of_entities(file, entities: Iterable[str]) -> List[str]:
    attributes = set()
    for ent in entities:
        attributes = attributes.union(file[ent].keys())
    return list(attributes)


def get_length_of_data(file: h5py.File, entities: List[str]):
    first_ents = entities[0]
    attr = list(file[first_ents].keys())[0]
    return file[first_ents][attr].shape[0]


def get_meta_dict(file: h5py.File) -> Dict[str, Any]:
    meta = dict(file.attrs.items())
    return meta


def create_time_index(file: h5py.File) -> pd.DatetimeIndex:
    entities = get_rust_sim_entities(file)
    data_length = get_length_of_data(file, entities)
    return pd.date_range(get_meta_dict(file)["START"],
                         periods=data_length, freq="300S")


def create_df_for_entity(file: h5py.File, entity: str, attributes=None, index: pd.DatetimeIndex = None) -> pd.DataFrame:
    if not attributes:
        attributes = list(file[entity].keys())
    if not index:
        index = create_time_index(file)
    data = dict()
    actual_attributes = list(file[entity].keys())
    for attribute in attributes:
        d = None
        if attribute in actual_attributes:
            d = file[entity][attribute][()]
        else:
            nans = np.zeros(index.shape[0])
            nans[:] = np.nan
            d = nans
        data[attribute] = d
    return pd.DataFrame(data, index=index)


def get_marketplace_df(file: h5py.File) -> pd.DataFrame:
    path = 'Series/Rust_Sim-0.Neighborhood0'
    view = file[path]
    attributes = list((view.keys()))
    data = dict()
    index = create_time_index(file)
    for attr in attributes:
        data[attr] = view[attr][()]
    return pd.DataFrame(data, index=index)


def convert_hdf5_to_dfs(file: h5py.File) -> Dict[str, pd.DataFrame]:
    rust_sim_entities = get_rust_sim_entities(file)
    attributes = get_attributes_of_entities(file, rust_sim_entities)
    return dict([(entity, create_df_for_entity(file, entity, attributes=attributes))
                 for entity in rust_sim_entities])


def convert_hdf5_to_viewdict(file: h5py.File) -> Dict[str, h5py.Dataset]:
    rust_sim_entities = get_rust_sim_entities(file)
    return dict([(entity, file[entity])
                 for entity in rust_sim_entities])


if __name__ == "__main__":
    file = h5py.File(PurePath(
        "mosaik-simulations/visualization/data/integrated_rust/CSV_predictor.hdf5"))

    print(get_rust_sim_entities(file))
    entities = get_rust_sim_entities(file)
    attributes = get_attributes_of_entities(file, entities)

    time_index = pd.date_range(pd.Timestamp.now(), periods=5, freq="300S")
    print(get_length_of_data(file, entities))
    index = MultiIndex.from_product(
        [time_index, entities], names=("date", "entity"))
    df = (pd.DataFrame(index=index))

    print(create_df_for_entity(
        file, entities[0], attributes=attributes).info())
