# Simulation

Zentrales Simulationsrepo

### TODOs
    - Mechanismus zum richtigen Mapping zwischen Householdsim und Rust/enerdag entitäten erstellen, gibt  es derzeit nur für prosumenten indem darauf vertraut wird, dass die EID der HHSIM entitäten mit prosumerXY endet und dann für rust auf die pidXY gematcht wird. Das ist wichtig falls die Disposable Energy vorhersage aus einer CSV datei ausgelesen wird.


## Schnellstart

  Per Docker, Skript oder Manuell
### Docker

  `docker build . `
  `docker run --rm --name simulation_container -v "$(pwd):/simulation" -v "/dev/shm:/dev/shm"  <IMAGE_ID> `
  bzw: 
    docker-compose up
### Skript

`./setup_environment.sh`
`./additional_datasets/setup_datasets.sh`

### Manuell



- Erstellen der Python Umgebung
    - Eine `virtualenv` erstellen und aktivieren:
        (`virtualenv --python=/bin/python3.8 venv`)
    - `pip install -r requirements.txt`
    - Pfade in *.pth Dateien eintragen
        - Linux: "pfad/zur/virtuellen/umgebung"/lib/python3.8/site-packages/household.pth
            ```
            /path/to/simulation/
            /path/to/simulation/batterysim/
            ```
        - Windows: `"C:\path\to\venv"\Lib\simulation.pth`
             ```
            'C:\\Users\\Florian Gehring\\Workspace\\Uni\\Masterarbeit\\simulation'
            'C:\\Users\\Florian Gehring\\Workspace\\Uni\\Masterarbeit\\simulation\\batterysim'```
- Erstellen der Datensätze:
    - "Normaler" Datensatz von [Open Power Systems](https://open-power-system-data.org/)
        - Herunterladen der `household_data.sqlite` von der [Data Platform - Household Data](https://data.open-power-system-data.org/household_data/2020-04-15)
        - Speichern in `cosimulation_city_energy\simulation_data`
        - Ausführen von `data_preparation\datapreparation.py`
            - Eventuell muss dort der der Pfad angepasst werden, unten in der Datei kann er verändert werden
    - Neuer Datensatz von HTW Berlin
        - Für den HTW Berlin Datensatz gibt es die dazugehörige Readme-Datei (`additional_datasets\htw_berlin\Readme_DE.txt`).
            - Die Angegebenen Dateien runterladen und `additional_datasets\htw_berlin\create_dataset_csv.py` ausführen.
- Ausführen zweier einfacher Simulationen:
        Zwei Beispiele für Simulationen befinden sich in `cosimulation_city_energy\example.py`. 
Dort wird eine  Simulation mit dem "normalen" Datensatz  und eine Simulation mit dem neuen HTW-Berlin-Datensatz durchgeführt.
    - Die Haushaltssimulation, die die Datenbankanbindung durchführt (`householdsim\mosaik.py`) muss beim HTW-Berlin Datensatz speziell konfiguriert werden. Das wird im Beispiel mittels des `HOUSEHOLD_SIM_KWARGS` gezeigt. Für mehr Informationen die Dokumentation in `additional_datasets\htw_berlin\create_dataset_csv.py` lesen.
       

## Inhalt

<b>cosimulation_city_energy:</b> 
- city_energy_simulation.py: Interface für die Simulationen
- pandapower_mosaik.py: Schnittstelle zu Pandapower, zugeschnitten auf ein Stadtnetz
- simulation data: [Verbrauchsdaten](https://data.open-power-system-data.org/household_data/)
- Panda_Interface.py: Ist die Simulation von Pandapower + Householdsim
- Rust_Interface.py: Ist die Simulation von Householdsim (Rust Version)
- network_grid.py: Enthält die Funtion get_grid() welche für das Network zuständig ist.
- Connect_Grid.py: Enthält die Funktionen die die Modelle von Householdsim an das Grid verbinden.
- rust_integrated_battery.py: Enthält Simulationen mit Verbindung zur enerDAG-Marktplatzsimulation und in CSV-Dateien gespeichtert Vorhersagemodelle.

<b>householdsimulation:</b> 
- eigene householdsimulation. Ist Auf das Format von [Verbrauchsdaten](https://data.open-power-system-data.org/household_data/) angepasst.
Andere Datenformate können eingestellt werden. Sie abschnitt zu "additional datasets". 

<b>data_preparation:</b> 
- Nimmt Daten mit dem Format wie die [Verbrauchsdaten](https://data.open-power-system-data.org/household_data/) 
und versucht den Datensatz durch sinnvolles Füllen lückenfrei zu machen. Dadurch entsteht household_data_prepared.sqlite, 
welches für city_energy_simulation.py gebraucht wird 

<b>mosaik_web:</b> 
- modifizierte Version von mosaik_web, das aus der mosaik_demo bekannt ist

<b> additional_datasets:</b>
- TODO: Einheiten kWH / W beschreiben, ob datensätze kumulativ sind etc.
Stellt weitere Datensätze, neben dem [Verbrauchsdatensatz](https://data.open-power-system-data.org/household_data/) zur Verfügung. 
- create_pv_profiles: Erstellt Daten zur Produktion von Solarstrom mittels des [PVGIS-Tools](https://ec.europa.eu/jrc/en/PVGIS/docs/noninteractive).
- htw_berlin: Reine Verbrauchsdaten von 74 Berliner Haushalten. Mit create_pv_profiles können dazugehörige Solarproduktionsdaten erzeugt werden.

<b> battery_sim: </b>
- Modelliert das Verhalten von Prosumenten mit Batterien in einer enerDAG Nachbarschaft. 



# Simulieren mit zusätzlichen Datensätzen und Rust-Anbindung
* Ausführen der Dateien zum erstellen der Datensätze (htw_berlin/create_csv_dataset_csv.py oder preprocessed_householdsim/ground_truth_creator.py )
* Falls Energieverbrauchsvorhersagen in enerDAG/Rust über vorgefertige CSV Dateien stattfinden soll, dann führe additional_dataset/create_reference_predictions aus. 
    -> Dort kann die "perfekte", backshift und UEMA Vorhersage erstellt werden
* In cosimulation_city_energy/rust_integrated_battery.py befinden sich simulationen die die anbindung realisieren.


## Wie startet man das Programm?

### Vorraussetzungen:
- Python 3.8 oder höher
- pip
- virtualenv

### Benötigte Packages
- werden in `requirements.txt` aufgelistet und können mittels `pip install -r requirements.txt` installiert werden.
- data_preparation ausführen um die datenbank aufzufüllen/vorzubereiten.

In city_energy_simulation.py "NET" mit dem Network (z.B.: "MieterStromNetzMitPV") und "SIM" mit der Simulation (z.B.: "Panda_Simulation") die man verwenden möchte anpassen.

Abschließend die Simulation über `city_energy_simulation.py` starten. Die Ausgabe kann über `http://localhost:8000/` angezeigt werden
Achtung die Simulation funktioniert nur für Zeiträume, die im Datensatz abgedeckt sind.

Alle Network (NET) Möglichkeiten:
- "VorStadtNetz"
- "VorStadtNetzMit10PV"
- "VorStadtNetzMitProsumer"
- "VorStadtNetzMitProsumerundPV"
- "LandNetzMitPV"
- "MieterStromNetz"
- "DorfNetz"
- "DorfNetzMit2PV"
- "DorfNetzMit5PV"
- "DorfNetzMit7PV"
- "DorfNetzMit10PV"
- "DorfNetzMit5Prosumer"
- "DorfNetzMitPVundProsumer"
- "DemoNetz"

Alle Simulation (SIM) Möglichkeiten:
- "Panda_Simulation"
- "Rust_Simulation"



## Wie startet man das Programm unter Lucille?

Aus dem Uninetz: `ssh user@lucille.informatik.uni-tuebingen.de`

Einmalig: 
alias conda=/opt/anaconda3/bin/conda

conda activate simulation

### Zum erstellen der gleichen Simulation
conda create --name simulation --file lucille-package-list.txt

### Zum Updaten der Liste nach Änderungen in der Liste
conda env update --name simulation --file lucille-package-list.txt

### Zum Updaten der Liste
conda list --export > lucille-package-list.txt

### Was in der Liste angepasst wurde und wie sie erstellt wurde:
conda create -n simulation python
conda install panda
conda install scipy
conda install numba
pip install -r requirements.txt
conda list --export > lucille-package-list.txt


### Für Fehlermeldung: Module Householdsim (o.ä.) can not be found

'export PYTHONPATH="$PWD"' im Simulationsordner bzw. 'export PYTHONPATH="path/to/simulation/' 
Eventuell anzupassen für andere Shells.

Eine andere Möglichkeit ist das Anlgegen einer *.pth-Datei für die virtuelle Umgebung. 
"pfad/zur/virtuellen/umgebung"/lib/python3.8/site-packages/household.pth
Mit Inhalt  
```
"path/to/simulation/" 
"path/to/simulation/"batterysim/
```


### Zum Anschauen auf dem lokalen Rechner:

ssh -L 60001:localhost:60001 gross@lucille




