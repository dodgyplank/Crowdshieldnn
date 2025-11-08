

# Crowd Dynamics Forecasting & Safe Zone Simulation (West Coast Park)

## Overview

This project models and visualizes **crowd dynamics in public parks**, focusing on **spatial and temporal population distribution** during various scenarios (events, holidays, emergencies).  
The system predicts, simulates, and visualizes how people move within **West Coast Park**, integrates multiple data sources (taxi stands, bus stops, park sensors), and helps plan **safe zones** for effective crowd risk management.

---

## Objectives

1. **Forecast crowd density** at specific locations and times using historical and real-time data.  
2. **Simulate stimuli** (e.g., events, alerts, weather changes) to visualize crowd responses.  
3. **Generate population density maps** and contours over time.  
4. **Design safe zones** and compute risk levels based on predicted density and travel time.  
5. *(Optional)* Optimize placement of safe zones to minimize high-risk areas.  

---

## Project Structure
```
crowd_dynamics_minimal/
│
├── data/
│   ├── west_coast_park.geojson    # Park boundary / features
│   └── footfall_timeseries.csv    # Baseline / historical population counts (can be synthetic)
│
├── models/
│   └── crowd_forecast_model.py    # Core model: STGNN or LSTM predicting density
│
├── simulation/
│   └── stimulus_simulator.py      # Inject events and forecast density propagation
│
├── app/
│   └── main_app.py                # Minimal dashboard / map visualization + safe zones
│
├── utils.py                        # Optional: helper functions (geospatial, travel time)
├── requirements.txt
└── README.md
```
| Deliverable                     | Minimal File                       |
| ------------------------------- | ---------------------------------- |
| Predict crowd density           | `models/crowd_forecast_model.py`   |
| Simulate stimulus               | `simulation/stimulus_simulator.py` |
| Display density map / contours  | `app/main_app.py`                  |
| Safe zones + risk (optional)    | `app/main_app.py` + `utils.py`     |
| Baseline footfall / assumptions | `data/footfall_timeseries.csv`     |
| Park boundaries                 | `data/west_coast_park.geojson`     |
| Documentation                   | `README.md`                        |
