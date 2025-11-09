import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import json

# Set page config
st.set_page_config(
    page_title="West Coast Park Analytics",
    page_icon="🌳",
    layout="wide"
)

# Load the data
@st.cache_data
def load_data():
    df = pd.read_csv('data/processed/master_dataset.csv')
    with open('data/west_coast_park.json', 'r') as f:
        park_geojson = json.load(f)
    return df, park_geojson

# Initialize the app
def main():
    st.title('West Coast Park Analytics Dashboard')
    st.write('Interactive analytics dashboard for West Coast Park')

    # Load data
    df, park_geojson = load_data()

    # Create two columns for layout
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader('Park Map')
        # Initialize folium map centered on West Coast Park
        m = folium.Map(
            location=[1.2937, 103.7686],
            zoom_start=15
        )

        # Add park boundary
        folium.GeoJson(
            park_geojson,
            name='West Coast Park'
        ).add_to(m)

        # Display the map
        st_folium(m, width=700)

    with col2:
        st.subheader('Park Statistics')
        # Add some basic statistics about the park
        st.metric("Total Area", "50 hectares")
        st.metric("Number of Facilities", "20+")

if __name__ == "__main__":
    main()