import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
from snowflake.snowpark.context import get_active_session

# Set page configuration
st.set_page_config(
    page_title="Healthcare Analytics Dashboard",
    page_icon="🏥",
    layout="wide"
)

# Use custom CSS to style the application
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        font-weight: 400;
        color: #4B5563;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1E3A8A;
    }
    .metric-label {
        font-size: 1.1rem;
        color: #4B5563;
    }
    .score-high {
        color: #047857;
    }
    .score-medium {
        color: #B45309;
    }
    .score-low {
        color: #DC2626;
    }
    hr {
        margin-top: 2rem;
        margin-bottom: 2rem;
    }
    .stRadio > div {
        flex-direction: row;
        gap: 10px;
    }
    .stRadio label {
        background-color: #F3F4F6;
        padding: 10px 15px;
        border-radius: 5px;
        margin-right: 10px;
        cursor: pointer;
    }
</style>
""", unsafe_allow_html=True)

# Establish connection to Snowflake using Snowpark
@st.cache_resource
def get_snowpark_session():
    return get_active_session()

# Get the session
session = get_snowpark_session()

# Function to execute Snowflake queries with proper error handling
def run_query(query):
    try:
        if query.strip().upper().startswith("SELECT"):
            return session.sql(query).to_pandas()
        else:
            result = session.sql(query).collect()
            if result:
                data = [row.asDict() for row in result]
                return pd.DataFrame(data)
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Query execution error: {str(e)}")
        return pd.DataFrame()

# Tables
master_table = "HEALTH_NAV.CORE.MASTER_TABLE"
hospital_table = "HEALTH_NAV.CORE.HOSPITAL_DATA"
provider_table = "HEALTH_NAV.CORE.INSURANCE_PROVIDERS"
plan_table = "HEALTH_NAV.CORE.INSURANCE_PLANS"
service_code_table = "HEALTH_NAV.CORE.SERVICE_CODES"

# Cache function to get all states from the data
@st.cache_data(ttl=3600)
def get_states():
    query = f"""
    SELECT DISTINCT STATE
    FROM {hospital_table}
    WHERE STATE IS NOT NULL
    ORDER BY STATE
    """
    return run_query(query)

# Cache function to get cities by state
@st.cache_data(ttl=3600)
def get_cities_by_state(state=None):
    if state and state != "All States":
        query = f"""
        SELECT DISTINCT CITY
        FROM {hospital_table}
        WHERE STATE = '{state}'
        AND CITY IS NOT NULL
        ORDER BY CITY
        """
    else:
        query = f"""
        SELECT DISTINCT CITY
        FROM {hospital_table}
        WHERE CITY IS NOT NULL
        ORDER BY CITY
        """
    return run_query(query)

# Cache function to get available CPT codes
@st.cache_data(ttl=3600)
def get_cpt_codes():
    query = f"""
    SELECT DISTINCT CODE, DESCRIPTION
    FROM {service_code_table}
    ORDER BY CODE
    """
    return run_query(query)

# Sidebar navigation
st.sidebar.title("Navigation")
app_mode = st.sidebar.radio("Select Dashboard", 
    options=["Hospital Price Variation", "Insurance Provider Comparison", "Healthcare Cost Explorer", "Health Cost Navigator"])

if app_mode == "Hospital Price Variation":
    # Display header for this section
    st.markdown('<div class="main-header">Hospital Price Variation Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Identifying hospitals with consistent pricing across treatments</div>', unsafe_allow_html=True)
    
    # Sidebar for filters
    st.sidebar.header("Filters")

    # State selection
    states_df = get_states()
    state_options = ["All States"] + states_df["STATE"].tolist() if not states_df.empty else ["All States"]
    selected_state = st.sidebar.selectbox(
        "Select State:",
        options=state_options,
        key="price_variation_state"
    )

    # Get cities for selected state
    cities_df = get_cities_by_state(selected_state if selected_state != "All States" else None)
    city_options = ["All Cities"] + cities_df["CITY"].tolist() if not cities_df.empty else ["All Cities"]

    # City selection
    selected_city = st.sidebar.selectbox(
        "Select City:",
        options=city_options,
        key="price_variation_city"
    )

    # Minimum number of procedures filter
    min_procedures = st.sidebar.slider(
        "Minimum Number of Procedures per Hospital:",
        min_value=5,
        max_value=50,
        value=10,
        step=5,
        help="Only include hospitals that offer at least this many procedures"
    )

    # Metric selection for variation
    variation_metric = st.sidebar.selectbox(
        "Variation Metric:",
        options=["Coefficient of Variation", "Standard Deviation", "Price Range"]
    )

    # Apply filters button
    apply_filters = st.sidebar.button("Apply Filters", use_container_width=True, key="price_variation_apply")

    # Main area for displaying results
    if apply_filters:
        # Build state and city filter conditions
        state_condition = ""
        if selected_state and selected_state != "All States":
            state_condition = f"AND h.STATE = '{selected_state}'"
        else:
            state_condition = "AND h.STATE IN ('NV', 'IL', 'NC')"
        
        city_condition = ""
        if selected_city and selected_city != "All Cities":
            city_condition = f"AND h.CITY = '{selected_city}'"
        
        # Query to calculate price variation by hospital
        location_display = f"in {selected_state}" if selected_state != "All States" else "in Nevada, Illinois, and North Carolina"
        st.info(f"Analyzing price variation for hospitals {location_display}...")
        
        # Different SQL calculation based on the selected variation metric
        if variation_metric == "Coefficient of Variation":
            variation_sql = "STDDEV(m.STANDARD_CHARGE_DOLLAR) / NULLIF(AVG(m.STANDARD_CHARGE_DOLLAR), 0) * 100 as PRICE_VARIATION"
            sort_order = "ASC"  # Lower CV is better
            explanation = "Coefficient of Variation (CV) measures the ratio of the standard deviation to the mean, expressed as a percentage. Lower values indicate more consistent pricing."
        elif variation_metric == "Standard Deviation":
            variation_sql = "STDDEV(m.STANDARD_CHARGE_DOLLAR) as PRICE_VARIATION"
            sort_order = "ASC"  # Lower SD is better
            explanation = "Standard Deviation measures the amount of dispersion in pricing. Lower values indicate less spread in prices."
        else:  # Price Range
            variation_sql = "MAX(m.STANDARD_CHARGE_DOLLAR) - MIN(m.STANDARD_CHARGE_DOLLAR) as PRICE_VARIATION"
            sort_order = "ASC"  # Lower range is better
            explanation = "Price Range is the difference between the highest and lowest prices. Lower values indicate more consistent pricing."
        
        # Query to get hospitals and their price variation
        hospital_variation_query = f"""
        WITH hospital_procedures AS (
            SELECT 
                h.HOSPITAL_ID,
                h.HOSPITAL_NAME,
                h.CITY,
                h.STATE,
                COUNT(DISTINCT m.CODE) as PROCEDURE_COUNT
            FROM 
                {hospital_table} h
            JOIN 
                {master_table} m ON h.HOSPITAL_ID = m.HOSPITAL_ID
            WHERE 
                m.STANDARD_CHARGE_DOLLAR > 0
                {state_condition}
                {city_condition}
            GROUP BY 
                h.HOSPITAL_ID, h.HOSPITAL_NAME, h.CITY, h.STATE
            HAVING 
                PROCEDURE_COUNT >= {min_procedures}
        )
        
        SELECT 
            hp.HOSPITAL_ID,
            hp.HOSPITAL_NAME,
            hp.CITY,
            hp.STATE,
            hp.PROCEDURE_COUNT,
            {variation_sql},
            AVG(m.STANDARD_CHARGE_DOLLAR) as AVG_PRICE,
            MIN(m.STANDARD_CHARGE_DOLLAR) as MIN_PRICE,
            MAX(m.STANDARD_CHARGE_DOLLAR) as MAX_PRICE,
            COUNT(DISTINCT m.CODE) as UNIQUE_CODES
        FROM 
            hospital_procedures hp
        JOIN 
            {master_table} m ON hp.HOSPITAL_ID = m.HOSPITAL_ID
        WHERE 
            m.STANDARD_CHARGE_DOLLAR > 0
        GROUP BY 
            hp.HOSPITAL_ID, hp.HOSPITAL_NAME, hp.CITY, hp.STATE, hp.PROCEDURE_COUNT
        ORDER BY 
            PRICE_VARIATION {sort_order}
        """
        
        # Execute query
        hospital_variation_df = run_query(hospital_variation_query)
        
        # Check if data was returned
        if hospital_variation_df.empty:
            st.warning(f"No hospitals found with at least {min_procedures} procedures in the selected area.")
        else:
            # Calculate score for each hospital (lower variation = higher score)
            # Convert to percentile ranking (higher is better)
            hospital_variation_df['PERCENTILE_RANK'] = 100 - (hospital_variation_df['PRICE_VARIATION'].rank(pct=True) * 100)
            
            # Add a scoring column
            def get_score_category(percentile):
                if percentile >= 80:
                    return "High Consistency"
                elif percentile >= 50:
                    return "Medium Consistency"
                else:
                    return "Low Consistency"
            
            hospital_variation_df['SCORE_CATEGORY'] = hospital_variation_df['PERCENTILE_RANK'].apply(get_score_category)
            
            # Calculate summary metrics
            total_hospitals = len(hospital_variation_df)
            high_consistency = sum(hospital_variation_df['SCORE_CATEGORY'] == "High Consistency")
            med_consistency = sum(hospital_variation_df['SCORE_CATEGORY'] == "Medium Consistency")
            low_consistency = sum(hospital_variation_df['SCORE_CATEGORY'] == "Low Consistency")
            
            # Display explanation of the metric
            st.info(explanation)
            
            # Show summary metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Total Hospitals Analyzed</div>
                    <div class="metric-value">{total_hospitals}</div>
                    <div>With {min_procedures}+ procedures</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Hospital Price Consistency</div>
                    <div class="metric-value score-high">{high_consistency} High</div>
                    <div class="score-medium">{med_consistency} Medium | <span class="score-low">{low_consistency} Low</span></div>
                </div>
                """, unsafe_allow_html=True)
                
            with col3:
                # Get the hospital with the most consistent pricing
                best_hospital = hospital_variation_df.iloc[0]
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Most Consistent Pricing</div>
                    <div class="metric-value">{best_hospital['HOSPITAL_NAME']}</div>
                    <div>{best_hospital['CITY']}, {best_hospital['STATE']}</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<hr>", unsafe_allow_html=True)
            
            # Tabs for different visualizations
            tab1, tab2, tab3 = st.tabs(["Hospital Rankings", "Detailed Comparison", "Raw Data"])
            
            with tab1:
                # Create a bar chart of hospitals by price variation
                # Sort by variation for the chart
                chart_df = hospital_variation_df.sort_values('PRICE_VARIATION')
                
                # Limit to top 30 for readability
                if len(chart_df) > 30:
                    chart_df = chart_df.head(30)
                    st.info(f"Showing the 30 hospitals with the most consistent pricing out of {total_hospitals} total.")
                
                # Create color scale based on score category
                color_scale = alt.Scale(
                    domain=['High Consistency', 'Medium Consistency', 'Low Consistency'],
                    range=['#047857', '#B45309', '#DC2626']
                )
                
                # Format variation value based on metric type
                if variation_metric == "Coefficient of Variation":
                    tooltip_format = '.2f'
                    axis_title = 'Coefficient of Variation (%)'
                elif variation_metric == "Standard Deviation":
                    tooltip_format = '$,.2f'
                    axis_title = 'Standard Deviation ($)'
                else:  # Price Range
                    tooltip_format = '$,.2f'
                    axis_title = 'Price Range ($)'
                
                bar_chart = alt.Chart(chart_df).mark_bar().encode(
                    x=alt.X('PRICE_VARIATION:Q', title=axis_title),
                    y=alt.Y('HOSPITAL_NAME:N', title='Hospital', sort=None),
                    color=alt.Color('SCORE_CATEGORY:N', scale=color_scale, title='Price Consistency'),
                    tooltip=[
                        alt.Tooltip('HOSPITAL_NAME:N', title='Hospital'),
                        alt.Tooltip('CITY:N', title='City'),
                        alt.Tooltip('PRICE_VARIATION:Q', title=variation_metric, format=tooltip_format),
                        alt.Tooltip('PROCEDURE_COUNT:Q', title='Procedure Count'),
                        alt.Tooltip('AVG_PRICE:Q', title='Average Price', format='$,.2f'),
                        alt.Tooltip('PERCENTILE_RANK:Q', title='Consistency Score', format='.1f')
                    ]
                ).properties(
                    width=700,
                    height=600,
                    title=f'Hospitals Ranked by Price Consistency ({variation_metric})'
                )
                
                st.altair_chart(bar_chart, use_container_width=True)
                
            with tab2:
                # Create a more detailed comparison
                st.subheader("Hospital Price Statistics")
                
                # Calculate additional statistics
                comparison_df = hospital_variation_df.copy()
                
                # Format for display
                display_df = comparison_df[['HOSPITAL_NAME', 'CITY', 'PROCEDURE_COUNT', 'PRICE_VARIATION', 
                                            'AVG_PRICE', 'MIN_PRICE', 'MAX_PRICE', 'PERCENTILE_RANK', 'SCORE_CATEGORY']]
                
                # Add a search box for filtering the hospitals
                hospital_search = st.text_input("Search for a specific hospital:")
                
                # Filter the data based on search
                if hospital_search:
                    filtered_df = display_df[
                        display_df['HOSPITAL_NAME'].str.contains(hospital_search, case=False)
                    ]
                else:
                    filtered_df = display_df
                
                # Create a scatter plot comparing procedure count vs price variation
                scatter_chart = alt.Chart(comparison_df).mark_circle(size=60).encode(
                    x=alt.X('PROCEDURE_COUNT:Q', title='Number of Procedures'),
                    y=alt.Y('PRICE_VARIATION:Q', title=variation_metric),
                    color=alt.Color('SCORE_CATEGORY:N', scale=color_scale, title='Price Consistency'),
                    size=alt.Size('AVG_PRICE:Q', title='Average Price', scale=alt.Scale(range=[50, 300])),
                    tooltip=[
                        alt.Tooltip('HOSPITAL_NAME:N', title='Hospital'),
                        alt.Tooltip('CITY:N', title='City'),
                        alt.Tooltip('PROCEDURE_COUNT:Q', title='Procedure Count'),
                        alt.Tooltip('PRICE_VARIATION:Q', title=variation_metric, format=tooltip_format),
                        alt.Tooltip('AVG_PRICE:Q', title='Average Price', format='$,.2f')
                    ]
                ).properties(
                    width=700,
                    height=400,
                    title=f'Hospital Price Variation vs. Procedure Count'
                )
                
                st.altair_chart(scatter_chart, use_container_width=True)
                
                # Display the filtered comparison data
                st.dataframe(
                    filtered_df.rename(
                        columns={
                            'HOSPITAL_NAME': 'Hospital',
                            'CITY': 'City',
                            'PROCEDURE_COUNT': 'Procedure Count',
                            'PRICE_VARIATION': variation_metric,
                            'AVG_PRICE': 'Average Price',
                            'MIN_PRICE': 'Minimum Price',
                            'MAX_PRICE': 'Maximum Price',
                            'PERCENTILE_RANK': 'Consistency Score',
                            'SCORE_CATEGORY': 'Rating'
                        }
                    ).style.format({
                        variation_metric: '${:,.2f}' if variation_metric != "Coefficient of Variation" else '{:,.2f}%',
                        'Average Price': '${:,.2f}',
                        'Minimum Price': '${:,.2f}',
                        'Maximum Price': '${:,.2f}',
                        'Consistency Score': '{:,.1f}'
                    }),
                    use_container_width=True
                )
                
            with tab3:
                # Show the raw data
                st.subheader("Raw Hospital Data")
                
                # Display the raw data
                st.dataframe(
                    hospital_variation_df.style.format({
                        'PRICE_VARIATION': '${:,.2f}' if variation_metric != "Coefficient of Variation" else '{:,.2f}%',
                        'AVG_PRICE': '${:,.2f}',
                        'MIN_PRICE': '${:,.2f}',
                        'MAX_PRICE': '${:,.2f}',
                        'PERCENTILE_RANK': '{:,.1f}'
                    }),
                    use_container_width=True
                )
            
            # Additional analysis - Procedure-level price consistency
            st.markdown("<hr>", unsafe_allow_html=True)
            st.subheader("Deep Dive: Procedure-level Price Consistency")
            
            # Let user select a hospital to see procedure-level details
            selected_hospital = st.selectbox(
                "Select a hospital to view procedure-level price consistency:",
                options=hospital_variation_df['HOSPITAL_NAME'].tolist()
            )
            
            if selected_hospital:
                selected_hospital_id = hospital_variation_df[hospital_variation_df['HOSPITAL_NAME'] == selected_hospital]['HOSPITAL_ID'].iloc[0]
                
                # Query to get procedure-level prices for the selected hospital
                procedure_query = f"""
                SELECT 
                    s.CODE,
                    s.DESCRIPTION,
                    m.STANDARD_CHARGE_DOLLAR as PRICE,
                    AVG(m2.STANDARD_CHARGE_DOLLAR) OVER (PARTITION BY s.CODE) as AVG_PRICE_ACROSS_HOSPITALS,
                    (m.STANDARD_CHARGE_DOLLAR - AVG(m2.STANDARD_CHARGE_DOLLAR) OVER (PARTITION BY s.CODE)) / 
                        NULLIF(AVG(m2.STANDARD_CHARGE_DOLLAR) OVER (PARTITION BY s.CODE), 0) * 100 as PERCENT_DIFF_FROM_AVG
                FROM 
                    {master_table} m
                JOIN 
                    {service_code_table} s ON m.CODE = s.CODE
                JOIN 
                    {master_table} m2 ON s.CODE = m2.CODE AND m2.STANDARD_CHARGE_DOLLAR > 0
                WHERE 
                    m.HOSPITAL_ID = '{selected_hospital_id}'
                    AND m.STANDARD_CHARGE_DOLLAR > 0
                ORDER BY 
                    ABS(PERCENT_DIFF_FROM_AVG) DESC
                """
                
                procedure_df = run_query(procedure_query)
                
                if not procedure_df.empty:
                    # Add columns to categorize price differences
                    def get_price_category(pct_diff):
                        if pct_diff < -20:
                            return "Significantly Lower"
                        elif pct_diff < -5:
                            return "Moderately Lower"
                        elif pct_diff <= 5:
                            return "Comparable"
                        elif pct_diff <= 20:
                            return "Moderately Higher"
                        else:
                            return "Significantly Higher"
                    
                    procedure_df['PRICE_CATEGORY'] = procedure_df['PERCENT_DIFF_FROM_AVG'].apply(get_price_category)
                    
                    # Summary of procedure pricing
                    category_counts = procedure_df['PRICE_CATEGORY'].value_counts().reset_index()
                    category_counts.columns = ['Category', 'Count']
                    
                    # Create a horizontal bar chart for price categories
                    category_order = ["Significantly Lower", "Moderately Lower", "Comparable", 
                                      "Moderately Higher", "Significantly Higher"]
                    
                    color_scale = alt.Scale(
                        domain=category_order,
                        range=['#059669', '#10B981', '#6B7280', '#F59E0B', '#DC2626']
                    )
                    
                    category_chart = alt.Chart(category_counts).mark_bar().encode(
                        y=alt.Y('Category:N', sort=category_order, title=None),
                        x=alt.X('Count:Q', title='Number of Procedures'),
                        color=alt.Color('Category:N', scale=color_scale, title=None),
                        tooltip=['Category:N', 'Count:Q']
                    ).properties(
                        width=600,
                        height=300,
                        title=f'Procedure Price Categories for {selected_hospital}'
                    )
                    
                    st.altair_chart(category_chart, use_container_width=True)
                    
                    # Display the procedure table
                    st.subheader("Procedure Price Details")
                    st.dataframe(
                        procedure_df.rename(
                            columns={
                                'CODE': 'CPT Code',
                                'DESCRIPTION': 'Procedure Description',
                                'PRICE': 'Hospital Price',
                                'AVG_PRICE_ACROSS_HOSPITALS': 'Avg Market Price',
                                'PERCENT_DIFF_FROM_AVG': '% Difference',
                                'PRICE_CATEGORY': 'Price Category'
                            }
                        ).style.format({
                            'Hospital Price': '${:,.2f}',
                            'Avg Market Price': '${:,.2f}',
                            '% Difference': '{:+,.1f}%'
                        }),
                        use_container_width=True
                    )
                else:
                    st.warning(f"No procedure data found for {selected_hospital}.")
    else:
        # Initial state before filters are applied
        st.markdown("""
        ## Welcome to the Hospital Price Variation Analysis

        This dashboard helps healthcare consumers and administrators identify hospitals with consistent pricing across medical procedures. 

        ### Key Features
        - Compare hospitals by price consistency across procedures
        - Analyze different price variation metrics
        - Deep dive into procedure-level pricing for specific hospitals
        - Compare hospital prices against market averages
        
        To begin, select a state and other filters in the sidebar, then click "Apply Filters".
        """)

elif app_mode == "Insurance Provider Comparison":
    # Custom title using markdown
    st.markdown("# Average Standard Charge by Insurance Provider")

    # Create a container for filters to keep layout clean
    filter_container = st.container()

    # Retrieve state data
    states_df = get_states()

    # Create filters in the designated container
    with filter_container:
        col1, col2 = st.columns(2)
        
        with col1:
            if not states_df.empty:
                selected_state = st.selectbox(
                    "Filter by State:",
                    options=["All States"] + states_df["STATE"].tolist(),
                    key="provider_comp_state"
                )
            else:
                selected_state = "All States"
        
        with col2:
            if selected_state != "All States":
                cities_df = get_cities_by_state(selected_state)
            else:
                cities_df = get_cities_by_state()
            
            if not cities_df.empty:
                selected_city = st.selectbox(
                    "Filter by City:",
                    options=["All Cities"] + cities_df["CITY"].tolist(),
                    key="provider_comp_city"
                )
            else:
                selected_city = "All Cities"

    # Add spacing
    st.markdown("---")

    # Build query conditions
    state_condition = ""
    if selected_state != "All States":
        state_condition = f"AND h.STATE = '{selected_state}'"

    city_condition = ""
    if selected_city != "All Cities":
        city_condition = f"AND h.CITY = '{selected_city}'"

    # Query for average prices
    query = f"""
    SELECT 
        p.PAYER_NAME,
        AVG(m.STANDARD_CHARGE_DOLLAR) as AVG_CHARGE
    FROM 
        {master_table} m
    JOIN 
        {hospital_table} h ON m.HOSPITAL_ID = h.HOSPITAL_ID
    JOIN 
        {provider_table} p ON m.INSURANCE_PROVIDER_ID = p.INSURANCE_PROVIDER_ID
    JOIN 
        {service_code_table} s ON m.CODE = s.CODE
    WHERE 
        m.STANDARD_CHARGE_DOLLAR > 0
        {state_condition}
        {city_condition}
    GROUP BY 
        p.PAYER_NAME
    ORDER BY 
        AVG_CHARGE DESC
    """

    # Execute the query
    results = run_query(query)

    # Check for results and render chart
    if results.empty:
        st.warning("No data available for the selected filters.")
    else:
        # Display location context
        location_text = ""
        if selected_city != "All Cities" and selected_state != "All States":
            location_text = f"in {selected_city}, {selected_state}"
        elif selected_state != "All States":
            location_text = f"in {selected_state}"
        elif selected_city != "All Cities":
            location_text = f"in {selected_city}"
        
        if location_text:
            st.markdown(f"**Showing data {location_text}**")
        
        # Create bar chart
        chart = alt.Chart(results).mark_bar().encode(
            x=alt.X('PAYER_NAME:N', title='Insurance Provider', axis=alt.Axis(labelAngle=-45)),
            y=alt.Y('AVG_CHARGE:Q', title='Average Standard Charge ($)'),
            color=alt.Color('AVG_CHARGE:Q', scale=alt.Scale(scheme='blues'), legend=None),
            tooltip=[
                alt.Tooltip('PAYER_NAME:N', title='Insurance Provider'),
                alt.Tooltip('AVG_CHARGE:Q', title='Average Standard Charge', format='$,.2f')
            ]
        ).properties(
            height=500
        )
        
        # Add price labels on bars
        text = chart.mark_text(
            align='center',
            baseline='bottom',
            dy=-5
        ).encode(
            text=alt.Text('AVG_CHARGE:Q', format='$,.0f')
        )
        
        # Combine chart and labels
        final_chart = chart + text
        
        # Display the chart
        st.altair_chart(final_chart, use_container_width=True)

elif app_mode == "Healthcare Cost Explorer":
    # Custom title without anchor link
    st.markdown('<div class="main-header">Healthcare Cost Explorer</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Analyze medical procedure costs across cities</div>', unsafe_allow_html=True)

    # Load CPT codes and states
    cpt_codes_df = get_cpt_codes()
    states_df = get_states()

    # Sidebar for filters
    st.sidebar.header("Filters")

    # CPT code selection (with description)
    if not cpt_codes_df.empty:
        # Format CPT codes with descriptions for the dropdown
        cpt_options = [f"{row['CODE']} - {row['DESCRIPTION'][:50]}..." if len(row['DESCRIPTION']) > 50 
                      else f"{row['CODE']} - {row['DESCRIPTION']}" 
                      for _, row in cpt_codes_df.iterrows()]
        
        selected_cpt_option = st.sidebar.selectbox(
            "Select Medical Procedure (CPT Code):",
            options=cpt_options,
            key="cost_explorer_cpt"
        )
        
        # Extract just the code from the selection
        selected_cpt = selected_cpt_option.split(" - ")[0].strip() if selected_cpt_option else None
    else:
        selected_cpt = st.sidebar.text_input("Enter CPT Code:", key="cost_explorer_cpt_input")
        st.sidebar.warning("No CPT codes loaded. Please enter a code manually.")
    
    # State selection for filtering cities
    if not states_df.empty:
        selected_state = st.sidebar.selectbox(
            "Select State (Optional):",
            options=["All States"] + states_df["STATE"].tolist(),
            key="cost_explorer_state"
        )
    else:
        selected_state = st.sidebar.text_input("Enter State Code (Optional):", key="cost_explorer_state_input")
    
    # Metric selection
    metric_options = ["Average", "Median", "Minimum", "Maximum"]
    selected_metric = st.sidebar.selectbox(
        "Select Price Metric:", 
        options=metric_options,
        key="cost_explorer_metric"
    )
    
    # Set standard charge as default
    charge_column = "STANDARD_CHARGE_DOLLAR"
    
    # Apply filters button
    apply_filters = st.sidebar.button("Apply Filters", use_container_width=True, key="cost_explorer_apply")
    
    # Main area for displaying results
    if apply_filters and selected_cpt:
        # Get the name of the CPT code for display
        cpt_description_query = f"""
        SELECT DESCRIPTION 
        FROM {service_code_table} 
        WHERE CODE = '{selected_cpt}'
        LIMIT 1
        """
        cpt_desc_df = run_query(cpt_description_query)
        cpt_description = cpt_desc_df.iloc[0]['DESCRIPTION'] if not cpt_desc_df.empty else "Unknown Procedure"
        
        # Display procedure information
        st.subheader(f"Analysis for: {cpt_description} (CPT {selected_cpt})")
        
        # Build state filter condition
        state_condition = ""
        if selected_state and selected_state != "All States":
            state_condition = f"AND h.STATE = '{selected_state}'"
        
        # Query to get city-level metrics
        metric_sql = ""
        if selected_metric == "Average":
            metric_sql = f"AVG({charge_column})"
        elif selected_metric == "Median":
            metric_sql = f"MEDIAN({charge_column})"
        elif selected_metric == "Minimum":
            metric_sql = f"MIN({charge_column})"
        elif selected_metric == "Maximum":
            metric_sql = f"MAX({charge_column})"
        
        city_metrics_query = f"""
        SELECT 
            h.CITY,
            h.STATE,
            {metric_sql} as PRICE_METRIC,
            COUNT(*) as NUM_PROVIDERS
        FROM 
            {master_table} m
        JOIN 
            {hospital_table} h ON m.HOSPITAL_ID = h.HOSPITAL_ID
        JOIN 
            {service_code_table} s ON m.CODE = s.CODE
        WHERE 
            s.CODE = '{selected_cpt}'
            AND h.CITY IS NOT NULL
            AND h.STATE IS NOT NULL
            AND {charge_column} > 0
            {state_condition}
        GROUP BY 
            h.CITY, h.STATE
        ORDER BY 
            PRICE_METRIC DESC
        """
        
        # Execute query
        st.info(f"Calculating {selected_metric} Standard Charge by city...")
        city_metrics_df = run_query(city_metrics_query)
        
        # Check if data was returned
        if city_metrics_df.empty:
            st.warning(f"No data found for CPT code {selected_cpt} in the selected area.")
        else:
            # Format city name with state for display
            city_metrics_df['CITY_STATE'] = city_metrics_df.apply(lambda x: f"{x['CITY']}, {x['STATE']}", axis=1)
            
            # Show summary metrics
            col1, col2, col3 = st.columns(3)
            
            # Calculate overall statistics
            overall_avg = city_metrics_df['PRICE_METRIC'].mean()
            most_expensive_city = city_metrics_df.iloc[0]['CITY_STATE']
            most_expensive_price = city_metrics_df.iloc[0]['PRICE_METRIC']
            least_expensive_city = city_metrics_df.iloc[-1]['CITY_STATE']
            least_expensive_price = city_metrics_df.iloc[-1]['PRICE_METRIC']
            num_cities = len(city_metrics_df)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">National {selected_metric}</div>
                    <div class="metric-value">${overall_avg:,.2f}</div>
                    <div>Based on {num_cities} cities</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Most Expensive City</div>
                    <div class="metric-value">{most_expensive_city}</div>
                    <div>${most_expensive_price:,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
                
            with col3:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Least Expensive City</div>
                    <div class="metric-value">{least_expensive_city}</div>
                    <div>${least_expensive_price:,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<hr>", unsafe_allow_html=True)
            
            # Tabs for different visualizations - removed city map tab
            tab1, tab2, tab3 = st.tabs(["Bar Chart", "Raw Data", "State Comparison"])
            
            with tab1:
                # Create a bar chart of prices by city, limit to top 30 for readability
                top_cities = city_metrics_df.head(30)
                
                bar_chart = alt.Chart(top_cities).mark_bar().encode(
                    x=alt.X('PRICE_METRIC:Q', title=f'{selected_metric} Price ($)'),
                    y=alt.Y('CITY_STATE:N', title='City', sort='-x'),
                    color=alt.Color('PRICE_METRIC:Q', scale=alt.Scale(scheme='blues')),
                    tooltip=[
                        alt.Tooltip('CITY_STATE:N', title='City'),
                        alt.Tooltip('PRICE_METRIC:Q', title=f'{selected_metric} Price', format='$,.2f'),
                        alt.Tooltip('NUM_PROVIDERS:Q', title='Number of Providers')
                    ]
                ).properties(
                    width=700,
                    height=500,
                    title=f'Top Cities by {selected_metric} Standard Charge for {cpt_description}'
                )
                
                st.altair_chart(bar_chart, use_container_width=True)
                
                # If there are more than 30 cities, let the user know
                if len(city_metrics_df) > 30:
                    st.info(f"Showing top 30 cities out of {len(city_metrics_df)} total. Use the raw data tab to see all cities.")
                
            with tab2:
                # Show the raw data
                st.subheader("Raw Data by City")
                
                # Add a search box for filtering the data
                city_search = st.text_input("Search for a specific city:", key="cost_explorer_city_search")
                
                # Filter the data based on search
                if city_search:
                    filtered_df = city_metrics_df[
                        city_metrics_df['CITY_STATE'].str.contains(city_search, case=False)
                    ]
                else:
                    filtered_df = city_metrics_df
                
                # Display the filtered data
                st.dataframe(
                    filtered_df[['CITY_STATE', 'PRICE_METRIC', 'NUM_PROVIDERS']].rename(
                        columns={
                            'CITY_STATE': 'City',
                            'PRICE_METRIC': f'{selected_metric} Price',
                            'NUM_PROVIDERS': 'Number of Providers'
                        }
                    ).style.format({
                        f'{selected_metric} Price': '${:,.2f}'
                    }),
                    use_container_width=True
                )
                
            with tab3:
                # Aggregate data by state for comparison
                state_aggregation = city_metrics_df.groupby('STATE').agg(
                    AvgPrice=('PRICE_METRIC', 'mean'),
                    MinPrice=('PRICE_METRIC', 'min'),
                    MaxPrice=('PRICE_METRIC', 'max'),
                    TotalCities=('CITY', 'count'),
                    TotalProviders=('NUM_PROVIDERS', 'sum')
                ).reset_index()
                
                # Create a state comparison chart
                state_chart = alt.Chart(state_aggregation).mark_bar().encode(
                    x=alt.X('STATE:N', title='State'),
                    y=alt.Y('AvgPrice:Q', title=f'Average {selected_metric} Price ($)'),
                    color=alt.Color('STATE:N', legend=None),
                    tooltip=[
                        alt.Tooltip('STATE:N', title='State'),
                        alt.Tooltip('AvgPrice:Q', title=f'Average {selected_metric} Price', format='$,.2f'),
                        alt.Tooltip('MinPrice:Q', title='Minimum Price', format='$,.2f'),
                        alt.Tooltip('MaxPrice:Q', title='Maximum Price', format='$,.2f'),
                        alt.Tooltip('TotalCities:Q', title='Number of Cities'),
                        alt.Tooltip('TotalProviders:Q', title='Total Providers')
                    ]
                ).properties(
                    width=700,
                    height=400,
                    title=f'State Comparison of {selected_metric} Standard Charge for {cpt_description}'
                )
                
                st.altair_chart(state_chart, use_container_width=True)
                
                # Show table of state data
                st.dataframe(
                    state_aggregation.rename(
                        columns={
                            'STATE': 'State',
                            'AvgPrice': f'Average {selected_metric} Price',
                            'MinPrice': 'Minimum Price',
                            'MaxPrice': 'Maximum Price',
                            'TotalCities': 'Number of Cities',
                            'TotalProviders': 'Total Providers'
                        }
                    ).style.format({
                        f'Average {selected_metric} Price': '${:,.2f}',
                        'Minimum Price': '${:,.2f}',
                        'Maximum Price': '${:,.2f}'
                    }),
                    use_container_width=True
                )
            
            # Additional analysis section - cities with highest variation from state average
            # Removed the Distribution of City Prices section
            st.markdown("<hr>", unsafe_allow_html=True)
            st.subheader("Price Variation Analysis")
            
            # Cities with highest variation from state average
            if len(city_metrics_df) >= 3:
                # Calculate state averages
                state_avgs = city_metrics_df.groupby('STATE')['PRICE_METRIC'].mean().reset_index()
                state_avgs.columns = ['STATE', 'STATE_AVG']
                
                # Merge state averages with city data
                city_comparison = city_metrics_df.merge(state_avgs, on='STATE', how='left')
                
                # Calculate price difference from state average
                city_comparison['PRICE_DIFF'] = city_comparison['PRICE_METRIC'] - city_comparison['STATE_AVG']
                city_comparison['PRICE_DIFF_PCT'] = (city_comparison['PRICE_DIFF'] / city_comparison['STATE_AVG']) * 100
                
                # Display cities with highest positive and negative differences
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Cities with Highest Prices Above State Average")
                    above_avg = city_comparison.nlargest(10, 'PRICE_DIFF_PCT')
                    st.dataframe(
                        above_avg[['CITY_STATE', 'PRICE_METRIC', 'STATE_AVG', 'PRICE_DIFF', 'PRICE_DIFF_PCT']].rename(
                            columns={
                                'CITY_STATE': 'City',
                                'PRICE_METRIC': f'{selected_metric} Price',
                                'STATE_AVG': 'State Average',
                                'PRICE_DIFF': 'Price Difference',
                                'PRICE_DIFF_PCT': '% Above Average'
                            }
                        ).style.format({
                            f'{selected_metric} Price': '${:,.2f}',
                            'State Average': '${:,.2f}',
                            'Price Difference': '${:,.2f}',
                            '% Above Average': '{:,.1f}%'
                        }),
                        use_container_width=True
                    )
                
                with col2:
                    st.subheader("Cities with Lowest Prices Below State Average")
                    below_avg = city_comparison.nsmallest(10, 'PRICE_DIFF_PCT')
                    st.dataframe(
                        below_avg[['CITY_STATE', 'PRICE_METRIC', 'STATE_AVG', 'PRICE_DIFF', 'PRICE_DIFF_PCT']].rename(
                            columns={
                                'CITY_STATE': 'City',
                                'PRICE_METRIC': f'{selected_metric} Price',
                                'STATE_AVG': 'State Average',
                                'PRICE_DIFF': 'Price Difference',
                                'PRICE_DIFF_PCT': '% Below Average'
                            }
                        ).style.format({
                            f'{selected_metric} Price': '${:,.2f}',
                            'State Average': '${:,.2f}',
                            'Price Difference': '${:,.2f}',
                            '% Below Average': '{:,.1f}%'
                        }),
                        use_container_width=True
                    )
    else:
        # Initial state before filters are applied - removed Popular Medical Procedures section
        st.markdown("""
        ## Welcome to the Healthcare Cost Explorer
    
        This dashboard helps you visualize and analyze healthcare costs across cities in the United States. You can:
        
        - See how medical procedure prices vary by city
        - Compare average, median, minimum, or maximum prices
        - Filter by state
        - Identify cities with prices significantly above or below state averages
        
        To begin, select a CPT code and other filters in the sidebar, then click "Apply Filters".
        """)

elif app_mode == "Health Cost Navigator":
    # Streamlit app header
    st.title("Health Cost Navigator")
    
    # Input form
    with st.form(key="search_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            cpt_code = st.text_input("Enter CPT Code (Service Code):")
        with col2:
            zip_code = st.text_input("Enter ZIP Code (Optional):")
        with col3:
            city_name = st.text_input("Enter City Name (Optional):")
        search_button = st.form_submit_button("Search", use_container_width=True)
    
    st.markdown("---")
    
    def build_exact_query(zip_code_val):
        return f"""
        SELECT 
            s.DESCRIPTION,
            h.HOSPITAL_NAME, 
            h.CITY, 
            h.STATE, 
            h.ZIPCODE, 
            p.PAYER_NAME, 
            pl.PLAN_NAME, 
            m.STANDARD_CHARGE_DOLLAR, 
            m.MINIMUM_CHARGE, 
            m.MAXIMUM_CHARGE
        FROM {master_table} m
        JOIN {hospital_table} h ON m.HOSPITAL_ID = h.HOSPITAL_ID
        JOIN {provider_table} p ON m.INSURANCE_PROVIDER_ID = p.INSURANCE_PROVIDER_ID
        JOIN {plan_table} pl ON m.INSURANCE_PLAN_ID = pl.INSURANCE_PLAN_ID
        JOIN {service_code_table} s ON m.CODE = s.CODE
        WHERE s.CODE = '{cpt_code}' AND h.ZIPCODE = '{zip_code_val}'
        """
    
    def build_multi_zipcode_query(zipcode_list):
        zipcode_condition = "', '".join(zipcode_list)
        return f"""
        SELECT 
            s.DESCRIPTION,
            h.HOSPITAL_NAME, 
            h.CITY, 
            h.STATE, 
            h.ZIPCODE, 
            p.PAYER_NAME, 
            pl.PLAN_NAME, 
            m.STANDARD_CHARGE_DOLLAR, 
            m.MINIMUM_CHARGE, 
            m.MAXIMUM_CHARGE
        FROM {master_table} m
        JOIN {hospital_table} h ON m.HOSPITAL_ID = h.HOSPITAL_ID
        JOIN {provider_table} p ON m.INSURANCE_PROVIDER_ID = p.INSURANCE_PROVIDER_ID
        JOIN {plan_table} pl ON m.INSURANCE_PLAN_ID = pl.INSURANCE_PLAN_ID
        JOIN {service_code_table} s ON m.CODE = s.CODE
        WHERE s.CODE = '{cpt_code}' AND h.ZIPCODE IN ('{zipcode_condition}')
        """
    
    if search_button:
        if cpt_code:
            results = pd.DataFrame()
    
            if zip_code:
                query = build_exact_query(zip_code)
                st.info(f"Searching for CPT code {cpt_code} in ZIP code {zip_code}...")
                results = run_query(query)
    
                if results.empty:
                    st.warning(f"No results found for ZIP {zip_code}.")
            
            elif city_name:
                city_query = f"""
                SELECT DISTINCT ZIPCODE
                FROM {hospital_table}
                WHERE UPPER(CITY) = '{city_name.strip().upper()}'
                """
                zip_df = run_query(city_query)
    
                if not zip_df.empty:
                    zipcodes = zip_df["ZIPCODE"].tolist()
                    query = build_multi_zipcode_query(zipcodes)
                    st.info(f"Searching for CPT code {cpt_code} in city: {city_name}")
                    results = run_query(query)
    
                    if results.empty:
                        st.warning(f"No results for CPT code {cpt_code} in city {city_name}.")
                    else:
                        st.success(f"Found {len(results)} results for city {city_name}")
    
                else:
                    st.warning(f"No ZIP codes found for city: {city_name}")
    
            else:
                fallback_query = f"""
                SELECT 
                    s.DESCRIPTION,
                    h.HOSPITAL_NAME, 
                    h.CITY, 
                    h.STATE, 
                    h.ZIPCODE, 
                    p.PAYER_NAME, 
                    pl.PLAN_NAME, 
                    m.STANDARD_CHARGE_DOLLAR, 
                    m.MINIMUM_CHARGE, 
                    m.MAXIMUM_CHARGE
                FROM {master_table} m
                JOIN {hospital_table} h ON m.HOSPITAL_ID = h.HOSPITAL_ID
                JOIN {provider_table} p ON m.INSURANCE_PROVIDER_ID = p.INSURANCE_PROVIDER_ID
                JOIN {plan_table} pl ON m.INSURANCE_PLAN_ID = pl.INSURANCE_PLAN_ID
                JOIN {service_code_table} s ON m.CODE = s.CODE
                WHERE s.CODE = '{cpt_code}'
                ORDER BY m.STANDARD_CHARGE_DOLLAR ASC
                LIMIT 10
                """
                st.info(f"No location provided. Showing top 10 cheapest charges for CPT code {cpt_code}.")
                results = run_query(fallback_query)
    
            if not results.empty:
                st.dataframe(
                    results,
                    column_config={
                        "DESCRIPTION": "Service Description",
                        "HOSPITAL_NAME": "Hospital Name",
                        "CITY": "City",
                        "STATE": "State",
                        "ZIPCODE": "ZIP Code",
                        "PAYER_NAME": "Insurance Provider",
                        "PLAN_NAME": "Insurance Plan",
                        "STANDARD_CHARGE_DOLLAR": st.column_config.NumberColumn("Standard Charge ($)", format="$%.2f"),
                        "MINIMUM_CHARGE": st.column_config.NumberColumn("Minimum Charge ($)", format="$%.2f"),
                        "MAXIMUM_CHARGE": st.column_config.NumberColumn("Maximum Charge ($)", format="$%.2f")
                    },
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.warning("Please enter a CPT code.")

# Footer
st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("""
<div style="text-align: center; color: gray; font-size: 0.8em;">
    Healthcare Analytics Dashboard | Powered by Snowflake
</div>
""", unsafe_allow_html=True)