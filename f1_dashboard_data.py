import pandas as pd
import os

def process_f1_data(data_dir='data'):
    """
    Loads F1 data from CSV files, calculates various statistics,
    and saves them into new CSV files.

    Args:
        data_dir (str): The directory where the F1 CSV files are located.
    """
    print(f"Loading data from {data_dir}...")

    try:
        # Load datasets
        drivers_df = pd.read_csv(os.path.join(data_dir, 'drivers.csv'))
        races_df = pd.read_csv(os.path.join(data_dir, 'races.csv'))
        results_df = pd.read_csv(os.path.join(data_dir, 'results.csv'))
        qualifying_df = pd.read_csv(os.path.join(data_dir, 'qualifying.csv'))
        constructors_df = pd.read_csv(os.path.join(data_dir, 'constructors.csv')) # Load constructors data
        print("All raw data loaded successfully.")

    except FileNotFoundError as e:
        print(f"Error: One or more required CSV files not found. Make sure '{data_dir}' exists and contains all files.")
        print(f"Missing file: {e.filename}")
        return

    # --- 1. Calculate Average Positions Gained (Qualifying to Race Finish) ---
    print("Calculating Average Positions Gained...")

    # Select relevant columns and rename for clarity
    qualifying_positions = qualifying_df[['raceId', 'driverId', 'position']].rename(columns={'position': 'qualifyingPosition'})
    race_finish_positions = results_df[['raceId', 'driverId', 'positionOrder']].rename(columns={'positionOrder': 'raceFinishPosition'})

    # Merge qualifying and race finish positions
    # Use 'inner' merge to only include races where both qualifying and race results exist for a driver
    merged_positions = pd.merge(qualifying_positions, race_finish_positions,
                                on=['raceId', 'driverId'], how='inner')

    # Convert positions to numeric, coercing errors to NaN (e.g., if 'R' for retired is present)
    merged_positions['qualifyingPosition'] = pd.to_numeric(merged_positions['qualifyingPosition'], errors='coerce')
    merged_positions['raceFinishPosition'] = pd.to_numeric(merged_positions['raceFinishPosition'], errors='coerce')

    # Drop rows where positions are NaN (e.g., driver didn't qualify or finish with a valid position)
    merged_positions.dropna(subset=['qualifyingPosition', 'raceFinishPosition'], inplace=True)

    # Calculate positions gained: qualifying position - race finish position
    # A positive value means positions were gained (finished better than qualified)
    # A negative value means positions were lost (finished worse than qualified)
    merged_positions['positionsGained'] = merged_positions['qualifyingPosition'] - merged_positions['raceFinishPosition']

    # Merge with drivers_df to get driver names
    positions_gained_df = pd.merge(merged_positions, drivers_df[['driverId', 'forename', 'surname']],
                                   on='driverId', how='left')

    # Create full driver name
    positions_gained_df['driverName'] = positions_gained_df['forename'] + ' ' + positions_gained_df['surname']

    # Calculate average positions gained per driver
    average_positions_gained = positions_gained_df.groupby('driverName')['positionsGained'].mean().reset_index()
    average_positions_gained.rename(columns={'positionsGained': 'AveragePositionsGained'}, inplace=True)
    average_positions_gained = average_positions_gained.sort_values(by='AveragePositionsGained', ascending=False)

    print("Average Positions Gained calculated.")

    # --- 2. Calculate Total Career Races ---
    print("Calculating Total Career Races...")
    # Count unique raceId for each driverId in results_df
    total_career_races = results_df.groupby('driverId')['raceId'].nunique().reset_index()
    total_career_races.rename(columns={'raceId': 'TotalCareerRaces'}, inplace=True)

    # Merge with drivers_df to get driver names
    total_career_races_df = pd.merge(total_career_races, drivers_df[['driverId', 'forename', 'surname']],
                                     on='driverId', how='left')
    total_career_races_df['driverName'] = total_career_races_df['forename'] + ' ' + total_career_races_df['surname']
    total_career_races_df = total_career_races_df[['driverName', 'TotalCareerRaces']].sort_values(by='TotalCareerRaces', ascending=False)
    print("Total Career Races calculated.")

    # --- 3. Calculate Average Career Finish Position ---
    print("Calculating Average Career Finish Position...")
    # Filter out non-numeric positions (e.g., 'R', 'D', 'W')
    results_df_clean = results_df[pd.to_numeric(results_df['positionOrder'], errors='coerce').notna()]
    results_df_clean['positionOrder'] = pd.to_numeric(results_df_clean['positionOrder'])

    # Calculate average finish position per driver
    avg_finish_position = results_df_clean.groupby('driverId')['positionOrder'].mean().reset_index()
    avg_finish_position.rename(columns={'positionOrder': 'AverageFinishPosition'}, inplace=True)

    # Merge with drivers_df to get driver names
    avg_finish_position_df = pd.merge(avg_finish_position, drivers_df[['driverId', 'forename', 'surname']],
                                      on='driverId', how='left')
    avg_finish_position_df['driverName'] = avg_finish_position_df['forename'] + ' ' + avg_finish_position_df['surname']
    avg_finish_position_df = avg_finish_position_df[['driverName', 'AverageFinishPosition']].sort_values(by='AverageFinishPosition')
    print("Average Career Finish Position calculated.")

    # --- 4. Calculate Total Career Points for Drivers ---
    print("Calculating Total Career Points for Drivers...")
    # Ensure 'points' column is numeric
    results_df['points'] = pd.to_numeric(results_df['points'], errors='coerce')
    total_driver_points = results_df.groupby('driverId')['points'].sum().reset_index()
    total_driver_points.rename(columns={'points': 'TotalCareerPoints'}, inplace=True)

    # Merge with drivers_df to get driver names
    total_driver_points_df = pd.merge(total_driver_points, drivers_df[['driverId', 'forename', 'surname']],
                                      on='driverId', how='left')
    total_driver_points_df['driverName'] = total_driver_points_df['forename'] + ' ' + total_driver_points_df['surname']
    total_driver_points_df = total_driver_points_df[['driverName', 'TotalCareerPoints']].sort_values(by='TotalCareerPoints', ascending=False)
    print("Total Career Points for Drivers calculated.")

    # --- 5. Calculate Most Points Scored by F1 Teams (Constructors) ---
    print("Calculating Most Points Scored by F1 Teams...")
    # Ensure 'points' column is numeric
    results_df['points'] = pd.to_numeric(results_df['points'], errors='coerce')
    total_team_points = results_df.groupby('constructorId')['points'].sum().reset_index()
    total_team_points.rename(columns={'points': 'TotalTeamPoints'}, inplace=True)

    # Merge with constructors_df to get constructor names
    total_team_points_df = pd.merge(total_team_points, constructors_df[['constructorId', 'name']],
                                    on='constructorId', how='left')
    total_team_points_df.rename(columns={'name': 'TeamName'}, inplace=True)
    total_team_points_df = total_team_points_df[['TeamName', 'TotalTeamPoints']].sort_values(by='TotalTeamPoints', ascending=False)
    print("Most Points Scored by F1 Teams calculated.")


    # --- Save to CSV files ---
    output_dir = 'processed_data'
    os.makedirs(output_dir, exist_ok=True) # Create output directory if it doesn't exist

    average_positions_gained.to_csv(os.path.join(output_dir, 'average_positions_gained.csv'), index=False)
    total_career_races_df.to_csv(os.path.join(output_dir, 'total_career_races.csv'), index=False)
    avg_finish_position_df.to_csv(os.path.join(output_dir, 'average_career_finish_position.csv'), index=False)
    total_driver_points_df.to_csv(os.path.join(output_dir, 'total_career_points_drivers.csv'), index=False) # New file
    total_team_points_df.to_csv(os.path.join(output_dir, 'total_career_points_teams.csv'), index=False)     # New file

    print(f"\nProcessed data saved to '{output_dir}' directory:")
    print(f"- average_positions_gained.csv")
    print(f"- total_career_races.csv")
    print(f"- average_career_finish_position.csv")
    print(f"- total_career_points_drivers.csv")
    print(f"- total_career_points_teams.csv")
    print("\nData processing complete. You can now import these CSVs into Power BI.")

if __name__ == "__main__":
    process_f1_data()
