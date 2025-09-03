import pandas as pd

# Load the CSV files
case_df = pd.read_csv('case.csv')
event_df = pd.read_csv('event.csv')
analysis_df = pd.read_csv('analysis.csv')

# Concatenate them side by side (columns)
combined_df = pd.concat([case_df, event_df, analysis_df], axis=1)

# Save the combined CSV
combined_df.to_csv('irlog.csv', index=False)

print("CSV files successfully merged 'irlog.csv'.")

