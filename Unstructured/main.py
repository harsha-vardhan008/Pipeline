from extract import extract
from transform import trans
import load as l



def main():
    # Extract
    df = extract()

    # Transform
    project_df, client_df, team_df, milestone_df, fact_df = trans(df)

    # Connect to SQL Server
    engine = l.connect_to_sqlserver()

    # Load and log each table
    l.load_to_sql(project_df, "dim_project", engine)
    

    l.load_to_sql(client_df, "dim_client", engine)

    l.load_to_sql(team_df, "dim_team", engine)

    l.load_to_sql(milestone_df, "dim_milestone", engine)

    l.load_to_sql(fact_df, "fact_project", engine)

    print(" successfull.")

if __name__ == "__main__":
    main()

