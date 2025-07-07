from extract import extract
from transform import trans
from load_data import connect_to_sqlserver, load_to_sql
from log import log_table


def main():
    # Extract
    df = extract()

    # Transform
    project_df, client_df, team_df, milestone_df, fact_df = trans(df)

    # Connect to SQL Server
    engine = connect_to_sqlserver()

    # Load and log each table
    load_to_sql(project_df, "dim_project", engine)
    log_table(source="transform", destination="dim_project", action="insert", transformation_type="project_master", engine=engine)

    load_to_sql(client_df, "dim_client", engine)
    log_table(source="transform", destination="dim_client", action="insert", transformation_type="client_flattening", engine=engine)

    load_to_sql(team_df, "dim_team", engine)
    log_table(source="transform", destination="dim_team", action="insert", transformation_type="team_unroll", engine=engine)

    load_to_sql(milestone_df, "dim_milestone", engine)
    log_table(source="transform", destination="dim_milestone", action="insert", transformation_type="milestone_normalization", engine=engine)

    load_to_sql(fact_df, "fact_project", engine)
    log_table(source="transform", destination="fact_project", action="insert", transformation_type="project_aggregation", engine=engine)

    print(" successfull.")

if __name__ == "__main__":
    main()

