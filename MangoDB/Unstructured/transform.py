import pandas as pd 
from datetime import datetime

def trans(df):
    #create prooject table
    df["_id"]=df["_id"].astype(str)
    #creating project table
    project_df=df[['_id','project_id','technologies','status']]
    
    #creating client table
    client_df=pd.json_normalize(df['client'],sep='_')
    client_df['project_id']=df['project_id']
    client_df=client_df.rename(columns={
    "name": "client_name",
    "location_city": "city",
    "location_country": "country"
    })[["project_id", "client_name", "industry", "city", "country"]]
    client_df['client_id']=['CLI'+str(i+1).zfill(3) for i in range(len(client_df))]
    client_df=client_df[['project_id','client_id','client_name','industry','city','country']]

    #creating members table

    rows = []
    for _, row in df.iterrows():
        project_id = row["project_id"]
        team = row.get("team", {})
        
        # Add project manager
        pm = team.get("project_manager")
        if pm:
            rows.append({
                "project_id": project_id,
                "member_name": pm,
                "role": "Project Manager"
            })
        # Add team members
        members = team.get("members", [])
        for member in members:
            rows.append({
                "project_id": project_id,
                "member_name": member.get("name"),
                "role": member.get("role")
            })
    team_df = pd.DataFrame(rows)

    team_df['member_id']=['MEM'+str(i+1).zfill(3) for i in range(len(team_df))]
    team_df=team_df[['project_id','member_id','member_name','role']]

    # creating milestones table
    
    milestone_rows=[]

    for i,row in df.iterrows():
        project_id=row['project_id']
        milestones=row.get('milestones',[])

        for milestone in milestones:
            milestone_rows.append({
                'project_id':project_id,
                'milestone_name':milestone.get('name'),
                'due_date':pd.to_datetime(milestone.get("due_date"),errors='coerce')
            })

    milestone_df=pd.DataFrame(milestone_rows)
    milestone_df['milestone_id']=['MIL'+str(i+1).zfill(3) for i in range(len(milestone_df))]
    milestone_df=milestone_df[['project_id','milestone_id','milestone_name','due_date']]

    # Fact table
    fact_df = project_df.merge(client_df[['project_id', 'client_id']], on='project_id', how='left')
    team_counts = team_df.groupby('project_id').size().reset_index(name='total_members')
    fact_df = fact_df.merge(team_counts, on='project_id', how='left')
    milestone_counts = milestone_df.groupby('project_id').size().reset_index(name='total_milestones')
    fact_df = fact_df.merge(milestone_counts, on='project_id', how='left')
    fact_df['technologies_count'] = project_df['technologies'].apply(lambda x: len(x) if isinstance(x, list) else 0)
    fact_df = fact_df[['project_id', 'client_id', 'status', 'total_members', 'total_milestones', 'technologies_count']]
    fact_df = fact_df.rename(columns={"status": "project_status"})

    # Return all tables cleanly
    return (
        project_df,
        client_df,
        team_df,
        milestone_df,
        fact_df
    )

   